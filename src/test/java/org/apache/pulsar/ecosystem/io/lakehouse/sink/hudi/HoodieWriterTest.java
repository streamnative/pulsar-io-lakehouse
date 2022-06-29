/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.ecosystem.io.lakehouse.sink.hudi;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.AvroIgnore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.client.transaction.FileSystemBasedLockProviderTestClass;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.ecosystem.io.lakehouse.SinkConnectorConfig;
import org.apache.pulsar.ecosystem.io.lakehouse.common.Utils;
import org.apache.pulsar.ecosystem.io.lakehouse.sink.PrimitiveFactory;
import org.apache.pulsar.ecosystem.io.lakehouse.sink.PulsarObject;
import org.intellij.lang.annotations.Language;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
public class HoodieWriterTest {

    private static final Path PROJECT_DATA = FileSystems.getDefault().getPath("data").toAbsolutePath();
    private static final String STORAGE_LOCAL = "LOCAL";
    private static final String STORAGE_S3 = "S3";
    private static final String STORAGE_GCS = "GCS";

    private URI testPath;
    private SinkConnectorConfig sinkConfig;

    @DataProvider(name = "storage")
    public Object[][] storageType() {
        return new Object[][]{
            {STORAGE_LOCAL},
            {STORAGE_S3}
        };
    }

    @BeforeMethod()
    public void setup() {
        testPath = Paths.get(PROJECT_DATA.toString(), "hudi", "writer-test-" + TestUtils.randomString(4)).toUri();
        // initialize the configuration
        sinkConfig = new SinkConnectorConfig.DefaultSinkConnectorConfig();
        Properties properties = new Properties();
        properties.put("hoodie.table.name", "hoodie-writer-test");
        properties.put("hoodie.table.type", "COPY_ON_WRITE");
        properties.put("hoodie.base.path", testPath.toString());
        properties.put("hoodie.datasource.write.recordkey.field", "id");
        properties.put("hoodie.datasource.write.partitionpath.field", "id");
        sinkConfig.setProperties(properties);
    }

    private void setCloudProperties(String storage) throws Exception {
        if (storage.equals("S3")) {
            testPath = URI.create(getBucket() + "/writer-test-" + TestUtils.randomString(4));
            sinkConfig.setProperty("hoodie.base.path", testPath.toString());
            sinkConfig.setProperty("hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
        }
    }

    private String getBucket() throws Exception {
        String bucket = System.getenv("CLOUD_BUCKET_NAME");
        if (bucket == null || bucket.trim().equals("")) {
            throw new Exception("Failed to get the bucket name from environment variable");
        }
        return bucket + "/hudi";
    }

    private Optional<FileSystem> setupFileSystem(String storage, HoodieWriter hoodieWriter, Configuration hadoopConf)
        throws IOException {
        if (!storage.equals(STORAGE_LOCAL)) {
            FileSystem fileSystem = FileSystem.get(
                URI.create(hoodieWriter.writer.getConfig().getBasePath()), hadoopConf);
            return Optional.of(fileSystem);
        }
        return Optional.empty();
    }

    @Test
    public void testHoodieWritePulsarPrimitiveTypeMessages() throws Exception {
        final SinkConnectorConfig sinkConnectorConfig = sinkConfig;
        sinkConnectorConfig.getProperties().remove("hoodie.datasource.write.recordkey.field");
        sinkConnectorConfig.setProperty("hoodie.datasource.write.partitionpath.field", "uuid");
        sinkConnectorConfig.setProperty("hoodie.datasource.write.recordkey.field", "uuid");
        final int maxNumber = 10;
        List<PulsarObject<byte[]>> writeSet = new ArrayList<>(maxNumber);
        for (int i = 0; i < maxNumber; i++) {
            String message = "message-" + i;
            byte[] value = message.getBytes(StandardCharsets.UTF_8);
            PulsarObject obj = PrimitiveFactory.getPulsarPrimitiveObject(SchemaType.BYTES, value, "");
            writeSet.add(obj);
        }

        HoodieWriter hoodieWriter = new HoodieWriter(sinkConnectorConfig, writeSet.get(0).getSchema());
        Configuration hadoopConf = hoodieWriter.writer.getContext().getHadoopConf().get();

        for (PulsarObject<byte[]> testDatum : writeSet) {
            hoodieWriter.writeAvroRecord(testDatum.getRecord());
        }

        hoodieWriter.flush();
        List<PulsarObject> readSet = getCommittedFiles(testPath, STORAGE_LOCAL, Optional.empty())
            .map(p -> {
                try {
                    return readRecordsFromFile(p, hadoopConf);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            })
            .flatMap(Collection::stream)
            .map(PulsarObject::fromGenericRecord)
            .collect(Collectors.toList());

        Assert.assertEquals(readSet.size(), writeSet.size());
        for (PulsarObject byteBufferPulsarObject : writeSet) {
            System.out.println(byteBufferPulsarObject.hashCode());
        }

        for (PulsarObject object : readSet) {
            System.out.println(object.hashCode());
        }
        Assert.assertTrue(writeSet.removeAll(readSet));
        Assert.assertEquals(writeSet.size(), 0);
        hoodieWriter.close();
    }

    @Test(dataProvider = "storage", timeOut = 10 * 60 * 1000)
    public void testHoodieWriteAndRead(String storage) throws Exception {
        setCloudProperties(storage);
        final SinkConnectorConfig sinkConnectorConfig = sinkConfig;

        // initialize the hoodie writer
        HoodieTestDataV1 testData = new HoodieTestDataV1();
        log.info("Using schema {} to initialize hoodie writer", testData.getSchema().toString());
        HoodieWriter hoodieWriter = new HoodieWriter(sinkConnectorConfig, testData.getSchema());
        Configuration hadoopConf = hoodieWriter.writer.getContext().getHadoopConf().get();
        Optional<FileSystem> hdfs = setupFileSystem(storage, hoodieWriter, hadoopConf);

        // write test data
        List<HoodieTestDataV1> writeSet = new LinkedList<>();
        for (int i = 0; i < 3; i++) {
            HoodieTestDataV1 data = new HoodieTestDataV1(i, i + "-" + TestUtils.randomString(4));
            hoodieWriter.writeAvroRecord(data.genericRecord());
            writeSet.add(data);
        }

        List<String> committedFiles = getCommittedFiles(testPath, storage, hdfs).collect(Collectors.toList());
        Assert.assertEquals(committedFiles.size(), 0);

        // flush the record and commit
        hoodieWriter.flush();

        // read test data
        List<HoodieTestDataV1> readSet = getCommittedFiles(testPath, storage, hdfs)
            .map(p -> {
                try {
                    return readRecordsFromFile(p, hadoopConf);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            })
            .flatMap(Collection::stream)
            .map(HoodieTestDataV1::fromGenericRecord)
            .collect(Collectors.toList());

        Assert.assertEquals(readSet.size(), writeSet.size());
        Assert.assertTrue(writeSet.removeAll(readSet));
        Assert.assertEquals(writeSet.size(), 0);
        hoodieWriter.close();
    }

    @Test(dataProvider = "storage", timeOut = 10 * 60 * 1000)
    public void testHoodieSchemaFieldAdd(String storage) throws Exception {
        setCloudProperties(storage);
        final SinkConnectorConfig sinkConnectorConfig = sinkConfig;

        // initialize the hoodie writer
        HoodieTestDataV1 v1Data = new HoodieTestDataV1();
        log.info("Using schema {} to initialize hoodie writer", v1Data.getSchema().toString());
        HoodieWriter hoodieWriter = new HoodieWriter(sinkConnectorConfig, v1Data.getSchema());
        Configuration hadoopConf = hoodieWriter.writer.getContext().getHadoopConf().get();
        Optional<FileSystem> hdfs = setupFileSystem(storage, hoodieWriter, hadoopConf);

        // write v1 test data
        List<HoodieTestDataV1> writeSetV1 = new LinkedList<>();
        for (int i = 0; i < 3; i++) {
            HoodieTestDataV1 data = new HoodieTestDataV1(i, i + "-" + TestUtils.randomString(4));
            hoodieWriter.writeAvroRecord(data.genericRecord());
            writeSetV1.add(data);
        }
        Assert.assertEquals(writeSetV1.size(), 3);

        List<String> committedFiles = getCommittedFiles(testPath, storage, hdfs).collect(Collectors.toList());
        Assert.assertEquals(committedFiles.size(), 0);

        // update the record schema
        HoodieTestDataV2 v2Data = new HoodieTestDataV2();
        hoodieWriter.updateSchema(v2Data.getSchema());

        // verify the current committed files contains the records
        List<HoodieTestDataV1> w1 = new LinkedList<>(writeSetV1);
        List<HoodieTestDataV1> readSetV1 = getCommittedFiles(testPath, storage, hdfs)
            .map(p -> {
                try {
                    return readRecordsFromFile(p, hadoopConf);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            })
            .flatMap(Collection::stream)
            .map(HoodieTestDataV1::fromGenericRecord)
            .collect(Collectors.toList());

        Assert.assertEquals(readSetV1.size(), w1.size());
        Assert.assertTrue(w1.removeAll(readSetV1));
        Assert.assertEquals(w1.size(), 0);

        // the v1 data should be committed after updating the schema
        Random random = new SecureRandom();
        List<HoodieTestDataV2> writeSetV2 = new LinkedList<>();
        for (int i = 3; i < 6; i++) {
            HoodieTestDataV2 data = new HoodieTestDataV2(i, i + "-" + TestUtils.randomString(4), random.nextDouble());
            hoodieWriter.writeAvroRecord(data.genericRecord());
            writeSetV2.add(data);
        }
        Assert.assertEquals(writeSetV2.size(), 3);

        // flush the record and commit
        hoodieWriter.flush();

        // read test data
        List<GenericRecord> readSet = getCommittedFiles(testPath, storage, hdfs)
            .map(p -> {
                try {
                    return readRecordsFromFile(p, hadoopConf);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            })
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
        Assert.assertEquals(readSet.size(), writeSetV1.size() + writeSetV2.size());

        List<HoodieTestDataV1> v1 = readSet.stream()
            .filter(r -> !r.hasField("score"))
            .map(HoodieTestDataV1::fromGenericRecord)
            .collect(Collectors.toList());
        Assert.assertTrue(writeSetV1.removeAll(v1));
        Assert.assertEquals(writeSetV1.size(), 0);

        List<HoodieTestDataV2> v2 = readSet.stream()
            .filter(r -> r.hasField("aDouble"))
            .map(HoodieTestDataV2::fromGenericRecord)
            .collect(Collectors.toList());
        Assert.assertTrue(writeSetV2.removeAll(v2));
        Assert.assertEquals(writeSetV2.size(), 0);
        hoodieWriter.close();
    }

    @Test(dataProvider = "storage", timeOut = 10 * 60 * 1000)
    public void testConcurrentWrite(String storage) throws Exception {
        setCloudProperties(storage);
        final SinkConnectorConfig connectorConfig = sinkConfig;
        connectorConfig.setProperty("hoodie.write.concurrency.mode", "optimistic_concurrency_control");
        connectorConfig.setProperty("hoodie.failed.writes.cleaner.policy", "LAZY");
        connectorConfig.setProperty("hoodie.write.lock.provider", FileSystemBasedLockProviderTestClass.class.getName());
        connectorConfig.setProperty(LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY, testPath.toString() + "/filelock");
        // initialize the hoodie writer
        HoodieTestDataV1 testData = new HoodieTestDataV1();
        log.info("Using schema {} to initialize hoodie writer", testData.getSchema().toString());
        HoodieWriter hoodieWriter = new HoodieWriter(connectorConfig, testData.getSchema());
        Configuration hadoopConf = hoodieWriter.writer.getContext().getHadoopConf().get();
        final Optional<FileSystem> hdfs = setupFileSystem(storage, hoodieWriter, hadoopConf);

        CyclicBarrier barrier = new CyclicBarrier(2);
        CountDownLatch latch = new CountDownLatch(2);
        LinkedBlockingQueue<HoodieTestDataV1> writeSet = new LinkedBlockingQueue<>();

        AtomicLong commitATime = new AtomicLong(-1);
        AtomicLong commitBTime = new AtomicLong(-2);
        new Thread(() -> {
            try (HoodieWriter writer = new HoodieWriter(connectorConfig, testData.getSchema())){
                // write test data
                for (int i = 0; i < 3; i++) {
                    HoodieTestDataV1 data = new HoodieTestDataV1(i, i + "-" + TestUtils.randomString(4));
                    writer.writeAvroRecord(data.genericRecord());
                    writeSet.add(data);
                }

                List<String> committedFiles = getCommittedFiles(testPath, storage, hdfs).collect(Collectors.toList());
                Assert.assertEquals(committedFiles.size(), 0);

                // flush the record and commit
                barrier.await();
                commitATime.set(System.currentTimeMillis());
                log.info("flush A records at {}", commitATime);
                writer.flush();
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            } finally {
                latch.countDown();
            }
        }).start();

        new Thread(() -> {
            try (HoodieWriter writer = new HoodieWriter(connectorConfig, testData.getSchema())){
                // write test data
                for (int i = 3; i < 6; i++) {
                    HoodieTestDataV1 data = new HoodieTestDataV1(i, i + "-" + TestUtils.randomString(4));
                    writer.writeAvroRecord(data.genericRecord());
                    writeSet.add(data);
                }

                List<String> committedFiles = getCommittedFiles(testPath, storage, hdfs).collect(Collectors.toList());
                Assert.assertEquals(committedFiles.size(), 0);

                // flush the record and commit
                barrier.await();
                commitBTime.set(System.currentTimeMillis());
                log.info("flush B records at {}", commitBTime);
                writer.flush();
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            } finally {
                latch.countDown();
            }
        }).start();

        latch.await();

        long diff = Math.abs(commitATime.longValue() - commitBTime.longValue());
        Assert.assertTrue(diff < 50);

        // read test data
        List<HoodieTestDataV1> readSet = getCommittedFiles(testPath, storage, hdfs)
            .map(p -> {
                try {
                    return readRecordsFromFile(p, Utils.getDefaultHadoopConf(connectorConfig));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            })
            .flatMap(Collection::stream)
            .map(HoodieTestDataV1::fromGenericRecord)
            .collect(Collectors.toList());

        Assert.assertEquals(readSet.size(), writeSet.size());
        Assert.assertTrue(writeSet.removeAll(readSet));
        Assert.assertEquals(writeSet.size(), 0);
    }


    private Stream<String> getCommittedFiles(URI path, String type, Optional<FileSystem> hdfs) throws IOException {
        if (type.equals(STORAGE_LOCAL)) {
            return Files.walk(Paths.get(path))
                .filter(p -> p.toAbsolutePath().toString().endsWith(".parquet"))
                .map(Path::toString);
        } else {
            if (hdfs.isPresent()) {
                return getCommittedFiles(hdfs.get(), path);
            } else {
                throw new IOException("HDFS is not present");
            }
        }
    }

    private Stream<String> getCommittedFiles(FileSystem fileSystem, URI path) throws IOException {
        return TestUtils.walkThroughCloudDir(fileSystem, path.toString()).stream()
            .filter(f -> f.endsWith(".parquet"));
    }

    private List<GenericRecord> readRecordsFromFile(String path, Configuration configuration) throws IOException {
        List<GenericRecord> records = new LinkedList<>();
        org.apache.hadoop.fs.Path hdfs = new org.apache.hadoop.fs.Path(path);
        HoodieFileReader<GenericRecord> reader = HoodieFileReaderFactory.getFileReader(configuration, hdfs);
        log.info("Reader schema is {}", reader.getSchema().toString());
        reader.getRecordIterator().forEachRemaining(records::add);
        return records;
    }

    private static class HoodieTestDataV1 extends HoodieTestDataBase {
        @Language("JSON5")
        private static final String SCHEMA = "{\"type\":\"record\",\"name\":\"HoodieTestData\","
            + "\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}";

        int id;
        String name;

        public HoodieTestDataV1() {
            super(SCHEMA);
        }

        public HoodieTestDataV1(int id, String name) {
            super(SCHEMA);
            this.id = id;
            this.name = name;
        }

        @Override
        public GenericRecord genericRecord() {
            GenericRecord record = new GenericData.Record(getSchema());
            record.put("id", id);
            record.put("name", name);
            return record;
        }

        public static HoodieTestDataV1 fromGenericRecord(GenericRecord record){
            if (!record.hasField("id") || !record.hasField("name")) {
                throw new IllegalArgumentException("Generic record is mismatched");
            }
            return new HoodieTestDataV1(Integer.parseInt(record.get("id").toString()), record.get("name").toString());
        }
    }

    private static class HoodieTestDataV2 extends HoodieTestDataBase {
        @Language("JSON5")
        private static final String SCHEMA = "{\"type\":\"record\",\"name\":\"HoodieTestData\","
            + "\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"},"
            + " {\"name\": \"aDouble\", \"type\": \"double\"}]}";

        int id;
        String name;
        double aDouble;

        public HoodieTestDataV2() {
            super(SCHEMA);
        }

        public HoodieTestDataV2(int id, String name, double aDouble) {
            super(SCHEMA);
            this.id = id;
            this.name = name;
            this.aDouble = aDouble;
        }

        @Override
        public GenericRecord genericRecord() {
            GenericRecord record = new GenericData.Record(getSchema());
            record.put("id", id);
            record.put("name", name);
            record.put("aDouble", aDouble);
            return record;
        }

        public static HoodieTestDataV2 fromGenericRecord(GenericRecord record){
            if (!record.hasField("id") || !record.hasField("name") || !record.hasField("aDouble")) {
                throw new IllegalArgumentException("Generic record is mismatched");
            }
            return new HoodieTestDataV2(
                Integer.parseInt(record.get("id").toString()),
                record.get("name").toString(),
                Double.parseDouble(record.get("aDouble").toString()));
        }
    }

    @EqualsAndHashCode
    public abstract static class HoodieTestDataBase {
        @AvroIgnore
        private Schema schema;
        @AvroIgnore
        private String schemaStr;

        public HoodieTestDataBase(String schemaStr) {
            this.schemaStr = schemaStr;
        }

        public abstract GenericRecord genericRecord();

        public Schema getSchema() {
            if (schema == null) {
                schema = new Schema.Parser().parse(schemaStr);
            }
            return schema;
        }
    }
}
