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
package org.apache.pulsar.ecosystem.io.sink.hudi;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.AvroIgnore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.pulsar.ecosystem.io.SinkConnectorConfig;
import org.intellij.lang.annotations.Language;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class HoodieWriterTest {

    private static final Path PROJECT_DATA = FileSystems.getDefault().getPath("data").toAbsolutePath();

    private Path testPath;
    private SinkConnectorConfig sinkConnectorConfig;

    @BeforeMethod
    public void setup() {
        testPath = Paths.get(PROJECT_DATA.toString(), "hudi", "writer-test-" + TestUtils.randomString(4));
        // initialize the configuration
        sinkConnectorConfig = new SinkConnectorConfig.DefaultSinkConnectorConfig();
        Properties properties = new Properties();
        properties.put("hoodie.table.name", "hoodie-writer-test");
        properties.put("hoodie.table.type", "COPY_ON_WRITE");
        properties.put("hoodie.base.path", "file://" + testPath.toString());
        properties.put("hoodie.datasource.write.recordkey.field", "id");
        properties.put("hoodie.datasource.write.partitionpath.field", "id");
        sinkConnectorConfig.setProperties(properties);
    }

    @Test
    public void testHoodieWriteAndRead() throws Exception {
        // initialize the hoodie writer
        HoodieTestDataV1 testData = new HoodieTestDataV1();
        log.info("Using schema {} to initialize hoodie writer", testData.getSchema().toString());
        HoodieWriter writer = new HoodieWriter(sinkConnectorConfig, testData.getSchema());

        // write test data
        List<HoodieTestDataV1> writeSet = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            HoodieTestDataV1 data = new HoodieTestDataV1(i, i + "-" + TestUtils.randomString(4));
            writer.writeAvroRecord(data.genericRecord());
            writeSet.add(data);
        }

        List<Path> committedFiles = getCommittedFiles(testPath).collect(Collectors.toList());
        Assert.assertEquals(committedFiles.size(), 0);

        // flush the record and commit
        writer.flush();

        // read test data
        List<HoodieTestDataV1> readSet = getCommittedFiles(testPath)
            .map(p -> {
                try {
                    return readRecordsFromFile(p);
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
        writer.close();
    }

    @Test
    public void testHoodieSchemaFieldAdd() throws Exception {
        // initialize the hoodie writer
        HoodieTestDataV1 v1Data = new HoodieTestDataV1();
        log.info("Using schema {} to initialize hoodie writer", v1Data.getSchema().toString());
        HoodieWriter writer = new HoodieWriter(sinkConnectorConfig, v1Data.getSchema());

        // write v1 test data
        List<HoodieTestDataV1> writeSetV1 = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            HoodieTestDataV1 data = new HoodieTestDataV1(i, i + "-" + TestUtils.randomString(4));
            writer.writeAvroRecord(data.genericRecord());
            writeSetV1.add(data);
        }
        Assert.assertEquals(writeSetV1.size(), 10);

        List<Path> committedFiles = getCommittedFiles(testPath).collect(Collectors.toList());
        Assert.assertEquals(committedFiles.size(), 0);

        // update the record schema
        HoodieTestDataV2 v2Data = new HoodieTestDataV2();
        writer.updateSchema(v2Data.getSchema());

        // verify the current committed files contains the records
        List<HoodieTestDataV1> w1 = new LinkedList<>(writeSetV1);
        List<HoodieTestDataV1> readSetV1 = getCommittedFiles(testPath)
            .map(p -> {
                try {
                    return readRecordsFromFile(p);
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
        for (int i = 10; i < 20; i++) {
            HoodieTestDataV2 data = new HoodieTestDataV2(i, i + "-" + TestUtils.randomString(4), random.nextDouble());
            writer.writeAvroRecord(data.genericRecord());
            writeSetV2.add(data);
        }
        Assert.assertEquals(writeSetV2.size(), 10);

        // flush the record and commit
        writer.flush();

        // read test data
        List<GenericRecord> readSet = getCommittedFiles(testPath)
            .map(p -> {
                try {
                    return readRecordsFromFile(p);
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
            .filter(r -> r.hasField("score"))
            .map(HoodieTestDataV2::fromGenericRecord)
            .collect(Collectors.toList());
        Assert.assertTrue(writeSetV2.removeAll(v2));
        Assert.assertEquals(writeSetV2.size(), 0);
        writer.close();
    }


    private Stream<Path> getCommittedFiles(Path path) throws IOException {
        return Files.walk(path).filter(p -> p.toAbsolutePath().toString().endsWith(".parquet"));
    }

    private List<GenericRecord> readRecordsFromFile(Path path) throws IOException {
        List<GenericRecord> records = new LinkedList<>();
        Configuration configuration = new Configuration();
        org.apache.hadoop.fs.Path hdfs = new org.apache.hadoop.fs.Path(path.toAbsolutePath().toString());
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
            + " {\"name\": \"score\", \"type\": \"double\"}]}";

        int id;
        String name;
        double score;

        public HoodieTestDataV2() {
            super(SCHEMA);
        }

        public HoodieTestDataV2(int id, String name, double score) {
            super(SCHEMA);
            this.id = id;
            this.name = name;
            this.score = score;
        }

        @Override
        public GenericRecord genericRecord() {
            GenericRecord record = new GenericData.Record(getSchema());
            record.put("id", id);
            record.put("name", name);
            record.put("score", score);
            return record;
        }

        public static HoodieTestDataV2 fromGenericRecord(GenericRecord record){
            if (!record.hasField("id") || !record.hasField("name") || !record.hasField("score")) {
                throw new IllegalArgumentException("Generic record is mismatched");
            }
            return new HoodieTestDataV2(
                Integer.parseInt(record.get("id").toString()),
                record.get("name").toString(),
                Double.parseDouble(record.get("score").toString()));
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
