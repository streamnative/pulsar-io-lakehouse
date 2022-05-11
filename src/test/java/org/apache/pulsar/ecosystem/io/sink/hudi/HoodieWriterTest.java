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

import com.google.gson.Gson;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.Data;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.junit.Assert;
import org.junit.Test;


public class HoodieWriterTest {

    @Data
    @SuppressWarnings("checkstyle:MemberName")
    private static class TestData{
        String uuid;
        long ts;
        String rider;
        String driver;
        double begin_lat;
        double begin_lon;
        double end_lat;
        double end_lon;
        double fare;
        String evolution;
    }

    @Test
    public void integrationTests() throws Exception {
        Path root = FileSystems.getDefault().getPath("").toAbsolutePath();
        Path testPath = Paths.get(root.toString(), "data", "hoodie-test", "writertest").toAbsolutePath();
        Map<String, Object> properties = loadConfigurations();
        properties.put("hoodie.base.path", testPath.toString());
        HoodieSinkConfigs hoodieSinkConfigs = HoodieSinkConfigs.newBuilder()
            .withProperties(properties)
            .build();
        HoodieWriterProvider writerProvider = new HoodieWriterProvider(hoodieSinkConfigs);
        BufferedConnectWriter hoodieWriter = writerProvider.open(HoodieExampleDataGenerator.TRIP_EXAMPLE_SCHEMA);
        HoodieExampleDataGenerator<HoodieAvroPayload> dataGen = new HoodieExampleDataGenerator<>();

        HashMap<String, TestData> testData = new HashMap<>();
        for (HoodieRecord<HoodieAvroPayload> xyz : dataGen.generateInserts("xyz", 10, testData)) {
            hoodieWriter.writeHoodieRecord(xyz);
        }

        for (HoodieRecord<HoodieAvroPayload> xyz : dataGen.generateSchemaEvolution("yyy", 20, testData)) {
            hoodieWriter.writeHoodieRecord(xyz);
        }
        Assert.assertEquals(30, testData.size());
        hoodieWriter.close();
        Files.walk(testPath)
            .filter(p -> p.toAbsolutePath().toString().endsWith(".parquet"))
            .forEach(f -> {
                readData(testData, f);
            });
        Assert.assertEquals(0, testData.size());
        Files.walk(testPath)
            .sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(File::delete);
    }

    private void readData(HashMap<String, TestData> testData, Path path) {
        Configuration configuration = new Configuration();
        org.apache.hadoop.fs.Path hdfsPath = new org.apache.hadoop.fs.Path(path.toAbsolutePath().toString());
        try {
        HoodieFileReader<GenericRecord> reader = HoodieFileReaderFactory.getFileReader(configuration, hdfsPath);
            reader.getRecordIterator().forEachRemaining(r -> {
                String uuid = r.get("uuid").toString();
                TestData data = testData.remove(uuid);
                String rider = r.get("rider").toString();
                Assert.assertEquals(data.rider, rider);
                String driver = r.get("driver").toString();
                Assert.assertEquals(data.driver, driver);
                long ts = (long) r.get("ts");
                Assert.assertEquals(data.ts, ts);
                double beginLat = (double) r.get("begin_lat");
                Assert.assertEquals(data.begin_lat, beginLat, 0);
                double beginLon = (double) r.get("begin_lon");
                Assert.assertEquals(data.begin_lon, beginLon, 0);
                double endLat = (double) r.get("end_lat");
                Assert.assertEquals(data.end_lat, endLat, 0);
                double endLon = (double) r.get("end_lon");
                Assert.assertEquals(data.end_lon, endLon, 0);
                double fare = (double) r.get("fare");
                Assert.assertEquals(data.fare, fare, 0);
                if (r.hasField("evolution")) {
                    String evolution = r.get("evolution").toString();
                    Assert.assertEquals(data.evolution, evolution);
                    Assert.assertTrue(data.rider.contains("yyy"));
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Object> loadConfigurations() throws Exception {
        File configFile = new File(this.getClass().getClassLoader()
            .getResource("pulsar.hoodie.sink.write.test.conf.json").toURI());
        Gson gson = new Gson();
        try (FileReader reader = new FileReader(configFile)) {
            return gson.fromJson(reader, HashMap.class);
        }
    }

    static class HoodieExampleDataGenerator<T extends HoodieRecordPayload<T>> {

        public static final String DEFAULT_FIRST_PARTITION_PATH = "2020/01/01";
        public static final String DEFAULT_SECOND_PARTITION_PATH = "2020/01/02";
        public static final String DEFAULT_THIRD_PARTITION_PATH = "2020/01/03";

        public static final String[] DEFAULT_PARTITION_PATHS =
            {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH};
        public static final String TRIP_EXAMPLE_SCHEMA = "{\n"
            + "  \"fields\": [\n"
            + "    {\n"
            + "      \"name\": \"ts\",\n"
            + "      \"type\": \"long\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"uuid\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"rider\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"driver\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"begin_lat\",\n"
            + "      \"type\": \"double\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"begin_lon\",\n"
            + "      \"type\": \"double\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"end_lat\",\n"
            + "      \"type\": \"double\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"end_lon\",\n"
            + "      \"type\": \"double\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"fare\",\n"
            + "      \"type\": \"double\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"name\": \"triprec\",\n"
            + "  \"type\": \"record\"\n"
            + "}";

        public static final String TRIP_EXAMPLE_SCHEMA_EVO = "{\n"
            + "  \"fields\": [\n"
            + "    {\n"
            + "      \"name\": \"ts\",\n"
            + "      \"type\": \"long\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"uuid\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"rider\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"driver\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"begin_lat\",\n"
            + "      \"type\": \"double\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"begin_lon\",\n"
            + "      \"type\": \"double\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"end_lat\",\n"
            + "      \"type\": \"double\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"end_lon\",\n"
            + "      \"type\": \"double\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"fare\",\n"
            + "      \"type\": \"double\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"evolution\",\n"
            + "      \"type\": \"string\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"name\": \"triprec\",\n"
            + "  \"type\": \"record\"\n"
            + "}";
        private static final Random rand = new Random(46474747);
        public static Schema avroSchema = new Schema.Parser().parse(TRIP_EXAMPLE_SCHEMA);
        public static Schema evolutionSchema = new Schema.Parser().parse(TRIP_EXAMPLE_SCHEMA_EVO);
        private final Map<Integer, KeyPartition> existingKeys;
        private final String[] partitionPaths;
        private int numExistingKeys;

        public HoodieExampleDataGenerator(String[] partitionPaths) {
            this(partitionPaths, new HashMap<>());
        }

        public HoodieExampleDataGenerator() {
            this(DEFAULT_PARTITION_PATHS);
        }

        public HoodieExampleDataGenerator(String[] partitionPaths, Map<Integer, KeyPartition> keyPartitionMap) {
            this.partitionPaths = Arrays.copyOf(partitionPaths, partitionPaths.length);
            this.existingKeys = keyPartitionMap;
        }

        /**
         * Generates a new avro record of the above schema format, retaining the key if optionally provided.
         */
        @SuppressWarnings("unchecked")
        public T generateRandomValue(HoodieKey key, String commitTime, HashMap<String, TestData> testData) {
            GenericRecord rec = generateGenericRecord(key.getRecordKey(),
                "rider-" + commitTime, "driver-" + commitTime, 0, testData);
            return (T) new HoodieAvroPayload(Option.of(rec));
        }

        public T generateRandomValueSchemaEvolution(HoodieKey key, String commitTime,
                                                    HashMap<String, TestData> testData) {
            GenericRecord rec = generateSchemaEvolutionRecord(key.getRecordKey(),
                "rider-" + commitTime, "driver-" + commitTime, 0, testData);
            return (T) new HoodieAvroPayload(Option.of(rec));
        }

        public GenericRecord generateSchemaEvolutionRecord(String rowKey, String riderName, String driverName,
                                                           long timestamp, HashMap<String, TestData> testData) {

            GenericRecord rec = new GenericData.Record(evolutionSchema);
            TestData data = new TestData();
            data.uuid = rowKey;
            rec.put("uuid", rowKey);
            data.ts = timestamp;
            rec.put("ts", timestamp);
            data.rider = riderName;
            rec.put("rider", riderName);
            data.driver = driverName;
            rec.put("driver", driverName);
            data.begin_lat = rand.nextDouble();
            rec.put("begin_lat", data.begin_lat);
            data.begin_lon = rand.nextDouble();
            rec.put("begin_lon", data.begin_lon);
            data.end_lat = rand.nextDouble();
            rec.put("end_lat", data.end_lat);
            data.end_lon = rand.nextDouble();
            rec.put("end_lon", data.end_lon);
            data.fare = rand.nextDouble() * 100;
            rec.put("fare", data.fare);
            data.evolution = driverName;
            rec.put("evolution", driverName);
            testData.put(data.uuid, data);
            return rec;
        }

        public GenericRecord generateGenericRecord(String rowKey, String riderName, String driverName,
                                                   long timestamp, HashMap<String, TestData> testData) {
            GenericRecord rec = new GenericData.Record(avroSchema);
            TestData data = new TestData();
            data.uuid = rowKey;
            rec.put("uuid", rowKey);
            data.ts = timestamp;
            rec.put("ts", timestamp);
            data.rider = riderName;
            rec.put("rider", riderName);
            data.driver = driverName;
            rec.put("driver", driverName);
            data.begin_lat = rand.nextDouble();
            rec.put("begin_lat", data.begin_lat);
            data.begin_lon = rand.nextDouble();
            rec.put("begin_lon", data.begin_lon);
            data.end_lat = rand.nextDouble();
            rec.put("end_lat", data.end_lat);
            data.end_lon = rand.nextDouble();
            rec.put("end_lon", data.end_lon);
            data.fare = rand.nextDouble() * 100;
            rec.put("fare", data.fare);
            testData.put(data.uuid, data);
            return rec;
        }

        /**
         * Generates new inserts, uniformly across the partition paths above. It also updates the list of existing keys.
         */
        public List<HoodieRecord<T>> generateInserts(String commitTime, Integer n, HashMap<String, TestData> testData) {
            return generateInsertsStream(commitTime, n, testData).collect(Collectors.toList());
        }

        public List<HoodieRecord<T>> generateSchemaEvolution(String commitTime, Integer n,
                                                             HashMap<String, TestData> testData) {
            return generateInsertsSchemaEvolution(commitTime, n, testData).collect(Collectors.toList());
        }

        public Stream<HoodieRecord<T>> generateInsertsSchemaEvolution(String commitTime, Integer n,
                                                                      HashMap<String, TestData> testData) {
            int currSize = getNumExistingKeys();

            return IntStream.range(0, n).boxed().map(i -> {
                String partitionPath = partitionPaths[rand.nextInt(partitionPaths.length)];
                HoodieKey key = new HoodieKey(UUID.randomUUID().toString(), partitionPath);
                KeyPartition kp = new KeyPartition();
                kp.key = key;
                kp.partitionPath = partitionPath;
                existingKeys.put(currSize + i, kp);
                numExistingKeys++;
                return new HoodieRecord<>(key, generateRandomValueSchemaEvolution(key, commitTime, testData));
            });
        }

        /**
         * Generates new inserts, uniformly across the partition paths above. It also updates the list of existing keys.
         */
        public Stream<HoodieRecord<T>> generateInsertsStream(String commitTime, Integer n,
                                                             HashMap<String, TestData> testData) {
            int currSize = getNumExistingKeys();

            return IntStream.range(0, n).boxed().map(i -> {
                String partitionPath = partitionPaths[rand.nextInt(partitionPaths.length)];
                HoodieKey key = new HoodieKey(UUID.randomUUID().toString(), partitionPath);
                KeyPartition kp = new KeyPartition();
                kp.key = key;
                kp.partitionPath = partitionPath;
                existingKeys.put(currSize + i, kp);
                numExistingKeys++;
                return new HoodieRecord<>(key, generateRandomValue(key, commitTime, testData));
            });
        }

        /**
         * Generates new updates, randomly distributed across the keys above. There can be duplicates
         * within the returned list
         *
         * @param commitTime Commit Timestamp
         * @param n Number of updates (including dups)
         * @return list of hoodie record updates
         */
        public List<HoodieRecord<T>> generateUpdates(String commitTime, Integer n,
                                                     HashMap<String, TestData> testData) {
            List<HoodieRecord<T>> updates = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                KeyPartition kp = existingKeys.get(rand.nextInt(numExistingKeys - 1));
                HoodieRecord<T> record = generateUpdateRecord(kp.key, commitTime, testData);
                updates.add(record);
            }
            return updates;
        }

        public HoodieRecord<T> generateUpdateRecord(HoodieKey key, String commitTime,
                                                    HashMap<String, TestData> testData) {
            return new HoodieRecord<>(key, generateRandomValue(key, commitTime, testData));
        }

        private Option<String> convertToString(HoodieRecord<T> record) {
            try {
                String str = HoodieAvroUtils
                    .bytesToAvro(((HoodieAvroPayload) record.getData()).getRecordBytes(), avroSchema)
                    .toString();
                str = "{" + str.substring(str.indexOf("\"ts\":"));
                return Option.of(str.replaceAll("}", ", \"partitionpath\": \""
                    + record.getPartitionPath() + "\"}"));
            } catch (IOException e) {
                return Option.empty();
            }
        }

        public List<String> convertToStringList(List<HoodieRecord<T>> records) {
            return records.stream().map(this::convertToString).filter(Option::isPresent).map(Option::get)
                .collect(Collectors.toList());
        }

        public int getNumExistingKeys() {
            return numExistingKeys;
        }

        public void close() {
            existingKeys.clear();
        }

        public static class KeyPartition implements Serializable {

            HoodieKey key;
            String partitionPath;
        }

    }
}
