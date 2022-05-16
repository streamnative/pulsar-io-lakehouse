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

package org.apache.pulsar.ecosystem.io.sink.delta;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.ecosystem.io.SinkConnector;
import org.apache.pulsar.ecosystem.io.common.TestSinkContext;
import org.apache.pulsar.ecosystem.io.sink.SinkConnectorUtils;
import org.apache.pulsar.functions.api.Record;
import org.testng.annotations.Test;


/**
 * delta lake sink connector test.
 */
@Slf4j
public class DeltaLakeSinkConnectorTest {

    @Test
    public void testGenerateRecord() {

        Record<GenericObject> record = generateRecord();

        GenericObject r = record.getValue();
        assertEquals(((GenericRecord) r).getField("name"), "hang");
        assertEquals(((GenericRecord) r).getField("age"), 18);
        assertEquals(((GenericRecord) r).getField("phone"), "110");
        assertEquals(((GenericRecord) r).getField("address"), "GuangZhou, China");
        assertEquals(((GenericRecord) r).getField("score"), 59.9);
    }

    @Test
    public void testNonPartitionedIntegration() throws Exception {
        String tablePath = "/tmp/delta-test-data-" + UUID.randomUUID();
        Map<String, Object> config = new HashMap<>();
        config.put("tablePath", tablePath);
        config.put("type", "delta");

        SinkConnector sinkConnector = new SinkConnector();
        sinkConnector.open(config, new TestSinkContext());

        Map<String, SchemaType> schemaMap = new HashMap<>();
        schemaMap.put("name", SchemaType.STRING);
        schemaMap.put("age", SchemaType.INT32);
        schemaMap.put("phone", SchemaType.STRING);
        schemaMap.put("address", SchemaType.STRING);
        schemaMap.put("score", SchemaType.DOUBLE);

        Map<String, Object> recordMap = new HashMap<>();
        recordMap.put("name", "hang");
        recordMap.put("age", 18);
        recordMap.put("phone", "110");
        recordMap.put("address", "GuangZhou, China");
        recordMap.put("score", 59.9);
        for (int i = 0; i < 1500; ++i) {
            recordMap.put("age", i);
            recordMap.put("score", 59.9 + i);
            Record<GenericObject> record = SinkConnectorUtils.generateRecord(schemaMap, recordMap,
                SchemaType.AVRO, "MyRecord");
            sinkConnector.write(record);
        }

        while (!sinkConnector.getMessages().isEmpty()) {
            Thread.sleep(1000);
        }
        sinkConnector.close();

        // read data through delta lake api
        DeltaLog deltaLog = DeltaLog.forTable(new Configuration(), tablePath);
        Snapshot currentSnapshot = deltaLog.snapshot();
        StructType schema = currentSnapshot.getMetadata().getSchema();
        assertEquals(currentSnapshot.getVersion(), 1);
        assertEquals(currentSnapshot.getAllFiles().size(), 1);

        CloseableIterator<AddFile> dataFiles = currentSnapshot.scan().getFiles();

        dataFiles.forEachRemaining(file -> {
            assertTrue(file.getPath().contains("part-0000-"));
            assertTrue(file.getPath().endsWith("-c000.snappy.parquet"));
            assertTrue(file.getPartitionValues().isEmpty());
        });


        dataFiles.close();

        // verify schema
        assertNotNull(schema.getFields());
        List<String> fields = new ArrayList<>(schemaMap.keySet());
        for (int i = 0; i < schema.getFields().length; ++i) {
            StructField field = schema.getFields()[i];
            assertEquals(field.getName(), fields.get(i));
            assertNotNull(schemaMap.get(field.getName()));
            // delta integer type is `integer`, avro schema integer type is `INT32`
            if (field.getDataType().getTypeName().equals("integer")) {
                continue;
            }
            assertEquals(field.getDataType().getTypeName(),
                schemaMap.get(field.getName()).name().toLowerCase(Locale.ROOT));
        }

        // verify data
        try (CloseableIterator<RowRecord> iter = currentSnapshot.open()) {
            int cnt = 0;
            while (iter.hasNext()) {
                RowRecord row = iter.next();
                assertEquals(row.getString("name"), "hang");
                assertEquals(row.getString("address"), "GuangZhou, China");
                assertEquals(row.getString("phone"), "110");
                assertTrue(row.getInt("age") >= 0 && row.getInt("age") < 1500);
                assertTrue(row.getDouble("score") >= 59.9 && row.getDouble("score") < 1559.9);
                cnt++;
            }
            assertEquals(cnt, 1500);
        }

        deletePath(tablePath);
    }

    @Test
    public void testPartitionedIntegration() throws Exception {
        String tablePath = "/tmp/delta-test-data-" + UUID.randomUUID();

        Map<String, Object> config = new HashMap<>();
        config.put("tablePath", tablePath);
        config.put("type", "delta");
        config.put("partitionColumns", Arrays.asList("age", "phone"));

        SinkConnector sinkConnector = new SinkConnector();
        sinkConnector.open(config, new TestSinkContext());

        Map<String, SchemaType> schemaMap = new HashMap<>();
        schemaMap.put("name", SchemaType.STRING);
        schemaMap.put("age", SchemaType.INT32);
        schemaMap.put("phone", SchemaType.STRING);
        schemaMap.put("address", SchemaType.STRING);
        schemaMap.put("score", SchemaType.DOUBLE);

        Map<String, Object> recordMap = new HashMap<>();
        recordMap.put("name", "hang");
        recordMap.put("age", 18);
        recordMap.put("phone", "110");
        recordMap.put("address", "GuangZhou, China");
        recordMap.put("score", 59.9);
        Random random = new Random();

        for (int i = 0; i < 1000; ++i) {
            recordMap.put("age", random.nextInt(10));
            recordMap.put("phone", String.valueOf(random.nextInt(5) + 30));
            recordMap.put("score", 59.9 + i);
            Record<GenericObject> record = SinkConnectorUtils.generateRecord(schemaMap, recordMap,
                SchemaType.AVRO, "MyRecord");
            sinkConnector.write(record);
        }

        while (!sinkConnector.getMessages().isEmpty()) {
            Thread.sleep(1000);
        }

        sinkConnector.close();

        // TODO use delta lake java api to read records from delta table, and check whether the records are correct.
        DeltaLog deltaLog = DeltaLog.forTable(new Configuration(), tablePath);
        Snapshot currentSnapshot = deltaLog.snapshot();
        StructType schema = currentSnapshot.getMetadata().getSchema();
        assertEquals(currentSnapshot.getVersion(), 1);
        assertEquals(currentSnapshot.getAllFiles().size(), 5 * 10);

        CloseableIterator<AddFile> dataFiles = currentSnapshot.scan().getFiles();

        dataFiles.forEachRemaining(file -> {
            assertTrue(file.getPath().contains("part-0000-"));
            assertTrue(file.getPath().endsWith("-c000.snappy.parquet"));
            assertNotNull(file.getPartitionValues().get("phone"));
            int phone = Integer.parseInt(file.getPartitionValues().get("phone"));
            assertTrue(phone >= 30 && phone < 35);
            int age = Integer.parseInt(file.getPartitionValues().get("age"));
            assertTrue(age >= 0 && age < 10);
        });


        dataFiles.close();

        // verify schema
        assertNotNull(schema.getFields());
        List<String> fields = new ArrayList<>(schemaMap.keySet());
        for (int i = 0; i < schema.getFields().length; ++i) {
            StructField field = schema.getFields()[i];
            assertEquals(field.getName(), fields.get(i));
            assertNotNull(schemaMap.get(field.getName()));
            // delta integer type is `integer`, avro schema integer type is `INT32`
            if (field.getDataType().getTypeName().equals("integer")) {
                continue;
            }
            assertEquals(field.getDataType().getTypeName(),
                schemaMap.get(field.getName()).name().toLowerCase(Locale.ROOT));
        }

        // verify data
        try (CloseableIterator<RowRecord> iter = currentSnapshot.open()) {
            int cnt = 0;
            while (iter.hasNext()) {
                RowRecord row = iter.next();
                assertEquals(row.getString("name"), "hang");
                assertEquals(row.getString("address"), "GuangZhou, China");
                assertTrue(row.getInt("age") >= 0 && row.getInt("age") < 10);
                int phone = Integer.parseInt(row.getString("phone"));
                assertTrue(phone >= 30 && phone < 35);
                assertTrue(row.getDouble("score") >= 59.9 && row.getDouble("score") < 1059.9);
                cnt++;
            }
            assertEquals(cnt, 1000);
        }

        deletePath(tablePath);
    }

    private Record<GenericObject> generateRecord() {
        Map<String, SchemaType> schemaMap = new HashMap<>();
        schemaMap.put("name", SchemaType.STRING);
        schemaMap.put("age", SchemaType.INT32);
        schemaMap.put("phone", SchemaType.STRING);
        schemaMap.put("address", SchemaType.STRING);
        schemaMap.put("score", SchemaType.DOUBLE);

        Map<String, Object> recordMap = new HashMap<>();
        recordMap.put("name", "hang");
        recordMap.put("age", 18);
        recordMap.put("phone", "110");
        recordMap.put("address", "GuangZhou, China");
        recordMap.put("score", 59.9);

        return SinkConnectorUtils.generateRecord(schemaMap, recordMap,
            SchemaType.AVRO, "MyRecord");
    }

    private void deletePath(String path) {
        try {
            Path dir = Paths.get(path);
            Files.walkFileTree(dir,
                new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }
                });
        } catch (IOException e) {
            log.error("Failed to delete path: {} ", path, e);
        }
    }

}
