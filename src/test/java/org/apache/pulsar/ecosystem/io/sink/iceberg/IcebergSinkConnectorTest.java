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
package org.apache.pulsar.ecosystem.io.sink.iceberg;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.ecosystem.io.SinkConnector;
import org.apache.pulsar.ecosystem.io.common.TestSinkContext;
import org.apache.pulsar.ecosystem.io.sink.SinkConnectorUtils;
import org.apache.pulsar.functions.api.Record;
import org.awaitility.Awaitility;
import org.junit.Test;

/**
 * Iceberg sink connector test.
 */
@Slf4j
public class IcebergSinkConnectorTest {

    @Test
    public void testNonPartitionedIntegration() throws Exception {
        String tablePath = "/tmp/iceberg-test-data-" + UUID.randomUUID();
        Map<String, Object> config = new HashMap<>();
        config.put("maxParquetFileSize", 1024 * 1024 * 1);
        config.put("tableNamespace", "test-ns");
        config.put("tableName", "test-tb_v1");
        config.put("catalogName", "test-pulsar-catalog");
        Map<String, String> catalogProp = new HashMap<>();
        catalogProp.put(CatalogProperties.WAREHOUSE_LOCATION, tablePath);
        config.put("catalogProperties", catalogProp);
        config.put("type", "iceberg");

        SinkConnector sinkConnector = new SinkConnector();
        sinkConnector.open(config, new TestSinkContext());

        IcebergSinkConnectorConfig sinkConnectorConfig =
            (IcebergSinkConnectorConfig) sinkConnector.getSinkConnectorConfig();

        Map<String, SchemaType> schemaTypeMap = new HashMap<>();
        schemaTypeMap.put("name", SchemaType.STRING);
        schemaTypeMap.put("age", SchemaType.INT32);
        schemaTypeMap.put("phone", SchemaType.STRING);
        schemaTypeMap.put("address", SchemaType.STRING);
        schemaTypeMap.put("score", SchemaType.DOUBLE);

        Map<String, Object> recordMap = new HashMap<>();
        recordMap.put("name", "hang");
        recordMap.put("age", 18);
        recordMap.put("phone", "110");
        recordMap.put("address", "GuangZhou, China");
        recordMap.put("score", 59.9);
        for (int i = 0; i < 1500; ++i) {
            recordMap.put("age", i);
            recordMap.put("score", 59.9 + i);
            Record<GenericObject> record = SinkConnectorUtils
                .generateRecord(schemaTypeMap, recordMap, SchemaType.AVRO, "MyRecord");
            sinkConnector.write(record);
        }

        Awaitility.await().until(() -> sinkConnector.getMessages().isEmpty());
        sinkConnector.close();

        // read message from iceberg table using java api to check the data correctness.
        Configuration configuration = new Configuration();
        CatalogLoader hadoopCatalogLoader = CatalogLoader.hadoop(sinkConnectorConfig.getCatalogName(),
            configuration, sinkConnectorConfig.catalogProperties);
        TableIdentifier identifier =
            TableIdentifier.of(sinkConnectorConfig.tableNamespace, sinkConnectorConfig.getTableName());
        TableLoader tableLoader = TableLoader.fromCatalog(hadoopCatalogLoader, identifier);

        Table table = tableLoader.loadTable();

        Schema icebergSchema = table.schema();
        for (Types.NestedField field: icebergSchema.columns()) {
            String name = field.name();
            Type type = field.type();
            if (schemaTypeMap.get(name).equals(SchemaType.INT32)) {
                assertEquals(type.toString(), "int");
            } else {
                assertEquals(schemaTypeMap.get(name).toString().toLowerCase(Locale.ROOT), type.toString());
            }
        }

        assertEquals(table.spec().fields().size(), 0);
        table.currentSnapshot().addedFiles().forEach(dataFile -> {
            assertEquals(dataFile.format(), FileFormat.PARQUET);
            assertEquals(dataFile.partition().size(), 0);
            assertEquals(dataFile.recordCount(), 1500);
        });

        CloseableIterable<org.apache.iceberg.data.Record> records =
            IcebergGenerics.read(table).select("name", "age", "phone", "address", "score")
                .where(Expressions.greaterThan("age", 10))
                .where(Expressions.lessThan("age", 20))
                .where(Expressions.greaterThan("score", 75))
                .where(Expressions.lessThan("score", 80))
                .build();

        int cnt = 0;
        for (org.apache.iceberg.data.Record record : records) {
            assertEquals(record.getField("name"), "hang");
            assertEquals(record.getField("age"), 16 + cnt);
            assertEquals(record.getField("phone"), "110");
            assertEquals(record.getField("address"), "GuangZhou, China");
            assertEquals(record.getField("score"), 75.9 + cnt);
            cnt++;
        }

        assertEquals(cnt, 4);
        tableLoader.close();

        deletePath(tablePath);
    }

    @Test
    public void testPartitionedIntegration() throws Exception {
        String tablePath = "/tmp/iceberg-test-data-" + UUID.randomUUID();
        Map<String, Object> config = new HashMap<>();
        config.put("maxParquetFileSize", 1024 * 1024 * 1);
        config.put("tableNamespace", "test-ns");
        config.put("tableName", "test-tb_v1");
        config.put("catalogName", "test-pulsar-catalog");
        config.put("partitionColumns", Arrays.asList("age", "phone"));
        Map<String, String> catalogProp = new HashMap<>();
        catalogProp.put(CatalogProperties.WAREHOUSE_LOCATION, tablePath);
        config.put("catalogProperties", catalogProp);
        config.put("type", "iceberg");

        SinkConnector sinkConnector = new SinkConnector();
        sinkConnector.open(config, new TestSinkContext());

        IcebergSinkConnectorConfig sinkConnectorConfig =
            (IcebergSinkConnectorConfig) sinkConnector.getSinkConnectorConfig();

        Map<String, SchemaType> schemaTypeMap = new HashMap<>();
        schemaTypeMap.put("name", SchemaType.STRING);
        schemaTypeMap.put("age", SchemaType.INT32);
        schemaTypeMap.put("phone", SchemaType.STRING);
        schemaTypeMap.put("address", SchemaType.STRING);
        schemaTypeMap.put("score", SchemaType.DOUBLE);

        Map<String, Object> recordMap = new HashMap<>();
        recordMap.put("name", "hang");
        recordMap.put("age", 18);
        recordMap.put("phone", "110");
        recordMap.put("address", "GuangZhou, China");
        recordMap.put("score", 59.9);
        Random random = new Random();

        for (int i = 0; i < 1000; ++i) {
            recordMap.put("age", i % 10);
            recordMap.put("phone", String.valueOf(random.nextInt(5) + 30));
            recordMap.put("score", 59.9 + i);
            Record<GenericObject> record = SinkConnectorUtils.generateRecord(schemaTypeMap, recordMap,
                SchemaType.AVRO, "MyRecord");
            sinkConnector.write(record);
        }

        Awaitility.await().until(() -> sinkConnector.getMessages().isEmpty());
        sinkConnector.close();

        // read message from iceberg table uisng java api to check the data correctness.
        Configuration configuration = new Configuration();
        CatalogLoader hadoopCatalogLoader = CatalogLoader.hadoop(sinkConnectorConfig.getCatalogName(),
            configuration, sinkConnectorConfig.catalogProperties);
        TableIdentifier identifier = TableIdentifier.of(sinkConnectorConfig.tableNamespace,
            sinkConnectorConfig.getTableName());
        TableLoader tableLoader = TableLoader.fromCatalog(hadoopCatalogLoader, identifier);

        Table table = tableLoader.loadTable();

        Schema icebergSchema = table.schema();
        for (Types.NestedField field: icebergSchema.columns()) {
            String name = field.name();
            Type type = field.type();
            if (schemaTypeMap.get(name).equals(SchemaType.INT32)) {
                assertEquals(type.toString(), "int");
            } else {
                assertEquals(schemaTypeMap.get(name).toString().toLowerCase(Locale.ROOT), type.toString());
            }
        }
        assertEquals(table.spec().fields().get(0).name(), "age");
        assertEquals(table.spec().fields().get(1).name(), "phone");

        Map<Integer, Set<String>> partitionMap = new HashMap<>();
        int rowCnt = 0;
        for (DataFile dataFile : table.currentSnapshot().addedFiles()) {
            int age = dataFile.partition().get(0, Integer.class);
            String phone = dataFile.partition().get(1, String.class);
            partitionMap.putIfAbsent(age, new HashSet<>());
            partitionMap.get(age).add(phone);
            assertEquals(dataFile.format(), FileFormat.PARQUET);
            rowCnt += dataFile.recordCount();
        }

        assertEquals(rowCnt, 1000);
        assertEquals(partitionMap.keySet().size(), 10);
        partitionMap.keySet().forEach(k -> {
            assertTrue(k >= 0 && k <= 9);
        });

        partitionMap.values().forEach(v -> {
            assertEquals(v.size(), 5);
            v.forEach(item -> {
                assertTrue(Integer.parseInt(item) >= 30 && Integer.parseInt(item) <= 34);
            });
        });

        CloseableIterable<org.apache.iceberg.data.Record> records =
            IcebergGenerics.read(table)
                .select("name", "age", "phone", "address", "score")
                .where(Expressions.equal("age", 5))
                .where(Expressions.greaterThan("score", 80))
                .where(Expressions.lessThan("score", 200))
                .build();

        Set<Double> scores = new HashSet<>();
        for (int i = 20; i < 150; ++i) {
            if (i % 10 == 5 && i + 59.9 > 80 && i + 59.9 < 200) {
                scores.add(i + 59.9);
            }
        }

        int size = scores.size();
        int cnt = 0;
        for (org.apache.iceberg.data.Record record : records) {
            assertEquals(record.getField("name"), "hang");
            assertEquals(record.getField("age"), 5);
            assertEquals(record.getField("address"), "GuangZhou, China");
            double score = (double) record.getField("score");
            assertTrue(scores.contains(score));
            scores.remove(score);
            cnt++;
        }

        assertEquals(cnt, size);
        tableLoader.close();

        deletePath(tablePath);
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
