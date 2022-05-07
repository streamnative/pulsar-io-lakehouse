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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.types.StructType;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.ecosystem.io.common.SchemaConverter;
import org.apache.pulsar.ecosystem.io.parquet.DeltaParquetWriter;
import org.apache.pulsar.ecosystem.io.sink.SinkConnectorUtils;
import org.apache.pulsar.functions.api.Record;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Delta writer test.
 *
 */
@Slf4j
public class DeltaWriterTest {

    private DeltaSinkConnectorConfig config;
    private Schema schema;
    private org.apache.avro.generic.GenericRecord genericRecord;
    private String tablePath;
    private Map<String, SchemaType> schemaMap;
    private Map<String, Object> recordMap;

    @BeforeMethod
    public void setup() throws IOException {
        tablePath = "/tmp/delta-test-data-" + UUID.randomUUID();
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("tablePath", tablePath);
        configMap.put("type", "delta");

        config = DeltaSinkConnectorConfig.load(configMap);
        config.validate();

        schemaMap = new HashMap<>();
        schemaMap.put("name", SchemaType.STRING);
        schemaMap.put("age", SchemaType.INT32);
        schemaMap.put("phone", SchemaType.STRING);
        schemaMap.put("address", SchemaType.STRING);
        schemaMap.put("score", SchemaType.DOUBLE);

        recordMap = new HashMap<>();
        recordMap.put("name", "hang");
        recordMap.put("age", 18);
        recordMap.put("phone", "110");
        recordMap.put("address", "GuangZhou, China");
        recordMap.put("score", 59.9);
        Record<GenericObject> record = SinkConnectorUtils.generateRecord(schemaMap, recordMap,
                SchemaType.AVRO, "MyRecord");

        schema = new Schema.Parser().parse(record.getSchema().getSchemaInfo().getSchemaDefinition());
        genericRecord = (org.apache.avro.generic.GenericRecord) record.getValue().getNativeObject();
    }

    @AfterMethod
    public void cleanup() {
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

    @Test
    public void testCreateDeltaTable() {
        DeltaWriter writer = new DeltaWriter(config, schema);
        DeltaLog deltaLog = writer.getDeltaLog();
        assertTrue(deltaLog.tableExists());
        assertEquals(deltaLog.getPath().toString(), "file:" + tablePath);
        assertEquals(deltaLog.snapshot().getVersion(), 0);
        assertEquals(deltaLog.snapshot().getAllFiles().size(), 0);

        //assert delta schema
        StructType structType = SchemaConverter.convertAvroSchemaToDeltaSchema(schema);
        assertEquals(deltaLog.snapshot().getMetadata().getSchema(), structType);
        assertEquals(deltaLog.snapshot().getMetadata().getFormat().getProvider(), "parquet");
        assertEquals(deltaLog.snapshot().getMetadata().getDescription(), "create table");
        assertEquals(deltaLog.snapshot().getMetadata().getName(), DeltaWriter.NAME);
        assertEquals(deltaLog.snapshot().getMetadata().getPartitionColumns().size(), 0);
    }

    @Test
    public void testWriteNonPartitionedDeltaTable() {
        DeltaWriter writer = new DeltaWriter(config, schema);
        try {
            for (int i = 0; i < 1000; ++i) {
                writer.writeAvroRecord(genericRecord);
            }

            List<DeltaParquetWriter.FileStat> fileStats = writer.getWriter().closeAndFlush();
            writer.commitFiles(fileStats);
            writer.close();

            DeltaLog deltaLog = writer.getDeltaLog();

            // validate current snapshot
            Snapshot snapshot = deltaLog.snapshot();
            assertEquals(snapshot.getVersion(), 1);
            assertEquals(snapshot.getAllFiles().size(), 1);

            AddFile addFile = snapshot.getAllFiles().get(0);
            DeltaParquetWriter.FileStat fileStat = fileStats.get(0);
            assertEquals(addFile.getPath(), fileStat.getFilePath());
            assertEquals(addFile.getPartitionValues(), fileStat.getPartitionValues());
            assertEquals(addFile.getSize(), fileStat.getFileSize().longValue());

            String engineInfo = DeltaWriter.COMMIT_INFO + " Delta-Standalone/0.3.0";
            assertEquals(deltaLog.getCommitInfoAt(1).getEngineInfo().get(), engineInfo);
        } catch (IOException e) {
            log.error("write record into delta table failed. ", e);
            fail();
        }

    }

    @Test
    public void testUpdateSchema() {
        DeltaWriter writer = new DeltaWriter(config, schema);
        try {
            writer.writeAvroRecord(genericRecord);

            // use the same schema to update, it will skip.
            writer.updateSchema(schema);
            DeltaLog deltaLog = writer.getDeltaLog();
            assertEquals(deltaLog.snapshot().getVersion(), 0);

            // use new compatible schema to update.
            Map<String, SchemaType> schemaMap = new HashMap<>();
            schemaMap.put("name", SchemaType.STRING);
            schemaMap.put("age", SchemaType.INT32);
            schemaMap.put("phone", SchemaType.STRING);
            schemaMap.put("address", SchemaType.STRING);
            schemaMap.put("score", SchemaType.DOUBLE);
            schemaMap.put("grade", SchemaType.INT32);

            Map<String, Object> recordMap = new HashMap<>();
            recordMap.put("name", "hang");
            recordMap.put("age", 18);
            recordMap.put("phone", "110");
            recordMap.put("address", "GuangZhou, China");
            recordMap.put("score", 59.9);
            recordMap.put("grade", 1);
            Record<GenericObject> record = SinkConnectorUtils.generateRecord(schemaMap, recordMap,
                SchemaType.AVRO, "MyRecord");

            Schema newSchema = new Schema.Parser().parse(record.getSchema().getSchemaInfo().getSchemaDefinition());
            org.apache.avro.generic.GenericRecord newGenericRecord =
                (org.apache.avro.generic.GenericRecord) record.getValue().getNativeObject();
            writer.updateSchema(newSchema);
            assertEquals(deltaLog.snapshot().getVersion(), 2);
            StructType structType = SchemaConverter.convertAvroSchemaToDeltaSchema(newSchema);
            assertEquals(deltaLog.update().getMetadata().getSchema(), structType);

            // use new schema related record to write into the delta table.
            writer.writeAvroRecord(newGenericRecord);
            writer.close();

        } catch (IOException e) {
            log.error("Failed to update schema. ", e);
            fail();
        }

    }

    @Test
    public void testPartitionedDeltaTable() throws IOException {
        String partitionedTablePath = "/tmp/delta-test-data-" + UUID.randomUUID();
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("tablePath", partitionedTablePath);
        configMap.put("partitionColumns", Arrays.asList("name", "age"));
        configMap.put("type", "delta");

        DeltaSinkConnectorConfig partitionedConfig = DeltaSinkConnectorConfig.load(configMap);
        partitionedConfig.validate();

        DeltaWriter writer = new DeltaWriter(partitionedConfig, schema);
        try {
            // test write record
            for (int i = 0; i < 100; ++i) {
                recordMap.put("age", 18 + i % 10);
                Record<GenericObject> record = SinkConnectorUtils.generateRecord(schemaMap, recordMap,
                    SchemaType.AVRO, "MyRecord");
                org.apache.avro.generic.GenericRecord r =
                    (org.apache.avro.generic.GenericRecord) record.getValue().getNativeObject();
                writer.writeAvroRecord(r);
            }

            List<DeltaParquetWriter.FileStat> fileStats = writer.getWriter().closeAndFlush();
            writer.commitFiles(fileStats);
            writer.close();

            assertEquals(fileStats.size(), 10);
            DeltaLog deltaLog = writer.getDeltaLog();
            Snapshot snapshot = deltaLog.snapshot();
            assertEquals(snapshot.getVersion(), 1);
            assertEquals(snapshot.getAllFiles().size(), 10);
            for (AddFile addFile : snapshot.getAllFiles()) {
                for (DeltaParquetWriter.FileStat fileStat : fileStats) {
                    if (addFile.getPath().equals(fileStat.getFilePath())) {
                        assertEquals(addFile.getPath(), fileStat.getFilePath());
                        assertEquals(addFile.getPartitionValues(), fileStat.getPartitionValues());
                        assertEquals(addFile.getSize(), fileStat.getFileSize().longValue());
                    }
                }
            }

            String engineInfo = DeltaWriter.COMMIT_INFO + " Delta-Standalone/0.3.0";
            assertEquals(deltaLog.getCommitInfoAt(1).getEngineInfo().get(), engineInfo);

            // update schema
            // use new compatible schema to update.
            Map<String, SchemaType> schemaMap = new HashMap<>();
            schemaMap.put("name", SchemaType.STRING);
            schemaMap.put("age", SchemaType.INT32);
            schemaMap.put("phone", SchemaType.STRING);
            schemaMap.put("address", SchemaType.STRING);
            schemaMap.put("score", SchemaType.DOUBLE);
            schemaMap.put("grade", SchemaType.INT32);

            Map<String, Object> recordMap = new HashMap<>();
            recordMap.put("name", "hang");
            recordMap.put("age", 18);
            recordMap.put("phone", "110");
            recordMap.put("address", "GuangZhou, China");
            recordMap.put("score", 59.9);
            recordMap.put("grade", 1);
            Record<GenericObject> record = SinkConnectorUtils.generateRecord(schemaMap, recordMap,
                SchemaType.AVRO, "MyRecord");

            Schema newSchema = new Schema.Parser().parse(record.getSchema().getSchemaInfo().getSchemaDefinition());
            org.apache.avro.generic.GenericRecord newGenericRecord =
                (org.apache.avro.generic.GenericRecord) record.getValue().getNativeObject();
            writer.updateSchema(newSchema);

            assertEquals(deltaLog.snapshot().getVersion(), 2);
            StructType structType = SchemaConverter.convertAvroSchemaToDeltaSchema(newSchema);
            assertEquals(deltaLog.update().getMetadata().getSchema(), structType);

            for (int i = 0; i < 200; ++i) {
                recordMap.put("age", 18 + i % 20);
                record = SinkConnectorUtils.generateRecord(schemaMap, recordMap,
                    SchemaType.AVRO, "MyRecord");
                org.apache.avro.generic.GenericRecord r =
                    (org.apache.avro.generic.GenericRecord) record.getValue().getNativeObject();
                writer.writeAvroRecord(r);
            }

            fileStats = writer.getWriter().closeAndFlush();
            writer.commitFiles(fileStats);
            writer.close();

            assertEquals(fileStats.size(), 20);
            deltaLog = writer.getDeltaLog();
            snapshot = deltaLog.snapshot();
            assertEquals(snapshot.getVersion(), 3);
            assertEquals(snapshot.getAllFiles().size(), 30);
            for (AddFile addFile : snapshot.getAllFiles()) {
                for (DeltaParquetWriter.FileStat fileStat : fileStats) {
                    if (addFile.getPath().equals(fileStat.getFilePath())) {
                        assertEquals(addFile.getPath(), fileStat.getFilePath());
                        assertEquals(addFile.getPartitionValues(), fileStat.getPartitionValues());
                        assertEquals(addFile.getSize(), fileStat.getFileSize().longValue());
                    }
                }
            }

        } catch (IOException e) {
            log.error("Failed to write records. ", e);
            fail();
        }

        deletePath(partitionedTablePath);
    }
}
