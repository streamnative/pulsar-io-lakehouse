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
package org.apache.pulsar.ecosystem.io.source.delta;

import static org.apache.pulsar.ecosystem.io.source.delta.DeltaRecord.OP_ADD_RECORD;
import static org.apache.pulsar.ecosystem.io.source.delta.DeltaRecord.OP_FIELD;
import static org.apache.pulsar.ecosystem.io.source.delta.DeltaRecord.PARTITION_VALUE_FIELD;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.FileAssert.fail;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.standalone.DeltaScan;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.expressions.Expression;
import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructType;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.ecosystem.io.SinkConnector;
import org.apache.pulsar.ecosystem.io.SourceConnector;
import org.apache.pulsar.ecosystem.io.SourceConnectorConfig;
import org.apache.pulsar.ecosystem.io.common.TestSinkContext;
import org.apache.pulsar.ecosystem.io.common.Utils;
import org.apache.pulsar.ecosystem.io.sink.SinkConnectorUtils;
import org.apache.pulsar.functions.api.Record;
import org.mockito.Mockito;
import org.testng.annotations.Test;


/**
 * DeltaLake source connector test.
 *
 */
@Slf4j
public class DeltaSourceConnectorTest {
    final StructType deltaSchema = new StructType()
        .add("year", new LongType())
        .add("month", new LongType())
        .add("day", new LongType())
        .add("sale_id", new StringType())
        .add("customer", new StringType())
        .add("total_cost", new DoubleType());

    Snapshot snapshot = new Snapshot() {
        Metadata metadata = new Metadata.Builder().schema(deltaSchema).build();
        @Override
        public DeltaScan scan() {
            return null;
        }

        @Override
        public DeltaScan scan(Expression expression) {
            return null;
        }

        @Override
        public List<AddFile> getAllFiles() {
            return null;
        }

        @Override
        public Metadata getMetadata() {
            return metadata;
        }

        @Override
        public long getVersion() {
            return 0;
        }

        @Override
        public CloseableIterator<RowRecord> open() {
            return null;
        }
    };

    @Test
    public void testDeltaReadFilter() {
        Map<Integer, DeltaCheckpoint> checkpointMap = new HashMap<>();
        DeltaCheckpoint checkpointForPartition0 = new DeltaCheckpoint(DeltaCheckpoint.StateType.FULL_COPY, 4);
        DeltaCheckpoint checkpointForPartition1 = new DeltaCheckpoint(DeltaCheckpoint.StateType.INCREMENTAL_COPY, 3);
        checkpointForPartition0.setMetadataChangeFileIndex(4L);
        checkpointForPartition0.setRowNum(200L);
        checkpointForPartition1.setMetadataChangeFileIndex(10L);
        checkpointForPartition1.setRowNum(300L);
        checkpointMap.put(0, checkpointForPartition0);
        checkpointMap.put(1, checkpointForPartition1);

        AddFile addFile = new AddFile("tt", new HashMap<>(), 1, 1, false, "tt", new HashMap<>());
        Function<DeltaReader.ReadCursor, Boolean> filter =
            SourceConnector.initDeltaReadFilter(checkpointMap, 5);
        // check read cursor is null
        assertTrue(filter.apply(null));

        // falls to slot 2
        DeltaReader.ReadCursor readCursor =
            new DeltaReader.ReadCursor(addFile, 2, false, 3, "xx");
        // check read cursor target slot not in checkpoint.
        assertTrue(filter.apply(readCursor));

        // incremental_copy and falls to slot 0
        readCursor = new DeltaReader.ReadCursor(addFile, 2, false, 3, "");
        readCursor.setRowNum(150);
        assertTrue(filter.apply(readCursor));

        // full copy and falls into slot 0
        readCursor = new DeltaReader.ReadCursor(addFile, 2, true, 3, "");
        readCursor.setVersion(4);
        readCursor.setRowNum(150);
        assertFalse(filter.apply(readCursor));

        readCursor.setChangeIndex(4);
        readCursor.setRowNum(150);
        assertFalse(filter.apply(readCursor));

        readCursor.setRowNum(200);
        assertTrue(filter.apply(readCursor));

        readCursor.setRowNum(300);
        assertTrue(filter.apply(readCursor));

        readCursor.setRowNum(-1);
        readCursor.setChangeIndex(3);
        assertFalse(filter.apply(readCursor));

        readCursor.setChangeIndex(4);
        assertTrue(filter.apply(readCursor));

        readCursor.setChangeIndex(5);
        assertTrue(filter.apply(readCursor));
    }

    @Test
    public void testRestoreCheckpoint() throws Exception {
        // prepare source context
        SourceContextForTest sourceContextForTest = new SourceContextForTest();
        sourceContextForTest.setInstanceId(0);
        sourceContextForTest.setNumInstances(5);
        Map<Integer, DeltaCheckpoint> checkpoint = new HashMap<>();
        DeltaCheckpoint checkpoint0 = new DeltaCheckpoint(DeltaCheckpoint.StateType.FULL_COPY, 3);
        DeltaCheckpoint checkpoint1 = new DeltaCheckpoint(DeltaCheckpoint.StateType.INCREMENTAL_COPY, 5);
        checkpoint0.setMetadataChangeFileIndex(4L);
        checkpoint0.setRowNum(100L);
        checkpoint0.setSeqCount(20L);
        checkpoint1.setMetadataChangeFileIndex(5L);
        checkpoint1.setRowNum(200L);
        checkpoint1.setSeqCount(10L);
        checkpoint.put(0, checkpoint0);
        checkpoint.put(1, checkpoint1);
        ObjectMapper mapper = Utils.JSON_MAPPER.get();
        checkpoint.forEach((k, v) -> {
            try {
                sourceContextForTest.putState(DeltaCheckpoint.getStatekey(k),
                    ByteBuffer.wrap(mapper.writeValueAsBytes(v)));
            } catch (Exception e) {
                fail();
            }
        });

        //======= test normal get checkpoint from state store =======
        // prepare deltaLakeConnectorConfig
        Map<String, Object> map = new HashMap<>();
        map.put("tablePath", "/tmp/test.conf");
        map.put("startSnapshotVersion", 3);
        map.put("type", "delta");
        SourceConnectorConfig config = SourceConnectorConfig.load(map);
        config.validate();

        // set topicPartitionNum
        int topicPartitionNum = 5;

        // restore partition 0 checkpoint
        // prepare reader
        DeltaReader reader = spy(new DeltaReader(config, topicPartitionNum));
        reader.setFilter(readCursor -> true);
        DeltaReader.VersionResponse versionResponse = new DeltaReader.VersionResponse(3L, false);
        Mockito.doReturn(versionResponse).when(reader).getAndValidateSnapShotVersion(3);
        Mockito.doReturn(snapshot).when(reader).getSnapShot(3);

        Triple<Map<Integer, DeltaCheckpoint>, StructType, GenericSchema<GenericRecord>> triple =
            SourceConnector.restoreCheckpoint(sourceContextForTest, config,
                topicPartitionNum, reader);

        assertNotNull(triple);
        Map<Integer, DeltaCheckpoint> checkpointMap = triple.getLeft();
        assertEquals(deltaSchema, triple.getMiddle());
        assertNull(triple.getRight());
        assertEquals(2, checkpointMap.size());
        assertEquals(checkpointMap.get(SourceConnector.MIN_CHECKPOINT_KEY),
            checkpointMap.get(sourceContextForTest.getInstanceId()));
        assertEquals(checkpointMap.get(SourceConnector.MIN_CHECKPOINT_KEY), checkpoint0);

        // restore partition 1 checkpoint
        sourceContextForTest.setInstanceId(1);
        versionResponse = new DeltaReader.VersionResponse(5L, false);
        Mockito.doReturn(versionResponse).when(reader).getAndValidateSnapShotVersion(5);
        Mockito.doReturn(snapshot).when(reader).getSnapShot(5);
        triple = SourceConnector.restoreCheckpoint(sourceContextForTest, config, topicPartitionNum, reader);
        assertNotNull(triple);
        checkpointMap = triple.getLeft();
        assertEquals(deltaSchema, triple.getMiddle());
        assertNull(triple.getRight());
        assertEquals(2, checkpointMap.size());
        assertEquals(checkpointMap.get(SourceConnector.MIN_CHECKPOINT_KEY),
            checkpointMap.get(sourceContextForTest.getInstanceId()));
        assertEquals(checkpointMap.get(SourceConnector.MIN_CHECKPOINT_KEY), checkpoint1);

        //======= test this instance doesn't assign partitions =======
        sourceContextForTest.setInstanceId(5);
        sourceContextForTest.setNumInstances(6);
        triple = SourceConnector.restoreCheckpoint(sourceContextForTest, config, topicPartitionNum, reader);
        assertNull(triple);

        //======= test state store doesn't exist target partition's checkpoint =======
        sourceContextForTest.setInstanceId(3);
        sourceContextForTest.setNumInstances(5);
        // get snapshot with version 3 from reader.
        config.setStartSnapshotVersion(3L);
        triple = SourceConnector.restoreCheckpoint(sourceContextForTest, config, topicPartitionNum, reader);
        assertNotNull(triple);
        checkpointMap = triple.getLeft();
        assertEquals(deltaSchema, triple.getMiddle());
        assertNull(triple.getRight());
        assertEquals(2, checkpointMap.size());
        assertEquals(checkpointMap.get(SourceConnector.MIN_CHECKPOINT_KEY),
            checkpointMap.get(sourceContextForTest.getInstanceId()));
        DeltaCheckpoint deltaCheckpoint = checkpointMap.get(SourceConnector.MIN_CHECKPOINT_KEY);
        assertEquals(DeltaCheckpoint.StateType.INCREMENTAL_COPY, deltaCheckpoint.getState());
        assertEquals(3, (long) deltaCheckpoint.getSnapShotVersion());

        //======= deltaLake versionResponse check =======
        // version response version: 7, minCheckpoint version: 3
        try {
            sourceContextForTest.setInstanceId(0);
            sourceContextForTest.setNumInstances(5);
            config.setStartSnapshotVersion(3L);
            versionResponse = new DeltaReader.VersionResponse(7L, false);
            Mockito.doReturn(versionResponse).when(reader).getAndValidateSnapShotVersion(3);
            Mockito.doReturn(snapshot).when(reader).getSnapShot(3);
            triple = SourceConnector
                .restoreCheckpoint(sourceContextForTest, config, topicPartitionNum, reader);
            fail();
        } catch (IOException e) {
            assertEquals("last checkpoint version not exist, need to handle this manually", e.getMessage());
        }

        // version response version: 2, minCheckpoint version: 3
        try {
            sourceContextForTest.setInstanceId(0);
            sourceContextForTest.setNumInstances(5);
            config.setStartSnapshotVersion(3L);
            versionResponse = new DeltaReader.VersionResponse(2L, false);
            Mockito.doReturn(versionResponse).when(reader).getAndValidateSnapShotVersion(3);
            Mockito.doReturn(snapshot).when(reader).getSnapShot(2);
            triple = SourceConnector
                .restoreCheckpoint(sourceContextForTest, config, topicPartitionNum, reader);
        } catch (IOException e) {
            fail();
        }

        // version response version: 3, minCheckpoint version: 3
        try {
            sourceContextForTest.setInstanceId(0);
            sourceContextForTest.setNumInstances(5);
            config.setStartSnapshotVersion(3L);
            versionResponse = new DeltaReader.VersionResponse(3L, false);
            Mockito.doReturn(versionResponse).when(reader).getAndValidateSnapShotVersion(3);
            Mockito.doReturn(snapshot).when(reader).getSnapShot(3);
            triple = SourceConnector
                .restoreCheckpoint(sourceContextForTest, config, topicPartitionNum, reader);
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    public void integrationTest() throws Exception {
        String path = "src/test/java/resources/external/sales";
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("fetchHistoryData", true);
        configMap.put("tablePath", path);
        configMap.put("parquetParseThreads", 3);
        configMap.put("maxReadBytesSizeOneRound", 1024 * 1024);
        configMap.put("maxReadRowCountOneRound", 1000);
        configMap.put("checkpointInterval", 0);
        configMap.put("type", "delta");

        String outputTopic = "persistent://public/default/delta_test_v1";
        GenericSchema<GenericRecord> pulsarSchema = DeltaRecord.convertToPulsarSchema(deltaSchema);
        CompletableFuture<List<String>> future = new CompletableFuture<>();
        List<String> topics = new ArrayList<>();
        topics.add(outputTopic);
        future.complete(topics);

        SourceContextForTest sourceContextForTest = spy(new SourceContextForTest());
        sourceContextForTest.setTopic(outputTopic);
        sourceContextForTest.setInstanceId(0);
        sourceContextForTest.setNumInstances(1);

        Mockito.doReturn(mock(PulsarClientImpl.class)).when(sourceContextForTest).getPulsarClient();
        SourceConnector deltaLakeSourceConnector = new SourceConnector();
        PulsarClient pulsarClient = sourceContextForTest.getPulsarClient();
        Mockito.doReturn(future).when(pulsarClient).getPartitionsForTopic(any());
        deltaLakeSourceConnector.open(configMap, sourceContextForTest);

        try {
            for (int year = 2000; year < 2021; ++year) {
                for (int month = 1; month < 13; ++month) {
                    for (int day = 1; day < 29; ++day) {
                        Record<GenericRecord> record = deltaLakeSourceConnector.read();
                        GenericRecord genericRecord = record.getValue();
                        assertEquals(year, (long) genericRecord.getField("year"));
                        assertEquals(month, (long) genericRecord.getField("month"));
                        assertEquals(day, (long) genericRecord.getField("day"));
                        Map<String, String> properties = record.getProperties();
                        assertEquals(OP_ADD_RECORD, properties.get(OP_FIELD));
                        assertTrue(StringUtils.isBlank(properties.get(PARTITION_VALUE_FIELD)));
                        assertEquals(outputTopic, record.getDestinationTopic().get());
                        assertEquals(pulsarSchema.getSchemaInfo().getSchemaDefinition(),
                            record.getSchema().getSchemaInfo().getSchemaDefinition());
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to read from delta lake ", e);
            fail();
        }
    }

    //@Test
    public void testDeltaSinkAndSource() throws Exception {
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

        Map<String, Object> sourceConfigMap = new HashMap<>();
        sourceConfigMap.put("fetchHistoryData", true);
        sourceConfigMap.put("tablePath", tablePath);
        sourceConfigMap.put("fileSystemType", "filesystem");
        sourceConfigMap.put("parquetParseParallelism", 3);
        sourceConfigMap.put("maxReadBytesSizeOneRound", 1024 * 1024);
        sourceConfigMap.put("maxReadRowCountOneRound", 1000);
        sourceConfigMap.put("checkpointInterval", 0);


        String outputTopic = "persistent://public/default/delta_test_v1";
        CompletableFuture<List<String>> future = new CompletableFuture<>();
        List<String> topics = new ArrayList<>();
        topics.add(outputTopic);
        future.complete(topics);

        SourceContextForTest sourceContextForTest = spy(new SourceContextForTest());
        sourceContextForTest.setTopic(outputTopic);
        sourceContextForTest.setInstanceId(0);
        sourceContextForTest.setNumInstances(1);

        Mockito.doReturn(mock(PulsarClientImpl.class)).when(sourceContextForTest).getPulsarClient();
        SourceConnector deltaLakeSourceConnector = new SourceConnector();
        PulsarClient pulsarClient = sourceContextForTest.getPulsarClient();
        Mockito.doReturn(future).when(pulsarClient).getPartitionsForTopic(any());
        deltaLakeSourceConnector.open(sourceConfigMap, sourceContextForTest);

        Record<GenericRecord> record = deltaLakeSourceConnector.read();
        GenericRecord genericRecord = record.getValue();

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
