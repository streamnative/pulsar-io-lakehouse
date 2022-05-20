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
package org.apache.pulsar.ecosystem.io.lakehouse.source.delta;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.FileAssert.fail;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.ecosystem.io.lakehouse.SourceConnectorConfig;
import org.testng.annotations.Test;



/**
 * Test config.
 */
public class DeltaConnectorConfigTest {

    @Test
    public void testLoadConfig() {
        Map<String, Object> map = new HashMap<>();
        map.put("startSnapshotVersion", 1);
        map.put("fetchHistoryData", true);
        map.put("tablePath", "/tmp/test.conf");
        map.put("parquetParseThreads", 3);
        map.put("maxReadBytesSizeOneRound", 1024 * 1024);
        map.put("maxReadRowCountOneRound", 1000);
        map.put("checkpointInterval", 30);
        map.put("type", "delta");

        try {
            DeltaSourceConfig config = (DeltaSourceConfig) SourceConnectorConfig.load(map);
            assertEquals(1, config.getStartSnapshotVersion().longValue());
            assertTrue(config.getFetchHistoryData());
            assertEquals("/tmp/test.conf", config.getTablePath());
            assertEquals(3, config.getParquetParseThreads());
            assertEquals(1024 * 1024, config.getMaxReadBytesSizeOneRound());
            assertEquals(1000, config.getMaxReadRowCountOneRound());
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testRequiredField() {
        Map<String, Object> map = new HashMap<>();
        map.put("startSnapshotVersion", 1);
        map.put("fetchHistoryData", true);
        map.put("type", "delta");

        try {
            SourceConnectorConfig config = SourceConnectorConfig.load(map);
            config.validate();
            fail();
        } catch (Exception e) {
            assertEquals("tablePath should be set.", e.getMessage());
        }
    }

    @Test
    public void testDefaultValue() {
        Map<String, Object> map = new HashMap<>();
        map.put("tablePath", "/tmp/test.conf");
        map.put("type", "delta");

        try {
            DeltaSourceConfig config = (DeltaSourceConfig) SourceConnectorConfig.load(map);
            config.validate();

            assertEquals(DeltaSourceConfig.LATEST, config.getStartSnapshotVersion().longValue());
            assertFalse(config.getFetchHistoryData());
            assertEquals("/tmp/test.conf", config.getTablePath());
            assertEquals(Runtime.getRuntime().availableProcessors(), config.getParquetParseThreads());
            assertEquals(DeltaSourceConfig.DEFAULT_MAX_READ_BYTES_SIZE_ONE_ROUND,
                config.getMaxReadBytesSizeOneRound());
            assertEquals(DeltaSourceConfig.DEFAULT_MAX_READ_ROW_COUNT_ONE_ROUND,
                config.getMaxReadRowCountOneRound());
            assertEquals(DeltaSourceConfig.DEFAULT_CHECKPOINT_INTERVAL, config.getCheckpointInterval());
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testValidateStartSnapshotVersion() {
        Map<String, Object> map = new HashMap<>();
        map.put("tablePath", "/tmp/test.conf");
        map.put("startSnapshotVersion", 1);
        map.put("startTimestamp", 11111);
        map.put("type", "delta");

        try {
            SourceConnectorConfig config = SourceConnectorConfig.load(map);
            config.validate();
            fail();
        } catch (Exception e) {
            assertEquals("startSnapshotVersion and startTimeStamp should not be set at the same time.",
                e.getMessage());
        }
    }

    @Test
    public void testInvalidParquetParseThreads() {
        Map<String, Object> map = new HashMap<>();
        map.put("tablePath", "/tmp/test.conf");
        map.put("parquetParseThreads", DeltaSourceConfig.DEFAULT_PARQUET_PARSE_THREADS * 3);
        map.put("type", "delta");

        try {
            DeltaSourceConfig config = (DeltaSourceConfig) SourceConnectorConfig.load(map);
            assertEquals(DeltaSourceConfig.DEFAULT_PARQUET_PARSE_THREADS * 3,
                    config.getParquetParseThreads());
            config.validate();
            assertEquals(DeltaSourceConfig.DEFAULT_PARQUET_PARSE_THREADS,
                config.getParquetParseThreads());
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testInvalidMaxReadBytesSizeOneRound() {
        Map<String, Object> map = new HashMap<>();
        map.put("tablePath", "/tmp/test.conf");
        map.put("maxReadBytesSizeOneRound", -1);
        map.put("type", "delta");

        try {
            DeltaSourceConfig config = (DeltaSourceConfig) SourceConnectorConfig.load(map);
            assertEquals(-1, config.getMaxReadBytesSizeOneRound());
            config.validate();
            assertEquals(DeltaSourceConfig.DEFAULT_MAX_READ_BYTES_SIZE_ONE_ROUND,
                config.getMaxReadBytesSizeOneRound());
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testInvalidQueueSize() {
        Map<String, Object> map = new HashMap<>();
        map.put("tablePath", "/tmp/test.conf");
        map.put("queueSize", -1);
        map.put("type", "delta");

        try {
            SourceConnectorConfig config = SourceConnectorConfig.load(map);
            assertEquals(-1, config.getQueueSize());
            config.validate();
            assertEquals(DeltaSourceConfig.DEFAULT_QUEUE_SIZE,
                config.getQueueSize());
        } catch (Exception e) {
            fail();
        }
    }
}
