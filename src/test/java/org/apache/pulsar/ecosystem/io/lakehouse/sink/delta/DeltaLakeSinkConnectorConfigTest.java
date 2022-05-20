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

package org.apache.pulsar.ecosystem.io.lakehouse.sink.delta;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

/**
 * Delta lake sink connector config test.
 */
public class DeltaLakeSinkConnectorConfigTest {
    @Test
    public void testLoadConfig() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("maxCommitInterval", 100);
        config.put("sinkConnectorQueueSize", 1000);
        config.put("tablePath", "/tmp/test_v1");
        config.put("partitionColumns", Arrays.asList("a", "b", "c"));
        config.put("compression", "snappy");
        config.put("type", "delta");

        DeltaSinkConnectorConfig deltaLakeSinkConnectorConfig = DeltaSinkConnectorConfig.load(config);
        deltaLakeSinkConnectorConfig.validate();

        assertEquals(deltaLakeSinkConnectorConfig.getMaxCommitInterval(), 100);
        assertEquals(deltaLakeSinkConnectorConfig.getSinkConnectorQueueSize(), 1000);
        assertEquals(deltaLakeSinkConnectorConfig.getTablePath(), "/tmp/test_v1");
        assertEquals(deltaLakeSinkConnectorConfig.getPartitionColumns().get(0), "a");
        assertEquals(deltaLakeSinkConnectorConfig.getPartitionColumns().get(1), "b");
        assertEquals(deltaLakeSinkConnectorConfig.getPartitionColumns().get(2), "c");
        assertEquals(deltaLakeSinkConnectorConfig.getCompression(), "snappy");
    }

    @Test
    public void testInvalidCompressionType() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("maxFlushInterval", 100);
        config.put("sinkConnectorQueueSize", 1000);
        config.put("maxParquetFileSize", 1024 * 1024);
        config.put("tablePath", "/tmp/test_v1");
        config.put("partitionColumns", Arrays.asList("a", "b", "c"));
        config.put("compression", "aa");
        config.put("type", "delta");

        DeltaSinkConnectorConfig deltaLakeSinkConnectorConfig = DeltaSinkConnectorConfig.load(config);
        assertEquals(deltaLakeSinkConnectorConfig.getCompression(), "aa");

        deltaLakeSinkConnectorConfig.validate();

        assertEquals(deltaLakeSinkConnectorConfig.getCompression(), "SNAPPY");
    }

    @Test
    public void testDefaultConfigValue() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("tablePath", "/tmp/test_v1");
        config.put("type", "delta");

        DeltaSinkConnectorConfig deltaLakeSinkConnectorConfig = DeltaSinkConnectorConfig.load(config);
        deltaLakeSinkConnectorConfig.validate();

        assertEquals(deltaLakeSinkConnectorConfig.getMaxCommitInterval(),
            DeltaSinkConnectorConfig.DEFAULT_MAX_COMMIT_INTERVAL);
        assertEquals(deltaLakeSinkConnectorConfig.getSinkConnectorQueueSize(),
            DeltaSinkConnectorConfig.DEFAULT_SINK_CONNECTOR_QUEUE_SIZE);
        assertEquals(deltaLakeSinkConnectorConfig.getTablePath(), "/tmp/test_v1");
        assertTrue(deltaLakeSinkConnectorConfig.getPartitionColumns().isEmpty());
        assertEquals(deltaLakeSinkConnectorConfig.getCompression(),
            DeltaSinkConnectorConfig.DEFAULT_PARQUET_COMPRESSION_TYPE);

    }

    @Test
    public void testRequiredValue() {
        Map<String, Object> config = new HashMap<>();

        try {
            DeltaSinkConnectorConfig deltaLakeSinkConnectorConfig = DeltaSinkConnectorConfig.load(config);
            deltaLakeSinkConnectorConfig.validate();
            fail();
        } catch (IOException | IllegalArgumentException e) {
            assertEquals(e.getMessage(), "type must be set and must be one of hudi, iceberg or delta");
        }

    }
}
