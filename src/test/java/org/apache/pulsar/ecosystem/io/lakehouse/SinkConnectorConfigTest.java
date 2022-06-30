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
package org.apache.pulsar.ecosystem.io.lakehouse;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.ecosystem.io.lakehouse.exception.IncorrectParameterException;
import org.testng.annotations.Test;

public class SinkConnectorConfigTest {

    @Test
    public void testDefaultValues() throws Exception {
        Map<String, Object> properties = new HashMap<>();
        properties.put("type", "hudi");
        SinkConnectorConfig config = SinkConnectorConfig.load(properties);
        assertEquals(SinkConnectorConfig.DEFAULT_MAX_COMMIT_INTERVAL, config.getMaxCommitInterval());
        assertEquals(SinkConnectorConfig.DEFAULT_MAX_RECORDS_PER_COMMIT, config.getMaxRecordsPerCommit());
        assertEquals(SinkConnectorConfig.DEFAULT_MAX_COMMIT_FAILED_TIMES, config.getMaxCommitFailedTimes());
        assertEquals(SinkConnectorConfig.DEFAULT_SINK_CONNECTOR_QUEUE_SIZE, config.getSinkConnectorQueueSize());
        assertEquals(Collections.emptyList(), config.getPartitionColumns());
        assertEquals("", config.getOverrideFieldName());
    }

    @Test
    public void testOverrideDefaultValues() throws Exception {
        Map<String, Object> properties = new HashMap<>();
        properties.put("type", "hudi");
        properties.put("partitionColumns", Collections.singletonList("partition"));
        properties.put("maxCommitInterval", 10);
        properties.put("maxRecordsPerCommit", 10);
        properties.put("maxCommitFailedTimes", 10);
        properties.put("sinkConnectorQueueSize", 10);
        properties.put("overrideFieldName", "filedname");
        SinkConnectorConfig config = SinkConnectorConfig.load(properties);
        assertEquals(10, config.getMaxCommitInterval());
        assertEquals(10, config.getMaxRecordsPerCommit());
        assertEquals(10, config.getMaxCommitFailedTimes());
        assertEquals(10, config.getSinkConnectorQueueSize());
        assertEquals(Collections.singletonList("partition"), config.getPartitionColumns());
        assertEquals("filedname", config.getOverrideFieldName());
    }

    @Test
    public void testLoadInvalidType() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("type", "unknown");
        try {
            SinkConnectorConfig.load(properties);
            fail();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (IncorrectParameterException e) {
            // expected exception
        }
    }
}
