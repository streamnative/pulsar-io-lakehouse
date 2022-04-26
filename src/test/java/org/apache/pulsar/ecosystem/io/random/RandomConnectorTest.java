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
package org.apache.pulsar.ecosystem.io.random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.io.core.SourceContext;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test {@link RandomConnector}.
 */
public class RandomConnectorTest {

    private final Map<String, Object> goodConfig = new HashMap<>();
    private final Map<String, Object> badConfig = new HashMap<>();

    @Before
    public void setup() {
        goodConfig.put("randomSeed", System.currentTimeMillis());
        goodConfig.put("maxMessageSize", 1024);
    }

    /**
     * Test opening the connector with good configuration.
     *
     * @throws Exception when fail to open the connector
     */
    @Test
    public void testOpenConnectorWithGoodConfig() throws Exception {
        RandomConnector connector = new RandomConnector();
        connector.open(goodConfig, mock(SourceContext.class));

        assertNotNull("RandomConnectorConfig should be initialized", connector.getConfig());
        assertNotNull("Random instance should be initialized", connector.getRandom());

        assertEquals(RandomConnectorConfig.load(goodConfig), connector.getConfig());
    }

    /**
     * Test opening the connector with good configuration.
     *
     * @throws Exception when fail to open the connector
     */
    @Test
    public void testOpenConnectorWithBadConfig() throws Exception {
        RandomConnector connector = new RandomConnector();
        try {
            connector.open(badConfig, mock(SourceContext.class));
            fail("Should failed to open the connector when using an invalid configuration");
        } catch (NullPointerException npe) {
            // expected
        }

        assertNotNull("RandomConnectorConfig should be initialized", connector.getConfig());
        assertNull("Random instance should not be initialized", connector.getRandom());
    }

    /**
     * Test opening the connector twice.
     *
     * @throws Exception when fail to open the connector
     */
    @Test
    public void testOpenConnectorTwice() throws Exception {
        RandomConnector connector = new RandomConnector();
        connector.open(goodConfig, mock(SourceContext.class));
        assertEquals(RandomConnectorConfig.load(goodConfig), connector.getConfig());

        Map<String, Object> anotherConfig = new HashMap<>(goodConfig);
        // change the maxMessageSize
        anotherConfig.put("maxMessageSize", 2048);
        try {
            connector.open(anotherConfig, mock(SourceContext.class));
            fail("Should fail to open a connector multiple times");
        } catch (IllegalStateException ise) {
            // expected
        }
        assertEquals(RandomConnectorConfig.load(goodConfig), connector.getConfig());
    }

    /**
     * Test opening the connector twice.
     *
     * @throws Exception when fail to open the connector
     */
    @Test
    public void testReadRecordsBeforeOpeningConnector() throws Exception {
        RandomConnector connector = new RandomConnector();
        try {
            connector.read();
            fail("Should fail to read records if a connector is not open");
        } catch (IllegalStateException ise) {
            // expected
        }
    }

}
