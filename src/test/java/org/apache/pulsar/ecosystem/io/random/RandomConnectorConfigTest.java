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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

/**
 * Unit test {@link RandomConnectorConfig}.
 */
public class RandomConnectorConfigTest {

    /**
     * Test Case: load the configuration from an empty property map.
     *
     * @throws IOException when failed to load the property map
     */
    @Test
    public void testLoadEmptyPropertyMap() throws IOException {
        Map<String, Object> emptyMap = Collections.emptyMap();
        RandomConnectorConfig config = RandomConnectorConfig.load(emptyMap);
        assertNull("RandomSeed should not be set", config.getRandomSeed());
        assertNull("MaxMessageSize should not be set", config.getRandomSeed());
    }

    /**
     * Test Case: load the configuration from a property map.
     *
     * @throws IOException when failed to load the property map
     */
    @Test
    public void testLoadPropertyMap() throws IOException {
        Map<String, Object> properties = new HashMap<>();
        long seed = System.currentTimeMillis();
        properties.put("randomSeed", seed);
        properties.put("maxMessageSize", 2048);

        RandomConnectorConfig config = RandomConnectorConfig.load(properties);
        assertEquals("Mismatched random seed : " + config.getRandomSeed(),
            seed, config.getRandomSeed().longValue());
        assertEquals("Mismatched MaxMessageSize : " + config.getMaxMessageSize(),
            2048, config.getMaxMessageSize().intValue());
    }

    /**
     * Test Case: load the configuration from a string property map.
     *
     * @throws IOException when failed to load the property map
     */
    @Test
    public void testLoadStringPropertyMap() throws IOException {
        Map<String, Object> properties = new HashMap<>();
        long seed = System.currentTimeMillis();
        properties.put("randomSeed", "" + seed);
        properties.put("maxMessageSize", "2048");

        RandomConnectorConfig config = RandomConnectorConfig.load(properties);
        assertEquals("Mismatched random seed : " + config.getRandomSeed(),
            seed, config.getRandomSeed().longValue());
        assertEquals("Mismatched MaxMessageSize : " + config.getMaxMessageSize(),
            2048, config.getMaxMessageSize().intValue());
    }

    /**
     * Test Case: load the configuration from a string property map.
     *
     * @throws IOException when failed to load the property map
     */
    @Test(expected = JsonProcessingException.class)
    public void testLoadInvalidPropertyMap() throws IOException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("randomSeed", "invalid-seed");
        properties.put("maxMessageSize", "invalid-max-message-size");

        RandomConnectorConfig.load(properties);
    }

    /**
     * Test Case: validate the configuration.
     */
    @Test
    public void testValidConfiguration() throws IOException {
        Map<String, Object> emptyMap = Collections.emptyMap();
        RandomConnectorConfig config = RandomConnectorConfig.load(emptyMap);
        assertNull("RandomSeed should not be set", config.getRandomSeed());
        assertNull("MaxMessageSize should not be set", config.getRandomSeed());
        try {
            config.validate();
            fail("Should fail if `maxMessageSize is not provided");
        } catch (NullPointerException npe) {
            // expected
        }
    }

}
