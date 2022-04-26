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

import java.util.Map;
import java.util.Optional;
import java.util.Random;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;

/**
 * A source connector that generate randomized words.
 */
@Getter(AccessLevel.PACKAGE)
public class RandomConnector implements Source<byte[]> {

    private RandomConnectorConfig config;
    private Random random;

    @Override
    public void open(Map<String, Object> map,
                     SourceContext sourceContext) throws Exception {
        if (null != config) {
            throw new IllegalStateException("Connector is already open");
        }

        // load the configuration and validate it
        this.config = RandomConnectorConfig.load(map);
        this.config.validate();

        // create the random instance used by the connector
        this.random = new Random(null == config.getRandomSeed() ? System.currentTimeMillis() : config.getRandomSeed());
    }

    private void checkConnectorOpen() {
        if (null == config) {
            throw new IllegalStateException("Connector is not open yet");
        }
    }

    @Override
    public Record<byte[]> read() throws Exception {
        checkConnectorOpen();

        return new Record<byte[]>() {
            @Override
            public Optional<String> getKey() {
                return Optional.empty();
            }

            @Override
            public byte[] getValue() {
                int numBytes = random.nextInt(1024);
                byte[] data = new byte[numBytes];
                random.nextBytes(data);
                return data;
            }
        };
    }

    @Override
    public void close() throws Exception {
        // no-op
    }

}
