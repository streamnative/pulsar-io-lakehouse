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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * The configuration class for {@link RandomConnector}.
 */
@Getter
@EqualsAndHashCode
@ToString
public class RandomConnectorConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The seed used for creating a RANDOM instance.
     */
    private Long randomSeed;

    /**
     * Max message size for generating the record.
     */
    private Integer maxMessageSize;

    /**
     * Validate if the configuration is valid.
     */
    public void validate() {
        Objects.requireNonNull(maxMessageSize, "No `maxMessageSize` is provided");
    }

    /**
     * Load the configuration from provided properties.
     *
     * @param config property map
     * @return a loaded {@link RandomConnectorConfig}.
     * @throws IOException when fail to load the configuration from provided properties
     */
    public static RandomConnectorConfig load(Map<String, Object> config) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(config), RandomConnectorConfig.class);
    }


}
