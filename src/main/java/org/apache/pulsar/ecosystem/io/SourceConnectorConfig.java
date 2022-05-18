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
package org.apache.pulsar.ecosystem.io;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Properties;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.ecosystem.io.common.Category;
import org.apache.pulsar.ecosystem.io.common.FieldContext;
import org.apache.pulsar.ecosystem.io.common.Utils;
import org.apache.pulsar.ecosystem.io.exception.IncorrectParameterException;
import org.apache.pulsar.ecosystem.io.source.delta.DeltaSourceConfig;

/**
 * Abstract class of source connector config, providing common configuration fields for all lakehouse products.
 *
 */
@Slf4j
@Data
public abstract class SourceConnectorConfig implements Serializable {
    private static final long serialVersionUID = 1L;


    public static final int DEFAULT_QUEUE_SIZE = 100_000;
    public static final int DEFAULT_CHECKPOINT_INTERVAL = 30;
    public static final long LATEST = -1;
    public static final long EARLIEST = -2;

    @Category
    public static final String CATEGORY_SOURCE = "Source";

    private static Properties properties = new Properties();

    public static final String HUDI = "hudi";
    public static final String ICEBERG = "iceberg";
    public static final String DELTA = "delta";

    @FieldContext(
        category = CATEGORY_SOURCE,
        doc = "checkpoint interval, time unit: second, Default is 30s"
    )
    int checkpointInterval = DEFAULT_CHECKPOINT_INTERVAL;

    @FieldContext(
        category = CATEGORY_SOURCE,
        doc = "source connector queue size, used for store record before send to pulsar topic. "
            + "Default is 100_000"
    )
    int queueSize = DEFAULT_QUEUE_SIZE;

    @FieldContext(
        category = CATEGORY_SOURCE,
        doc = "Whether fetch the history data of the table. Default is: false"
    )
    Boolean fetchHistoryData = false;

    @FieldContext(
        category = CATEGORY_SOURCE,
        doc = "The start version of the delta lake table to fetch cdc log."
    )
    Long startSnapshotVersion;

    @FieldContext(
        category = CATEGORY_SOURCE,
        doc = "The start time stamp of the delta lake table to fetch cdc log. Time unit: second"
    )
    Long startTimestamp;

    public static SourceConnectorConfig load(Map<String, Object> map) throws IOException, IncorrectParameterException {
        properties.putAll(map);

        String type = (String) map.get("type");
        if (StringUtils.isBlank(type)) {
            String error = "type must be set.";
            log.error("{}", error);
            throw new IllegalArgumentException(error);
        }

        switch (type) {
            case DELTA:
                return Utils.JSON_MAPPER.get().readValue(new ObjectMapper().writeValueAsString(map),
                    DeltaSourceConfig.class);

            default:
                throw new IncorrectParameterException("Unexpected type. Only supports 'delta' type, but got " + type);
        }
    }

    public void validate() throws IllegalArgumentException {
        if (startSnapshotVersion != null && startTimestamp != null) {
            log.error("startSnapshotVersion: {} and startTimeStamp: {} "
                    + "should not be set at the same time.",
                startSnapshotVersion, startTimestamp);
            throw new IllegalArgumentException("startSnapshotVersion and startTimeStamp "
                + "should not be set at the same time.");
        } else if (startSnapshotVersion == null && startTimestamp == null) {
            startSnapshotVersion = LATEST;
        }


        if (queueSize <= 0) {
            log.warn("queueSize: {} should be > 0, using default: {}",
                queueSize, DEFAULT_QUEUE_SIZE);
            queueSize = DEFAULT_QUEUE_SIZE;
        }
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperty(String key, Object value) {
        properties.put(key, value);
    }

    public void setProperties(Properties extraProperties) {
        properties.putAll(extraProperties);
    }

    @Override
    public String toString() {
        try {
            return Utils.JSON_MAPPER.get().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            log.error("Failed to write sink connector config ", e);
            return "";
        }
    }
}
