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
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.ecosystem.io.common.Category;
import org.apache.pulsar.ecosystem.io.common.FieldContext;
import org.apache.pulsar.ecosystem.io.common.Utils;
import org.apache.pulsar.ecosystem.io.exception.IncorrectParameterException;
import org.apache.pulsar.ecosystem.io.sink.delta.DeltaSinkConnectorConfig;
import org.apache.pulsar.ecosystem.io.sink.iceberg.IcebergSinkConnectorConfig;

/**
 * Abstract class of sink connector config, providing common configuration fields for all lakehouse products.
 */
@Slf4j
@Data
public abstract class SinkConnectorConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private static Properties properties = new Properties();

    public static final int MB = 1024 * 1024;
    public static final int DEFAULT_SINK_CONNECTOR_QUEUE_SIZE = 10_000;
    public static final int DEFAULT_MAX_COMMIT_INTERVAL = 120;
    public static final int DEFAULT_MAX_RECORDS_PER_COMMIT = 10_000_000;
    public static final int DEFAULT_MAX_COMMIT_FAILED_TIMES = 5;

    public static final String HUDI = "hudi";
    public static final String ICEBERG = "iceberg";
    public static final String DELTA = "delta";

    @Category
    protected static final String CATEGORY_SINK = "Sink";

    @FieldContext(
        category = CATEGORY_SINK,
        required = true,
        doc = "Sink connector type, iceberg, hudi or delta"
    )
    String type;

    @FieldContext(
        category = CATEGORY_SINK,
        doc = "Max flush interval in seconds for each batch. Default is 120s "
    )
    int maxCommitInterval = DEFAULT_MAX_COMMIT_INTERVAL;

    @FieldContext(
        category = CATEGORY_SINK,
        doc = "Max records number for each batch to commit. Default is 100_000"
    )
    int maxRecordsPerCommit = DEFAULT_MAX_RECORDS_PER_COMMIT;

    @FieldContext(
        category = CATEGORY_SINK,
        doc = "Max commit fail times to fail the process. Default is 5"
    )
    int maxCommitFailedTimes = DEFAULT_MAX_COMMIT_FAILED_TIMES;

    @FieldContext(
        category = CATEGORY_SINK,
        doc = "sink connector queue size"
    )
    int sinkConnectorQueueSize = DEFAULT_SINK_CONNECTOR_QUEUE_SIZE;

    @FieldContext(
        category = CATEGORY_SINK,
        doc = "Partition columns for delta lake table"
    )
    List<String> partitionColumns = Collections.emptyList();

    static SinkConnectorConfig load(Map<String, Object> map) throws IOException, IncorrectParameterException {
        properties.putAll(map);
        String type = (String) map.get("type");
        if (StringUtils.isBlank(type)) {
            String error = "type must be set.";
            log.error("{}", error);
            throw new IllegalArgumentException(error);
        }

        switch (type.toLowerCase(Locale.ROOT)) {
            case ICEBERG:
                return jsonMapper().readValue(new ObjectMapper().writeValueAsString(map),
                    IcebergSinkConnectorConfig.class);
            case DELTA:
                return jsonMapper().readValue(new ObjectMapper().writeValueAsString(map),
                    DeltaSinkConnectorConfig.class);
            case HUDI:
                return jsonMapper().readValue(new ObjectMapper().writeValueAsString(map),
                    DefaultSinkConnectorConfig.class);
            default:
                throw new IncorrectParameterException("Unexpected type. Only supports 'iceberg', 'delta', and 'hudi', "
                    + "but got " + type);
        }
    }

    public static ObjectMapper jsonMapper() {
        return Utils.JSON_MAPPER.get();
    }

    public void validate() throws IllegalArgumentException {
        if (StringUtils.isBlank(type)
            || (!HUDI.equals(type.toLowerCase(Locale.ROOT))
                && !ICEBERG.equals(type.toLowerCase(Locale.ROOT))
                && !DELTA.equals(type.toLowerCase(Locale.ROOT)))) {
            String error = "type must be set and must be one of hudi, iceberg or delta";
            log.error("{}", error);
            throw new IllegalArgumentException(error);
        }

        type = type.toUpperCase(Locale.ROOT);

        if (maxCommitInterval <= 0) {
            log.warn("maxFlushInterval: {} should be > 0, using default: {}",
                maxCommitInterval, DEFAULT_MAX_COMMIT_INTERVAL);
            maxCommitInterval = DEFAULT_MAX_COMMIT_INTERVAL;
        }

        if (sinkConnectorQueueSize <= 0) {
            log.warn("sinkConnectorQueueSize: {} should be > 0, using default: {}",
                sinkConnectorQueueSize, DEFAULT_SINK_CONNECTOR_QUEUE_SIZE);
            sinkConnectorQueueSize = DEFAULT_SINK_CONNECTOR_QUEUE_SIZE;
        }

        if (maxRecordsPerCommit <= 0) {
            log.warn("maxRecordsPerCommit: {} should be > 0, using default: {}",
                maxRecordsPerCommit, DEFAULT_MAX_RECORDS_PER_COMMIT);
            maxRecordsPerCommit = DEFAULT_MAX_RECORDS_PER_COMMIT;
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
            return jsonMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            log.error("Failed to write sink connector config ", e);
            return "";
        }
    }

    public static class DefaultSinkConnectorConfig extends SinkConnectorConfig { }
}
