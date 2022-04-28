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
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.ecosystem.io.common.Category;
import org.apache.pulsar.ecosystem.io.common.FieldContext;
import org.apache.pulsar.ecosystem.io.sink.iceberg.IcebergSinkConnectorConfig;

/**
 * Abstract class of sink connector config, providing common configuration fields for all lakehouse products.
 */
@Slf4j
@Data
public abstract class SinkConnectorConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    protected static final int MB = 1024 * 1024;
    protected static final int DEFAULT_SINK_CONNECTOR_QUEUE_SIZE = 10_000;
    protected static final int DEFAULT_MAX_COMMIT_INTERVAL = 120;
    protected static final String DEFAULT_CATALOG_IMPL = "hadoopCatalog";

    public static final String HUDI_SINK = "hudi";
    public static final String ICEBERG_SINK = "iceberg";
    public static final String DELTA_SINK = "delta";

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
        doc = "max flush interval in seconds for each batch "
    )
    int maxCommitInterval = DEFAULT_MAX_COMMIT_INTERVAL;

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

    static SinkConnectorConfig load(Map<String, Object> map) throws IOException {
        String type = (String) map.get("type");
        if (StringUtils.isBlank(type)) {
            String error = "type must be set.";
            log.error("{}", error);
            throw new IllegalArgumentException(error);
        }

        switch (type) {
            case ICEBERG_SINK:
                return jsonMapper().readValue(new ObjectMapper().writeValueAsString(map),
                    IcebergSinkConnectorConfig.class);
        }
        return null;
    }

    static ObjectMapper jsonMapper() {
        return ObjectMapperFactory.getThreadLocal();
    }

    public void validate() throws IllegalArgumentException {
        if (StringUtils.isBlank(type)
            || (!HUDI_SINK.equals(type.toLowerCase(Locale.ROOT))
                && !ICEBERG_SINK.equals(type.toLowerCase(Locale.ROOT))
                && !DELTA_SINK.equals(type.toLowerCase(Locale.ROOT)))) {
            String error = "type must be set and must be one of hudi, iceberg or delta";
            log.error("{}", error);
            throw new IllegalArgumentException(error);
        }

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

}