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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.pulsar.ecosystem.io.lakehouse.SinkConnectorConfig;
import org.apache.pulsar.ecosystem.io.lakehouse.common.FieldContext;

/**
 * Delta lake sink connector config.
 */
@Slf4j
@Data
public class DeltaSinkConnectorConfig extends SinkConnectorConfig {
    protected static final String DEFAULT_PARQUET_COMPRESSION_TYPE = CompressionCodecName.SNAPPY.name();
    protected static final String DEFAULT_DELTA_FILE_TYPE = "parquet";
    protected static final String DEFAULT_APP_ID = "pulsar-delta-sink-connector";

    @FieldContext(
        category = CATEGORY_SINK,
        required = true,
        doc = "delta lake table path to write"
    )
    String tablePath;

    @FieldContext(
        category = CATEGORY_SINK,
        doc = "Delta parquet file compression type. Default is SNAPPY"
    )
    String compression = DEFAULT_PARQUET_COMPRESSION_TYPE;

    @FieldContext(
        category = CATEGORY_SINK,
        doc = "Delta file type. Default is parquet"
    )
    String deltaFileType = DEFAULT_DELTA_FILE_TYPE;

    @FieldContext(
        category = CATEGORY_SINK,
        doc = "Delta appid. Default is: pulsar-delta-sink-connector"
    )
    String appId = DEFAULT_APP_ID;

    public void validate() throws IllegalArgumentException {
        super.validate();

        try {
            CompressionCodecName.valueOf(compression.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            log.warn("Don't support compression type: {}, use default: {}",
                compression, DEFAULT_PARQUET_COMPRESSION_TYPE);
            compression = DEFAULT_PARQUET_COMPRESSION_TYPE;
        }
    }

    public static DeltaSinkConnectorConfig load(Map<String, Object> map) throws IOException {
        return jsonMapper().readValue(new ObjectMapper().writeValueAsString(map), DeltaSinkConnectorConfig.class);
    }


    @Override
    public String toString() {
        try {
            return jsonMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            log.error("Failed to write DeltaLakeSinkConnectorConfig ", e);
            return "";
        }
    }
}
