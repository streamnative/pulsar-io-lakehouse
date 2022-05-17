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

package org.apache.pulsar.ecosystem.io.source.delta;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.ecosystem.io.SourceConnectorConfig;
import org.apache.pulsar.ecosystem.io.common.Category;
import org.apache.pulsar.ecosystem.io.common.FieldContext;
import org.apache.pulsar.ecosystem.io.common.Utils;

/**
 * The configuration class for {@link org.apache.pulsar.ecosystem.io.SourceConnector}.
 */

@Data
@Slf4j
public class DeltaSourceConfig extends SourceConnectorConfig {
    public static final long DEFAULT_MAX_READ_BYTES_SIZE_ONE_ROUND =
        Double.valueOf(Runtime.getRuntime().totalMemory() * 0.2).longValue();
    public static final int DEFAULT_MAX_READ_ROW_COUNT_ONE_ROUND = 100_000;
    public static final int DEFAULT_PARQUET_PARSE_THREADS =
        Runtime.getRuntime().availableProcessors();
    public static final int DEFAULT_CHECKPOINT_INTERVAL = 30;
    public static final long LATEST = -1;
    public static final long EARLIEST = -2;
    public static final int DEFAULT_QUEUE_SIZE = 100_000;

    @Category
    private static final String CATEGORY_SOURCE = "Source";

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

    @FieldContext(
        category = CATEGORY_SOURCE,
        doc = "Whether fetch the history data of the table. Default is: false"
    )
    Boolean fetchHistoryData = false;

    @FieldContext(
        category = CATEGORY_SOURCE,
        required = true,
        doc = "The table path to fetch"
    )
    String tablePath;

    @FieldContext(
        category = CATEGORY_SOURCE,
        doc = "The parallelism of parsing parquet file. Default is the number of cpus"
    )
    int parquetParseThreads = DEFAULT_PARQUET_PARSE_THREADS;

    @FieldContext(
        category = CATEGORY_SOURCE,
        doc = "The max read bytes size in one round. Default is 20% of heap memory"
    )
    long maxReadBytesSizeOneRound = DEFAULT_MAX_READ_BYTES_SIZE_ONE_ROUND;

    @FieldContext(
        category = CATEGORY_SOURCE,
        doc = "The max read number of rows in one round. Default is 1_000_000"
    )
    int maxReadRowCountOneRound = DEFAULT_MAX_READ_ROW_COUNT_ONE_ROUND;

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


    /**
     * Validate if the configuration is valid.
     */
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

        if (StringUtils.isBlank(tablePath)) {
            log.error("tablePath should be set.");
            throw new IllegalArgumentException("tablePath should be set.");
        }

        if (parquetParseThreads > 2 * DEFAULT_PARQUET_PARSE_THREADS
            || parquetParseThreads <= 0) {
            log.warn("parquetParseThreads: {} is out of limit, using default cpus: {}",
                parquetParseThreads, DEFAULT_PARQUET_PARSE_THREADS);
            parquetParseThreads = DEFAULT_PARQUET_PARSE_THREADS;
        }

        if (maxReadBytesSizeOneRound <= 0) {
            log.warn("maxReadBytesSizeOneRound: {} should be > 0, using default: {}",
                maxReadBytesSizeOneRound, DEFAULT_MAX_READ_BYTES_SIZE_ONE_ROUND);
            maxReadBytesSizeOneRound = DEFAULT_MAX_READ_BYTES_SIZE_ONE_ROUND;
        }

        if (maxReadRowCountOneRound <= 0) {
            log.warn("maxReadRowCountOneRound: {} should be > 0, using default: {}",
                maxReadRowCountOneRound, DEFAULT_MAX_READ_ROW_COUNT_ONE_ROUND);
            maxReadRowCountOneRound = DEFAULT_MAX_READ_ROW_COUNT_ONE_ROUND;
        }

        if (queueSize <= 0) {
            log.warn("queueSize: {} should be > 0, using default: {}",
                queueSize, DEFAULT_QUEUE_SIZE);
            queueSize = DEFAULT_QUEUE_SIZE;
        }
    }

    /**
     * Pase DeltaLakeConnectorConfig from map.
     *
     * @param map
     * @return
     * @throws IOException
     */
    public static DeltaSourceConfig load(Map<String, Object> map) throws IOException {
        return Utils.JSON_MAPPER.get().readValue(new ObjectMapper().writeValueAsString(map),
            DeltaSourceConfig.class);
    }

    @Override
    public String toString() {
        try {
            return Utils.JSON_MAPPER.get().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            log.error("Failed to write DeltaLakeConnectorConfig ", e);
            return "";
        }
    }
}
