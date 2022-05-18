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

package org.apache.pulsar.ecosystem.io.parquet;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.ecosystem.io.common.Utils;

/**
 * Parquet writer interface.
 */
public interface DeltaParquetWriter {
    /**
     * file stat.
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Slf4j
    class FileStat {
        private String filePath;
        private Long fileSize;
        private Map<String, String> partitionValues;


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

    void writeToParquetFile(GenericRecord record) throws IOException;

    List<FileStat> closeAndFlush() throws IOException;

    void updateSchema(Schema schema);

}
