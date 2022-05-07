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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;

/**
 * partitioned delta parquet file writer.
 *
 */
@Slf4j
public class PartitionedDeltaParquetFileWriter implements DeltaParquetWriter {
    private final Map<String, DeltaParquetFileWriter> writerMap = new ConcurrentHashMap<>();
    private final String tablePath;
    private Schema schema;
    private final List<String> partitionColumns;
    private final Configuration configuration;
    private final String compression;

    public PartitionedDeltaParquetFileWriter(Configuration configuration, String tablePath,
                                             List<String> partitionColumns, String compression, Schema schema) {
        this.configuration = configuration;
        this.tablePath = tablePath;
        this.partitionColumns = partitionColumns;
        this.schema = schema;
        this.compression = compression;
    }

    @Override
    public List<FileStat> closeAndFlush() throws IOException {
        List<FileStat> fileStats = new ArrayList<>();
        for (DeltaParquetWriter writer : writerMap.values()) {
            List<FileStat> stats = writer.closeAndFlush();
            if (stats == null) {
                continue;
            }
            fileStats.addAll(stats);
        }

        return fileStats;
    }

    @Override
    public void writeToParquetFile(GenericRecord record) throws IOException {
        String partitionValue = getPartitionValuePath(record, partitionColumns);
        DeltaParquetFileWriter writer = writerMap.get(partitionValue);
        if (writer == null) {
            writer = new DeltaParquetFileWriter(configuration, tablePath,
                compression, schema, partitionValue, getPartitionValues(record, partitionColumns));
            writerMap.put(partitionValue, writer);
        }

        writer.writeToParquetFile(record);
    }

    public static Map<String, String> getPartitionValues(GenericRecord genericRecord, List<String> partitionColumns) {
        Map<String, String> partitionValues = new ConcurrentHashMap<>();
        if (partitionColumns == null || partitionColumns.isEmpty()) {
            return partitionValues;
        }
        for (String partitionColumn : partitionColumns) {
            Schema.Field field = genericRecord.getSchema().getField(partitionColumn);
            if (field == null) {
                continue;
            }
            partitionValues.put(partitionColumn, String.valueOf(genericRecord.get(field.name())));
        }
        return partitionValues;
    }

    public static String getPartitionValuePath(GenericRecord genericRecord, List<String> partitionColumns) {
        if (partitionColumns == null || partitionColumns.isEmpty()) {
            return "";
        }

        StringBuilder pathBuilder = new StringBuilder();
        boolean first = true;
        for (String column : partitionColumns) {
            Schema.Field field = genericRecord.getSchema().getField(column);
            if (field == null) {
                return "";
            }

            if (!first) {
                pathBuilder.append("/");
            }

            pathBuilder.append(field.name())
                .append("=")
                .append(genericRecord.get(field.name()));
            first = false;
        }
        return pathBuilder.toString();
    }

    @Override
    public void updateSchema(Schema schema) {
        for (DeltaParquetWriter writer : writerMap.values()) {
            writer.updateSchema(schema);
        }

        this.schema = schema;
    }

}
