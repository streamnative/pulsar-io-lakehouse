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
package org.apache.pulsar.ecosystem.io.sink.iceberg;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;

/**
 * Message task writer factory.
 */
public class MessageTaskWriterFactory implements TaskWriterFactory<GenericRecord> {
    private final Table table;
    private final Schema schema;
    private final org.apache.avro.Schema pulsarSchema;
    private final PartitionSpec spec;
    private final FileIO io;
    private final long targetFileSizeBytes;
    private final FileFormat format;
    private final List<Integer> equalityFieldIds;
    private final boolean upsert;
    private final FileAppenderFactory<GenericRecord> appenderFactory;

    private transient OutputFileFactory outputFileFactory;

    public MessageTaskWriterFactory(Table table,
                                    org.apache.avro.Schema pulsarSchema,
                                    long targetFileSizeBytes,
                                    FileFormat format,
                                    List<Integer> equalityFieldIds,
                                    boolean upsert) {
        this.table = table;
        this.schema = table.schema();
        this.pulsarSchema = pulsarSchema;
        this.spec = table.spec();
        this.io = table.io();
        this.targetFileSizeBytes = targetFileSizeBytes;
        this.format = format;
        this.equalityFieldIds = equalityFieldIds;
        this.upsert = upsert;

        if (equalityFieldIds == null || equalityFieldIds.isEmpty()) {
            this.appenderFactory = new PulsarAppenderFactory(schema, table, spec);
        } else {
            this.appenderFactory = new PulsarAppenderFactory(schema, table, spec,
                equalityFieldIds, schema, null);
        }
    }

    @Override
    public void initialize(int partitionId, int attemptId) {
        this.outputFileFactory = OutputFileFactory.builderFor(table, partitionId, attemptId).build();
    }

    @Override
    public TaskWriter<GenericRecord> create() {
        checkNotNull(outputFileFactory,
            "The outputFileFactory shouldn't be null if we have invoked the initialize()");

        if (equalityFieldIds == null || equalityFieldIds.isEmpty()) {
            // Initialize a task writer to write INSERT only.
            if (spec.isUnpartitioned()) {
                return new UnpartitionedWriter<>(spec, format, appenderFactory,
                    outputFileFactory, io, targetFileSizeBytes);
            } else {
                return new MessagePartitionedFanoutWriter(spec, format, appenderFactory,
                    outputFileFactory, io, targetFileSizeBytes, schema, pulsarSchema);
            }
        } else {
            // Initialize a task writer to write both INSERT and equality DELETE.
            // not support yet.
            return null;
            /*
            if (spec.isUnpartitioned()) {
                return new UnpartitionedDeltaWriter(spec, format, appenderFactory, outputFileFactory, io,
                    targetFileSizeBytes, schema, pulsarSchema, equalityFieldIds, upsert);
            } else {
                return new PartitionedDeltaWriter(spec, format, appenderFactory, outputFileFactory, io,
                    targetFileSizeBytes, schema, pulsarSchema, equalityFieldIds, upsert);
            }*/
        }
    }

}
