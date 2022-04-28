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

import java.io.Serializable;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetAvroWriter;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * Pulsar appender factory.
 */
@Slf4j
public class PulsarAppenderFactory implements FileAppenderFactory<GenericRecord>, Serializable {
    private static final long serialVersionUID = 1L;

    private final Schema schema;
    private final Table table;
    private final PartitionSpec spec;
    private final int[] equalityFieldIds;
    private final Schema eqDeleteRowSchema;
    private final Schema posDeleteRowSchema;
    private SchemaInfo eqDeletePulsarSchema = null;
    private SchemaInfo posDeletePulsarSchema = null;

    public PulsarAppenderFactory(Schema schema,
                                 Table table,
                                 PartitionSpec spec) {
        this(schema, table, spec, null, null, null);
    }

    public PulsarAppenderFactory(Schema schema,
                                 Table table,
                                 PartitionSpec spec,
                                 int[] equalityFieldIds,
                                 Schema eqDeleteRowSchema,
                                 Schema posDeleteRowSchema) {
        this.schema = schema;
        this.table = table;
        this.spec = spec;
        this.equalityFieldIds = equalityFieldIds;
        this.posDeleteRowSchema = posDeleteRowSchema;
        this.eqDeleteRowSchema = eqDeleteRowSchema;
    }

    @Override
    public FileAppender<GenericRecord> newAppender(OutputFile outputFile, FileFormat format) {
        MetricsConfig  metricsConfig = MetricsConfig.forTable(table);

        try {
            switch (format) {
                case PARQUET:
                    return Parquet.write(outputFile)
                        .createWriterFunc(ParquetAvroWriter::buildWriter)
                        .setAll(table.properties())
                        .metricsConfig(metricsConfig)
                        .schema(schema)
                        .overwrite()
                        .build();
            }
        } catch (Exception e) {
            log.error("failed to get parquet writer. ", e);
        }
        return null;
    }

    @Override
    public DataWriter<GenericRecord> newDataWriter(EncryptedOutputFile file, FileFormat format, StructLike partition) {
        return new DataWriter<>(
            newAppender(file.encryptingOutputFile(), format), format,
            file.encryptingOutputFile().location(), spec, partition, file.keyMetadata());
    }

    @Override
    public EqualityDeleteWriter<GenericRecord> newEqDeleteWriter(EncryptedOutputFile outputFile, FileFormat format,
                                                          StructLike partition) {
        // TODO
        return null;
    }

    @Override
    public PositionDeleteWriter<GenericRecord> newPosDeleteWriter(EncryptedOutputFile outputFile, FileFormat format,
                                                           StructLike partition) {
        return null;
    }
}
