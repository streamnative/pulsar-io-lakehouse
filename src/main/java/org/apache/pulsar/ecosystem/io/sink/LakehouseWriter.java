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
package org.apache.pulsar.ecosystem.io.sink;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.ecosystem.io.SinkConnectorConfig;
import org.apache.pulsar.ecosystem.io.exception.LakehouseConnectorException;
import org.apache.pulsar.ecosystem.io.exception.LakehouseWriterException;
import org.apache.pulsar.ecosystem.io.sink.delta.DeltaWriter;
import org.apache.pulsar.ecosystem.io.sink.iceberg.IcebergWriter;

/**
 * Lakehouse writer interface.
 */
public interface LakehouseWriter {

    static LakehouseWriter getWriter(SinkConnectorConfig config, Schema schema) throws LakehouseWriterException {
        switch (config.getType()) {
            case SinkConnectorConfig.DELTA_SINK:
                return new DeltaWriter(config, schema);
            case SinkConnectorConfig.ICEBERG_SINK:
                return new IcebergWriter(config, schema);
            case SinkConnectorConfig.HUDI_SINK:
            default:
                throw new LakehouseWriterException("Unknown type of the Lakehouse writer. Expected 'delta', 'iceberg',"
                    + " or 'hudi', but got " + config.getType());
        }
    }

    /**
     * Update lakehouse table's schema.
     * @param schema
     * @return
     */
    boolean updateSchema(Schema schema) throws IOException, LakehouseConnectorException;

    /**
     * Write avro record into lakehouse.
     * @param record
     */
    void writeAvroRecord(GenericRecord record) throws IOException;

    /**
     * Flush record into lakehouse table.
     * @return
     */
    boolean flush();

    /**
     * Close the writer.
     *
     */
    void close() throws IOException;
}
