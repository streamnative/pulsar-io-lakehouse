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

import static org.apache.pulsar.ecosystem.io.SinkConnectorConfig.ICEBERG_SINK;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.ecosystem.io.SinkConnector;
import org.apache.pulsar.ecosystem.io.SinkConnectorConfig;
import org.apache.pulsar.ecosystem.io.common.SchemaConverter;
import org.apache.pulsar.ecosystem.io.sink.iceberg.IcebergWriter;
import org.apache.pulsar.functions.api.Record;

/**
 * Writer thread. Fetch records from queue, and write them into lakehouse table.
 */
@Slf4j
public class WriterThread extends Thread {
    private final SinkConnector sink;
    private volatile boolean running;
    private LakehouseWriter writer;
    private final Schema.Parser parser;
    private String currentSchemaDefinition;
    private Schema currentSchema;
    private boolean schemaUpdated;
    private Schema avroSchema;
    private PulsarSinkRecord lastRecord;
    private DatumReader<GenericRecord> datumReader;
    private final long timeIntervalPerCommit;
    private long lastCommitTime;


    public WriterThread(SinkConnector sink) {
        this.sink = sink;
        this.running = true;
        this.parser = new Schema.Parser();
        this.currentSchemaDefinition = null;
        this.writer = null;
        this.currentSchema = null;
        this.schemaUpdated = false;
        this.timeIntervalPerCommit = TimeUnit.SECONDS.toMillis(sink.getConfig().getMaxCommitInterval());
        this.lastCommitTime = System.currentTimeMillis();
    }

    public void run() {
        while (running) {
            try {
                PulsarSinkRecord record = sink.getQueue().poll(100, TimeUnit.MILLISECONDS);
                if (record == null) {
                    continue;
                }

                String schemaStr = record.getSchema();
                if (currentSchemaDefinition == null
                    || (!StringUtils.isBlank(schemaStr) && !schemaStr.equals(currentSchemaDefinition))) {
                    // update current schema;
                    currentSchemaDefinition = schemaStr;
                    currentSchema = parser.parse(currentSchemaDefinition);
                    schemaUpdated = true;
                }
                lastRecord = record;
                GenericRecord genericRecord = convertToAvroGenericData(record.getRecord());

                if (genericRecord == null) {
                    continue;
                }

                if (writer == null) {
                    writer = createWriter(sink.getConfig(), currentSchema);
                }

                if (schemaUpdated) {
                    if (writer.updateSchema(currentSchema)) {
                        lastRecord.ack();
                    }
                    schemaUpdated = false;
                }

                writer.writeAvroRecord(genericRecord);
                if (lastCommitTime - System.currentTimeMillis() > timeIntervalPerCommit && writer.flush()) {
                    lastRecord.ack();
                    lastCommitTime = System.currentTimeMillis();
                }
            } catch (InterruptedException | IOException e) {
                log.error("process record failed. ", e);
                // TODO add retry logic.
            }
        }
    }

    protected GenericRecord convertToAvroGenericData(Record<org.apache.pulsar.client.api.schema.GenericRecord> record)
        throws IOException {
        switch (record.getValue().getSchemaType()) {
            case AVRO:
                return (GenericRecord) record.getValue().getNativeObject();
            case JSON:
                if (datumReader == null || schemaUpdated) {
                    avroSchema = SchemaConverter.convertPulsarAvroSchemaToNonNullSchema(currentSchema);
                    if (log.isDebugEnabled()) {
                        log.debug("new schema after convert: {}", avroSchema);
                    }
                    datumReader = new GenericDatumReader<>(avroSchema, avroSchema);
                }
                Decoder decoder = new DecoderFactory().jsonDecoder(avroSchema,
                    record.getValue().getNativeObject().toString());
                return datumReader.read(null, decoder);
            default:
                log.error("not support this kind of schema: {}", record.getValue().getSchemaType());
        }

        return null;
    }

    public void close() throws IOException {
        running = false;
        writer.close();
        lastRecord.ack();
    }

    protected LakehouseWriter createWriter(SinkConnectorConfig config, Schema schema) {
        switch (config.getType()) {
            case ICEBERG_SINK:
                return new IcebergWriter(config, schema);
        }

        return null;
    }
}