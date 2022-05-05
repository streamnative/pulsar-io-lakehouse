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
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.apache.pulsar.ecosystem.io.exception.CommitFailedException;
import org.apache.pulsar.ecosystem.io.sink.iceberg.IcebergWriter;
import org.apache.pulsar.functions.api.Record;

/**
 * Writer thread. Fetch records from queue, and write them into lakehouse table.
 */
@Slf4j
public class SinkWriter implements Runnable {
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
    private long recordsCnt;
    private final long maxRecordsPerCommit;
    private final int maxCommitFailedTimes;
    private final AtomicBoolean shouldFail;


    public SinkWriter(SinkConnector sink) {
        this.sink = sink;
        this.running = true;
        this.parser = new Schema.Parser();
        this.currentSchemaDefinition = null;
        this.writer = null;
        this.currentSchema = null;
        this.lastRecord = null;
        this.schemaUpdated = false;
        this.timeIntervalPerCommit = TimeUnit.SECONDS.toMillis(sink.getConfig().getMaxCommitInterval());
        this.maxRecordsPerCommit = sink.getConfig().getMaxRecordsPerCommit();
        this.maxCommitFailedTimes = sink.getConfig().getMaxCommitFailedTimes();
        this.lastCommitTime = System.currentTimeMillis();
        this.recordsCnt = 0;
        this.shouldFail = sink.getShouldFail();
    }

    public void run() {
        int commitFailedCnt = 0;
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
                        lastCommitTime = System.currentTimeMillis();
                        recordsCnt = 0;
                        commitFailedCnt = 0;
                    }
                    schemaUpdated = false;
                }

                writer.writeAvroRecord(genericRecord);
                lastRecord = record;

                if (needCommit()) {
                    if (writer.flush()) {
                        lastRecord.ack();
                        lastCommitTime = System.currentTimeMillis();
                        recordsCnt = 0;
                        commitFailedCnt = 0;
                    } else {
                        commitFailedCnt++;

                        if (commitFailedCnt > maxCommitFailedTimes) {
                            String errmsg = "Failed commit times: " + commitFailedCnt
                                + " exceed maxCommitFailedTimes: " + maxCommitFailedTimes;
                            log.error("{}", errmsg);
                            throw new CommitFailedException(errmsg);
                        }
                    }
                }
            } catch (InterruptedException | IOException e) {
                log.error("process record failed. ", e);
                // fail the sink connector.
                shouldFail.set(true);
            }
        }
    }

    private boolean needCommit() {
        return lastCommitTime - System.currentTimeMillis() > timeIntervalPerCommit
            || recordsCnt > maxRecordsPerCommit;
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
        if (writer != null) {
            writer.close();
        }

        if (lastRecord != null) {
            lastRecord.ack();
        }
    }

    protected LakehouseWriter createWriter(SinkConnectorConfig config, Schema schema) {
        switch (config.getType()) {
            case ICEBERG_SINK:
                return new IcebergWriter(config, schema);
        }

        return null;
    }
}
