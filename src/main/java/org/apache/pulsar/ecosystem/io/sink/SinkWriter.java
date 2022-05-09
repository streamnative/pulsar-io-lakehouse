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

import com.google.common.base.Strings;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.pulsar.ecosystem.io.SinkConnectorConfig;
import org.apache.pulsar.ecosystem.io.common.SchemaConverter;
import org.apache.pulsar.ecosystem.io.exception.CommitFailedException;
import org.apache.pulsar.ecosystem.io.exception.LakehouseWriterException;

/**
 * Writer thread. Fetch records from queue, and write them into lakehouse table.
 */
@Slf4j
public class SinkWriter implements Runnable {
    private final SinkConnectorConfig sinkConnectorConfig;
    private LakehouseWriter writer;
    private final Schema.Parser parser;
    private Schema currentPulsarSchema;
    private Schema avroSchema;
    private PulsarSinkRecord lastRecord;
    private final GenericDatumReader<GenericRecord> datumReader;
    private final long timeIntervalPerCommit;
    private long lastCommitTime;
    private long recordsCnt;
    private final long maxRecordsPerCommit;
    private final int maxCommitFailedTimes;
    private volatile boolean running;
    private final LinkedBlockingQueue<PulsarSinkRecord> messages;


    public SinkWriter(SinkConnectorConfig sinkConnectorConfig, LinkedBlockingQueue<PulsarSinkRecord> messages) {
        this.messages = messages;
        this.sinkConnectorConfig = sinkConnectorConfig;
        this.parser = new Schema.Parser();
        this.datumReader = new GenericDatumReader<>();
        this.timeIntervalPerCommit = TimeUnit.SECONDS.toMillis(sinkConnectorConfig.getMaxCommitInterval());
        this.maxRecordsPerCommit = sinkConnectorConfig.getMaxRecordsPerCommit();
        this.maxCommitFailedTimes = sinkConnectorConfig.getMaxCommitFailedTimes();
        this.lastCommitTime = System.currentTimeMillis();
        this.recordsCnt = 0;
        this.running = true;
    }

    public void run() {
        int commitFailedCnt = 0;
        while (running) {
            try {
                PulsarSinkRecord pulsarSinkRecord = messages.poll(100, TimeUnit.MILLISECONDS);
                if (pulsarSinkRecord == null) {
                    continue;
                }

                String schemaStr = pulsarSinkRecord.getSchema();
                if (Strings.isNullOrEmpty(schemaStr.trim())) {
                    log.error("Failed to get schema from record, skip the record");
                    continue;
                }
                Schema schema = parser.parse(schemaStr);
                if (currentPulsarSchema == null || !currentPulsarSchema.equals(schema)) {
                    currentPulsarSchema = schema;
                    avroSchema = SchemaConverter.convertPulsarAvroSchemaToNonNullSchema(currentPulsarSchema);
                    if (log.isDebugEnabled()) {
                        log.debug("new schema after convert: {}", avroSchema);
                    }
                    datumReader.setSchema(schema);
                    datumReader.setExpected(schema);
                    if (getOrCreateWriter().updateSchema(schema)) {
                        resetStatus();
                        commitFailedCnt = 0;
                    }
                }
                Optional<GenericRecord> avroRecord = convertToAvroGenericData(pulsarSinkRecord);
                if (avroRecord.isPresent()) {
                    getOrCreateWriter().writeAvroRecord(avroRecord.get());
                    lastRecord = pulsarSinkRecord;
                    if (needCommit()) {
                        if (getOrCreateWriter().flush()) {
                            resetStatus();
                            commitFailedCnt = 0;
                        } else {
                            commitFailedCnt++;
                            log.warn("Commit records failed {} times", commitFailedCnt);
                            if (commitFailedCnt > maxCommitFailedTimes) {
                                String errMsg = "Exceed the max commit failed times, the allowed max failure times is "
                                    + maxCommitFailedTimes;
                                log.error(errMsg);
                                throw new CommitFailedException(errMsg);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                log.error("process record failed. ", e);
                // fail the sink connector.
                running = false;
            }
        }
    }
    private LakehouseWriter getOrCreateWriter() throws LakehouseWriterException {
        if (writer != null) {
            return writer;
        }
        writer = LakehouseWriter.getWriter(sinkConnectorConfig, currentPulsarSchema);
        return writer;
    }

    private void resetStatus() throws IOException {
        if (lastRecord != null) {
            lastRecord.ack();
        }
        lastCommitTime = System.currentTimeMillis();
        recordsCnt = 0;
    }

    private boolean needCommit() {
        return lastCommitTime - System.currentTimeMillis() > timeIntervalPerCommit
            || recordsCnt > maxRecordsPerCommit;
    }

    public Optional<GenericRecord> convertToAvroGenericData(PulsarSinkRecord record) throws IOException {
        switch (record.getSchemaType()) {
            case AVRO:
                return Optional.of((GenericRecord) record.getNativeObject());
            case JSON:
                Decoder decoder = DecoderFactory.get().jsonDecoder(avroSchema, record.getNativeObject().toString());
                return Optional.of(datumReader.read(null, decoder));
            default:
                log.error("not support this kind of schema: {}", record.getSchemaType());
                return Optional.empty();
        }
    }

    public boolean isRunning() {
        return running;
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
}
