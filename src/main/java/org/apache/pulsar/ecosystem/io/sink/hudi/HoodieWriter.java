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
package org.apache.pulsar.ecosystem.io.sink.hudi;

import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory;
import org.apache.pulsar.ecosystem.io.SinkConnectorConfig;
import org.apache.pulsar.ecosystem.io.exception.HoodieConnectorException;
import org.apache.pulsar.ecosystem.io.sink.LakehouseWriter;

@Slf4j
public class HoodieWriter implements LakehouseWriter {

    BufferedConnectWriter writer;
    HoodieWriterProvider writerProvider;
    HoodieSinkConfigs hoodieSinkConfigs;
    private KeyGenerator keyGenerator;

    public HoodieWriter(SinkConnectorConfig sinkConnectorConfig, Schema schema) throws HoodieConnectorException {
        this.hoodieSinkConfigs = HoodieSinkConfigs.newBuilder()
            .withProperties(sinkConnectorConfig.getProperties())
            .build();
        try {
            this.writerProvider = new HoodieWriterProvider(hoodieSinkConfigs);
            this.keyGenerator = HoodieAvroKeyGeneratorFactory.createKeyGenerator(hoodieSinkConfigs.getProps());
        } catch (IOException e) {
            throw new HoodieConnectorException("Failed to initialize hoodie writer", e);
        }
        this.writer = writerProvider.open(schema.toString());
    }

    @Override
    public boolean updateSchema(Schema schema) throws IOException {
        if (writer.getConfig().getSchema().equals(schema.toString())) {
            log.info("The schema is not changed, continue to write records into current writer");
            return true;
        }
        log.info("Schema updated, trigger flush and switch new writer with new schema to writer");
        flush();
        writer = writerProvider.open(schema.toString());
        return true;
    }

    @Override
    public void writeAvroRecord(GenericRecord record) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("Writing generic records to the hudi: {}", record);
        }
        HoodieRecord hoodieRecord = new HoodieRecord(
            keyGenerator.getKey(record), new HoodieAvroPayload(Option.of(record)));
        writer.writeHoodieRecord(hoodieRecord);
    }

    @Override
    public boolean flush() {
        if (log.isDebugEnabled()) {
            log.debug("Flush current records");
        }
        try {
            writer.flushRecords();
            return true;
        } catch (HoodieConnectorException e) {
            log.error("Failed to flush records to the hudi table", e);
            return false;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            writer.close();
        } catch (HoodieConnectorException e) {
            throw new IOException(e);
        }
    }
}
