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

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory;
import org.apache.pulsar.ecosystem.io.SinkConnectorConfig;
import org.apache.pulsar.ecosystem.io.sink.LakehouseWriter;
import org.apache.pulsar.ecosystem.io.sink.hudi.exceptions.HoodieConnectorException;

import java.io.IOException;

@Slf4j
public class HoodieWriter implements LakehouseWriter {

    org.apache.pulsar.ecosystem.io.sink.hudi.BufferedConnectWriter writer;
    org.apache.pulsar.ecosystem.io.sink.hudi.HoodieWriterProvider writerProvider;
    org.apache.pulsar.ecosystem.io.sink.hudi.HoodieSinkConfigs hoodieSinkConfigs;
    private KeyGenerator keyGenerator;

    public HoodieWriter(SinkConnectorConfig sinkConnectorConfig, Schema schema) throws IOException {
        this.hoodieSinkConfigs = org.apache.pulsar.ecosystem.io.sink.hudi.HoodieSinkConfigs.newBuilder()
            .withProperties(sinkConnectorConfig.getProperties())
            .build();
        this.writerProvider = new org.apache.pulsar.ecosystem.io.sink.hudi.HoodieWriterProvider(hoodieSinkConfigs);
        this.keyGenerator = HoodieAvroKeyGeneratorFactory.createKeyGenerator(hoodieSinkConfigs.getProps());
        this.writer = writerProvider.open(schema.toString());
    }

    @Override
    public boolean updateSchema(Schema schema) throws IOException {
        flush();
        writer = writerProvider.open(schema.toString());
        return true;
    }

    @Override
    public void writeAvroRecord(GenericRecord record) throws IOException {
        HoodieRecord hoodieRecord = new HoodieRecord(keyGenerator.getKey(record), new HoodieAvroPayload(Option.of(record)));
        writer.writeHoodieRecord(hoodieRecord);
    }

    @Override
    public boolean flush() {
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
