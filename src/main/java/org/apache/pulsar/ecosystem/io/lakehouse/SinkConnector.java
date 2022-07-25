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
package org.apache.pulsar.ecosystem.io.lakehouse;

import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.ecosystem.io.lakehouse.exception.LakehouseConnectorException;
import org.apache.pulsar.ecosystem.io.lakehouse.sink.PulsarSinkRecord;
import org.apache.pulsar.ecosystem.io.lakehouse.sink.SinkWriter;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

/**
 * Sink Connector.
 */
@Slf4j
@Data
public class SinkConnector implements Sink<GenericObject> {
    private SinkConnectorConfig sinkConnectorConfig;
    private LinkedBlockingQueue<PulsarSinkRecord> messages;
    private ExecutorService executor;
    private SinkWriter writer;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        log.info("Starting lakehouse sink connector, topicName: {}, connector name: {}, subScriptionType: {}",
            sinkContext.getInputTopics(), sinkContext.getSinkName(), sinkContext.getSubscriptionType());

        if (sinkContext.getSubscriptionType() != SubscriptionType.Failover
            && sinkContext.getSubscriptionType() != SubscriptionType.Exclusive) {
            String msg = "Lakehouse sink connector only support accumulative acknowledge, "
                + "so only support Failover or Exclusive subscription type.";
            log.error(msg);
            throw new IllegalArgumentException(msg);
        }

        this.sinkConnectorConfig = SinkConnectorConfig.load(config);
        this.sinkConnectorConfig.validate();
        log.info("{} sink connector config: {}", this.sinkConnectorConfig.getType(), this.sinkConnectorConfig);

        messages = new LinkedBlockingQueue<>(this.sinkConnectorConfig.getSinkConnectorQueueSize());
        writer = new SinkWriter(sinkConnectorConfig, messages);
        executor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("lakehouse-io"));
        executor.execute(writer);
    }

    @Override
    public void write(Record<GenericObject> record) throws Exception {
        if (log.isDebugEnabled()) {
            record.getMessage().ifPresent(m -> {
                log.debug("Received message: {}", m.getMessageId());
            });
        }
        while (!messages.offer(new PulsarSinkRecord(record), 1, TimeUnit.SECONDS)) {
            if (!writer.isRunning()) {
                String err = "Exit caused by lakehouse writer stop working";
                log.error("{}", err);
                throw new LakehouseConnectorException(err);
            }
            if (log.isDebugEnabled()) {
                log.debug("pending on adding into the blocking queue.");
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (writer != null) {
            writer.close();
        }
        if (executor != null) {
            executor.shutdown();
        }
        log.info("{} sink connector closed.", sinkConnectorConfig.getType());
    }
}
