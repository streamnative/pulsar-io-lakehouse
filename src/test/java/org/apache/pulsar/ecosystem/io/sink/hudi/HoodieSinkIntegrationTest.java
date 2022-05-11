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

import com.google.gson.Gson;
import java.io.File;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.TopicMessageImpl;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.ecosystem.io.SinkConnector;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.instance.SinkRecord;
import org.apache.pulsar.functions.source.PulsarRecord;
import org.apache.pulsar.io.core.SinkContext;
import org.junit.Test;
import org.slf4j.Logger;


@Slf4j
public class HoodieSinkIntegrationTest {

    private static final Gson gson = new Gson();

    @Test
    public void integrationTests() throws Exception {
        String pulsarServiceUrl = "pulsar://localhost:6650";
        String pulsarTestTopic = "hudi-connector-test";
        String subscriptionName = "hudi-sub";
        int maxNumMessage = 10;
        PulsarClient pulsarClient = PulsarClient.builder()
            .serviceUrl(pulsarServiceUrl).build();
        Producer<Data> producer = pulsarClient.newProducer(AvroSchema.of(Data.class))
            .topic(pulsarTestTopic)
            .create();
        Consumer<GenericRecord> consumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
            .subscriptionName(subscriptionName)
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .topic(pulsarTestTopic)
            .subscribe();

        produceMessages(producer, maxNumMessage);

        log.info("Sleep 5 seconds to wait for the topic creates");
        TimeUnit.SECONDS.sleep(5);
        log.info("Continue to process the hoodie sink connector");

        SinkConnector hoodieSink = new SinkConnector();
        DummySinkContext context = new DummySinkContext(Collections.singletonList(pulsarTestTopic));
        hoodieSink.open(loadConfigurations(), context);

        for (int i = 0; i < maxNumMessage; i++) {
            Message<GenericRecord> message = consumer.receive();
            Schema<GenericRecord> schema = null;
            if (message instanceof MessageImpl) {
                MessageImpl impl = (MessageImpl) message;
                schema = impl.getSchemaInternal();
            } else if (message instanceof TopicMessageImpl) {
                TopicMessageImpl impl = (TopicMessageImpl) message;
                schema = impl.getSchemaInternal();
            }

            Record record = PulsarRecord.<GenericRecord>builder()
                .message(message)
                .schema(schema)
                .topicName(pulsarTestTopic).build();
            Record<GenericObject> sinkRecord = new SinkRecord<>(record, record.getValue());

            log.info("Receiving message and process by connector: " + sinkRecord.getValue().toString());
            hoodieSink.write(sinkRecord);
        }

        log.info("Waiting more time to flush the record");
        TimeUnit.SECONDS.sleep(15);
        log.info("Record flushed");

        consumer.close();
        producer.close();
        pulsarClient.close();
    }

    private void produceMessages(Producer producer, int maxNumMessages) {
        new Thread(() -> {
            log.info("Starting produce dummy messages to the topic");
            for (int i = 0; i < maxNumMessages; i++) {
                Data data = new Data();
                data.id = i;
                data.date = new Date().toString();
                try {
                    log.info("Sending ---- " + gson.toJson(data));
                    producer.send(data);
                } catch (PulsarClientException e) {
                    log.error("Failed to produce data to the topic, id is :" + i, e);
                }
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    log.error("Interrupted when waiting for sending next message", e);
                }
            }
        }).start();
    }

    private Map<String, Object> loadConfigurations() throws Exception {
        File configFile = new File(this.getClass().getClassLoader()
            .getResource("pulsar.hoodie.sink.conf.json").toURI());
        Gson gson = new Gson();
        try (FileReader reader = new FileReader(configFile)) {
            return gson.fromJson(reader, HashMap.class);
        }
    }

    public static class Data {
        int id;
        String date;
    }

    private static class DummySinkContext implements SinkContext {
        private final List<String> topics;

        public DummySinkContext(List<String> topics) {
            this.topics = topics;
        }

        @Override
        public Collection<String> getInputTopics() {
            return topics;
        }

        @Override
        public String getSinkName() {
            return null;
        }

        @Override
        public int getInstanceId() {
            return 0;
        }

        @Override
        public int getNumInstances() {
            return 0;
        }

        @Override
        public void recordMetric(String metricName, double value) {

        }

        @Override
        public String getTenant() {
            return null;
        }

        @Override
        public String getNamespace() {
            return null;
        }

        @Override
        public Logger getLogger() {
            return null;
        }

        @Override
        public String getSecret(String secretName) {
            return null;
        }

        @Override
        public void incrCounter(String key, long amount) {

        }

        @Override
        public CompletableFuture<Void> incrCounterAsync(String key, long amount) {
            return null;
        }

        @Override
        public long getCounter(String key) {
            return 0;
        }

        @Override
        public CompletableFuture<Long> getCounterAsync(String key) {
            return null;
        }

        @Override
        public void putState(String key, ByteBuffer value) {

        }

        @Override
        public CompletableFuture<Void> putStateAsync(String key, ByteBuffer value) {
            return null;
        }

        @Override
        public ByteBuffer getState(String key) {
            return null;
        }

        @Override
        public CompletableFuture<ByteBuffer> getStateAsync(String key) {
            return null;
        }

        @Override
        public void deleteState(String key) {

        }

        @Override
        public CompletableFuture<Void> deleteStateAsync(String key) {
            return null;
        }
    }
}
