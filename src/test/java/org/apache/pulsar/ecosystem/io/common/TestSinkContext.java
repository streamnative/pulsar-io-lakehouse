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

package org.apache.pulsar.ecosystem.io.common;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.io.core.SinkContext;
import org.slf4j.Logger;

/**
 * test sink context.
 */
public class TestSinkContext implements SinkContext {
    static Map<String, String> secretsMap = new HashMap<>();
    static {
        secretsMap.put("password", "my-sink-password");
        secretsMap.put("moreSensitiveStuff", "more-sensitive-stuff");
        secretsMap.put("derivedDerivedSensitive", "derived-derived-sensitive");
    }

    @Override
    public SubscriptionType getSubscriptionType() {
        return SubscriptionType.Failover;
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
    public Collection<String> getInputTopics() {
        return null;
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
    public String getSinkName() {
        return null;
    }

    @Override
    public Logger getLogger() {
        return null;
    }

    @Override
    public String getSecret(String secretName) {
        return secretsMap.get(secretName);
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

    @Override
    public PulsarClient getPulsarClient() {
        return null;
    }
}
