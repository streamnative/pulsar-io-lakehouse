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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import java.util.Map;
import java.util.Properties;

public class HoodieSinkConfigs extends HoodieConfig {

    public static final ConfigProperty<String> ASYNC_COMPACT_ENABLE = ConfigProperty
        .key("connector.enableHoodieCompactionAsync")
        .defaultValue("true")
        .withDocumentation("Controls whether async compaction should be turned on for MOR table writing.");

    public static final ConfigProperty<String> HADOOP_CONF_DIR = ConfigProperty
        .key("connector.hadoopConfDir")
        .noDefaultValue()
        .withDocumentation("The Hadoop configuration directory.");

    public static final ConfigProperty<String> HADOOP_HOME = ConfigProperty
        .key("connector.hadoopHome")
        .noDefaultValue()
        .withDocumentation("The Hadoop home directory.");

    public static final ConfigProperty<String> PULSAR_SERVICE_URL = ConfigProperty
        .key("connector.pulsar.serviceUrl")
        .defaultValue("")
        .withDocumentation("The pulsar service url which used for fetching schema");

    public static final ConfigProperty<Integer> COMMIT_INTERVAL_SECS = ConfigProperty
        .key("connector.hoodieCommitIntervalSecs")
        .defaultValue(60)
        .withDocumentation("The interval at which Hudi will commit the records written to "
            + "the files, making them consumable on the read-side");

    public static final ConfigProperty<Integer> MAX_RECORDS_PER_COMMIT = ConfigProperty
        .key("connector.hoodieCommitMaxRecords")
        .defaultValue(1000)
        .withDocumentation("");

    protected HoodieSinkConfigs() {
        super();
    }

    protected HoodieSinkConfigs(Properties properties) {
        super(properties);
    }

    public int getHoodieCommitIntervalSecs() {
        return getInt(COMMIT_INTERVAL_SECS);
    }

    public int getHoodieCommitMaxRecords() {
        return getInt(MAX_RECORDS_PER_COMMIT);
    }

    public String getPulsarServiceUrl() {
        String url = getStringOrDefault(PULSAR_SERVICE_URL);
        if (url.isEmpty()) {
            throw new IllegalArgumentException("Pulsar service url is empty");
        }
        return url;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        protected final HoodieSinkConfigs configs = new HoodieSinkConfigs();

        public Builder withProperties(Map<?, ?> properties) {
            configs.getProps().putAll(properties);
            return this;
        }

        public HoodieSinkConfigs build() {
            configs.setDefaults(HoodieSinkConfigs.class.getName());
            return new HoodieSinkConfigs(configs.getProps());
        }
    }
}
