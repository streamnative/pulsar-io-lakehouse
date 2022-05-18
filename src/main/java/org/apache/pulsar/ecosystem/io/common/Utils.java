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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.util.concurrent.FastThreadLocal;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.pulsar.ecosystem.io.SinkConnectorConfig;

@Slf4j
public class Utils {
    private static final String HADOOP_CONF_PREFIX = "hadoop.";

    public static Configuration getDefaultHadoopConf(SinkConnectorConfig configs) {
        return getDefaultHadoopConf(configs.getProperties());
    }

    public static Configuration getDefaultHadoopConf(Properties properties) {
        Configuration hadoopConf = new Configuration();
        properties.entrySet().stream()
            .filter(c -> c.getKey().toString().startsWith(HADOOP_CONF_PREFIX))
            .forEach(prop -> {
                hadoopConf.set(
                    prop.getKey().toString().replaceFirst(HADOOP_CONF_PREFIX, ""),
                    prop.getValue().toString());
            });
        return hadoopConf;
    }

    public static final FastThreadLocal<ObjectMapper> JSON_MAPPER = new FastThreadLocal<ObjectMapper>() {
        protected ObjectMapper initialValue() throws Exception {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            return mapper;
        }
    };
}
