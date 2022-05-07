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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.utilities.sources.helpers.AvroConvertor;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.ecosystem.io.sink.PulsarSinkRecord;
@Slf4j
public class Utils {

    private static final String HOODIE_CONF_PREFIX = "hoodie.";
    private static final String PULSAR_CONF_PREFIX = "pulsar.";
    private static final String HADOOP_CONF_PREFIX = "hadoop.";

    public static HoodieRecord<?> convert(KeyGenerator keyGenerator, PulsarSinkRecord sinkGenericRecord,
                                          String schema) {
        log.info("Convert schema {}", schema);
        AvroConvertor convertor = new AvroConvertor(schema);
        GenericData.Record record = (GenericData.Record) sinkGenericRecord.getNativeObject();
        Option<GenericRecord> avroRecord = Option.of(convertor.fromJson(record.toString()));
        return new HoodieRecord<>(keyGenerator.getKey(avroRecord.get()), new HoodieAvroPayload(avroRecord));
    }

    public static Configuration getDefaultHadoopConf(org.apache.pulsar.ecosystem.io.sink.hudi.HoodieSinkConfigs configs) {
        Configuration hadoopConf = new Configuration();
        configs.getProps().keySet().stream().filter(prop -> {
            return prop.toString().startsWith(HADOOP_CONF_PREFIX);
        }).forEach(prop -> {
            hadoopConf.set(prop.toString().replaceFirst(HADOOP_CONF_PREFIX, ""),
                configs.getProps().get(prop.toString()).toString());
        });
        return hadoopConf;
    }

    public static PulsarAdmin getPulsarAdmin(org.apache.pulsar.ecosystem.io.sink.hudi.HoodieSinkConfigs configs) throws PulsarClientException {
        return PulsarAdmin.builder()
            .serviceHttpUrl(configs.getPulsarServiceUrl())
            .build();
    }
}
