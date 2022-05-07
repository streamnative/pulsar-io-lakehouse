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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;

/**
 * Delta lake sink connector utils.
 *
 */
public class SinkConnectorUtils {
    public static Record<GenericRecord> generateRecord(Map<String, SchemaType> schemaMap,
                                                      Map<String, Object> objectMap,
                                                      SchemaType schemaType,
                                                      String recordName) {
        RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record(recordName);
        schemaMap.forEach((name, type) -> {
            recordSchemaBuilder.field(name).type(type);
        });
        GenericSchema<GenericRecord> schema = Schema.generic(recordSchemaBuilder.build(schemaType));

        GenericRecordBuilder recordBuilder = schema.newRecordBuilder();
        schemaMap.keySet().forEach(key -> {
            recordBuilder.set(key, objectMap.get(key));
        });
        GenericRecord genericRecord = recordBuilder.build();

        String topic = "test_delta_sink_v1";

        return new Record<GenericRecord>() {
            @Override
            public GenericRecord getValue() {
                return genericRecord;
            }

            @Override
            public Optional<String> getDestinationTopic() {
                return Optional.of(topic);
            }

            @Override
            public Schema<GenericRecord> getSchema() {
                return schema;
            }

            @Override
            public Optional<String> getPartitionId() {
                return Optional.of(topic + "-id-1");
            }

            @Override
            public Optional<Long> getRecordSequence() {
                return Optional.of(1L);
            }

            @Override
            public Optional<Integer> getPartitionIndex() {
                return Optional.of(1);
            }

            @Override
            public Map<String, String> getProperties() {
                Map<String, String> property = new HashMap<>();
                property.put("key-a", "value-a");
                property.put("key-b", "value-b");
                property.put("key-c", "value-c");
                return property;
            }
        };
    }
}
