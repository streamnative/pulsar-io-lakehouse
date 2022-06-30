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
package org.apache.pulsar.ecosystem.io.lakehouse.sink;

import java.util.UUID;
import lombok.EqualsAndHashCode;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * The pulsar object represents the pulsar message with primitive schemas.
 * This object will be serialized with Avro then writing to the lakehouse table.
 *
 * @param <T> is the type of the given value.
 */
@EqualsAndHashCode
public class PulsarObject<T> {

    private static String defaultFieldname = "message";
    private final Schema valueSchema;
    T value;
    String uuid;

    public PulsarObject(T value, Schema schema) {
        this.value = value;
        valueSchema = schema;
        this.uuid = UUID.randomUUID().toString();
    }

    private PulsarObject(T value, Schema schema, String uuid) {
        this.value = value;
        valueSchema = schema;
        this.uuid = uuid;
    }

    public static void overrideFieldName(String fieldName) {
        defaultFieldname = fieldName;
    }

    public Schema getSchema() {
        return SchemaBuilder.record("PulsarObject")
            .fields()
            .name(defaultFieldname).type(valueSchema).noDefault()
            .name("uuid").type(Schema.create(Schema.Type.STRING)).noDefault()
            .endRecord();
    }

    public GenericRecord getRecord() {
        GenericRecord record = new GenericData.Record(getSchema());
        record.put(defaultFieldname, value);
        record.put("uuid", uuid);
        return record;
    }

    public static <T> PulsarObject<T> fromGenericRecord(GenericRecord record) {
        if (!record.hasField(defaultFieldname) && !record.hasField("uuid")) {
            throw new RuntimeException("Unexpected record when parsing to the PulsarObject");
        }
        return new PulsarObject(record.get(defaultFieldname),
            record.getSchema().getField(defaultFieldname).schema()
            , record.get("uuid").toString());
    }
}
