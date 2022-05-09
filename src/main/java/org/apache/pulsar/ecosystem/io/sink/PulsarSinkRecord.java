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

import lombok.Data;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;

/**
 * Pulsar sink record.
 */
@Data
public class PulsarSinkRecord {
    private final Record<GenericRecord> record;

    public PulsarSinkRecord(Record<GenericRecord> record) {
        this.record = record;
    }

    public SchemaType getSchemaType() {
        return record.getValue().getSchemaType();
    }

    public String getSchema() {
        return record.getSchema().getSchemaInfo().getSchemaDefinition();
    }

    public Object getNativeObject() {
        return record.getValue().getNativeObject();
    }

    public void ack() {
        record.ack();
    }

    public void fail() {
        record.fail();
    }
}
