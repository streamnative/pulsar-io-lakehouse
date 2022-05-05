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

package org.apache.pulsar.ecosystem.io.sink.delta;

import static org.testng.Assert.assertEquals;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.ecosystem.io.sink.PulsarSinkRecord;
import org.apache.pulsar.functions.api.Record;
import org.testng.annotations.Test;


/**
 * delta sink record test.
 */
@Slf4j
public class DeltaSinkRecordTest {

    @Test
    public void testRecordSchema() {
        Map<String, SchemaType> schemaMap = new HashMap<>();
        schemaMap.put("name", SchemaType.STRING);
        schemaMap.put("age", SchemaType.INT32);
        schemaMap.put("phone", SchemaType.STRING);
        schemaMap.put("address", SchemaType.STRING);
        schemaMap.put("score", SchemaType.DOUBLE);

        Map<String, Object> recordMap = new HashMap<>();
        recordMap.put("name", "hang");
        recordMap.put("age", 18);
        recordMap.put("phone", "110");
        recordMap.put("address", "GuangZhou, China");
        recordMap.put("score", 59.9);

        Record<GenericRecord> record = DeltaLakeSinkConnectorUtils.generateRecord(schemaMap, recordMap,
            SchemaType.AVRO, "MyRecord");

        PulsarSinkRecord sinkRecord = new PulsarSinkRecord(record);

        assertEquals(sinkRecord.getRecord().getPartitionIndex().get(), Integer.valueOf(1));
        assertEquals(sinkRecord.getRecord().getProperties().get("key-a"), "value-a");

        Record<GenericRecord> r = sinkRecord.getRecord();
        // assert schema
        SchemaInfo schemaInfo = r.getSchema().getSchemaInfo();
        assertEquals(schemaInfo.getName(), "MyRecord");
        assertEquals(schemaInfo.getType(), SchemaType.AVRO);

        Schema schema = new Schema.Parser().parse(schemaInfo.getSchemaDefinition());
        for (Schema.Field field : schema.getFields()) {
            if (field.schema().getType().name().toUpperCase(Locale.ROOT).equals("INT")) {
                continue;
            }
            assertEquals(schemaMap.get(field.name()).toString().toUpperCase(Locale.ROOT),
                field.schema().getType().getName().toUpperCase(Locale.ROOT));
        }

        // assert value
        for (Field field: r.getValue().getFields()) {
            assertEquals(r.getValue().getField(field), recordMap.get(field.getName()));
        }
    }
}
