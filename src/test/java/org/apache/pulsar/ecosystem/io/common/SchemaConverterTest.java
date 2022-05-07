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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.ecosystem.io.sink.SinkConnectorUtils;
import org.apache.pulsar.functions.api.Record;
import org.testng.annotations.Test;



/**
 * schema converter tests.
 *
 */
@Slf4j
public class SchemaConverterTest {

    @Test
    public void testConvertAvroSchemaToDeltaSchema() {
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
        Record<GenericObject> record = SinkConnectorUtils.generateRecord(schemaMap, recordMap,
            SchemaType.AVRO, "MyRecord");

        Schema schema = new Schema.Parser().parse(record.getSchema().getSchemaInfo().getSchemaDefinition());

        StructType structType = SchemaConverter.convertAvroSchemaToDeltaSchema(schema);

        List<String> fields = new ArrayList<>(schemaMap.keySet());
        for (int i = 0; i < structType.getFields().length; ++i) {
            StructField field = structType.getFields()[i];
            assertEquals(field.getName(), fields.get(i));
            assertNotNull(schemaMap.get(field.getName()));
            assertFalse(field.isNullable());
            // delta integer type is `integer`, avro schema integer type is `INT32`
            if (field.getDataType().getTypeName().equals("integer")) {
                continue;
            }
            assertEquals(field.getDataType().getTypeName(),
                schemaMap.get(field.getName()).name().toLowerCase(Locale.ROOT));
        }

        // TODO test complex field type
    }
}
