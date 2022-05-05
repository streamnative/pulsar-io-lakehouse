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
import static org.testng.Assert.fail;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.ecosystem.io.SinkConnector;
import org.apache.pulsar.ecosystem.io.sink.SinkWriter;
import org.apache.pulsar.functions.api.Record;
import org.mockito.Mockito;
import org.testng.annotations.Test;



/**
 * Delta writer thread test.
 *
 */
@Slf4j
public class DeltaWriterThreadTest {

    @Test
    public void testAvroGenericDataConverter() {
        SinkConnector sinkConnector = Mockito.mock(SinkConnector.class);
        SinkWriter sinkWriter = new SinkWriter(sinkConnector);

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

        Record<org.apache.pulsar.client.api.schema.GenericRecord> record =
            DeltaLakeSinkConnectorUtils.generateRecord(schemaMap, recordMap,
            SchemaType.AVRO, "MyRecord");

        try {
            GenericRecord genericRecord = sinkWriter.convertToAvroGenericData(record);
            assertEquals(genericRecord.get("name"), "hang");
            assertEquals(genericRecord.get("age"), 18);
            assertEquals(genericRecord.get("phone"), "110");
            assertEquals(genericRecord.get("address"), "GuangZhou, China");
            assertEquals(genericRecord.get("score"), 59.9);
            assertEquals(genericRecord.getSchema().toString(),
                record.getSchema().getSchemaInfo().getSchemaDefinition());
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    public void testJsonGenericDataConverter() {
        // TODO
    }

    @Test
    public void testPrimitiveSchemaTypeConverter() {
        // TODO
    }
}
