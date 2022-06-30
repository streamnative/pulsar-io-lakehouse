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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.avro.Schema;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.Test;

public class PrimitiveFactoryTest {

    @Test(expectedExceptions = RuntimeException.class)
    public void testUnsupportedType() {
        PrimitiveFactory.getPulsarPrimitiveObject(SchemaType.AVRO, null, "");
    }

    @Test
    public void testPrimitiveBytes() {
        byte[] message = "test".getBytes(StandardCharsets.UTF_8);
        PulsarObject object = PrimitiveFactory.getPulsarPrimitiveObject(SchemaType.BYTES, message, "");
        Object value = object.getRecord().get("message");
        assertTrue(value instanceof ByteBuffer);
        assertEquals(Schema.Type.BYTES, object.getSchema().getField("message").schema().getType());
        ByteBuffer byteBufferValue = (ByteBuffer) value;
        assertEquals(byteBufferValue.array(), message);
    }

    @Test
    public void testPrimitiveString() {
        String message = "test";
        PulsarObject object = PrimitiveFactory.getPulsarPrimitiveObject(SchemaType.STRING, message, "");
        Object value = object.getRecord().get("message");
        assertTrue(value instanceof String);
        assertEquals(Schema.Type.STRING, object.getSchema().getField("message").schema().getType());
        assertEquals(value.toString(), message);
    }
}
