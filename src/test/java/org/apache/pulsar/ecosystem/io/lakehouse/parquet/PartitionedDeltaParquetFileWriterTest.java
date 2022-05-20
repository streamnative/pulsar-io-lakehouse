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
package org.apache.pulsar.ecosystem.io.lakehouse.parquet;

import static org.testng.Assert.assertEquals;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.ecosystem.io.lakehouse.sink.SinkConnectorUtils;
import org.apache.pulsar.functions.api.Record;
import org.testng.annotations.Test;

/**
 * partitioned delta parquet file writer test.
 *
 */

public class PartitionedDeltaParquetFileWriterTest {

    @Test
    public void testGeneratePartitionValues() {
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

        List<String> partitionColumns = Arrays.asList("name", "age");

        for (int i = 0; i < 100; ++i) {
            recordMap.put("age", 18 + i);
            recordMap.put("score", 59.9 + i);

            Record<GenericObject> record = SinkConnectorUtils.generateRecord(schemaMap, recordMap,
                SchemaType.AVRO, "MyRecord");

            Map<String, String> partitionValues =
                PartitionedDeltaParquetFileWriter.getPartitionValues(
                    (org.apache.avro.generic.GenericRecord) record.getValue().getNativeObject(), partitionColumns);

            assertEquals(partitionValues.get("name"), "hang");
            assertEquals(partitionValues.get("age"), String.valueOf(18 + i));
        }
    }

    @Test
    public void testGeneratePartitionValuePath() {
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

        List<String> partitionColumns = Arrays.asList("name", "age");
        for (int i = 0; i < 100; ++i) {
            recordMap.put("age", 18 + i);
            recordMap.put("score", 59.9 + i);

            Record<GenericObject> record = SinkConnectorUtils.generateRecord(schemaMap, recordMap,
                SchemaType.AVRO, "MyRecord");

            String path = PartitionedDeltaParquetFileWriter.getPartitionValuePath(
                    (org.apache.avro.generic.GenericRecord) record.getValue().getNativeObject(), partitionColumns);

            assertEquals(path, "name=hang/age=" + (18 + i));
        }

    }
}
