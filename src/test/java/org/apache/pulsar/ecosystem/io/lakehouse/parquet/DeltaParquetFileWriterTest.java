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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.ecosystem.io.lakehouse.sink.SinkConnectorUtils;
import org.apache.pulsar.functions.api.Record;
import org.junit.Assert;
import org.testng.annotations.Test;


/**
 * Delta parquet file writer test.
 *
 */
@Slf4j
public class DeltaParquetFileWriterTest {

    @Test
    public void testGenerateNextFilePath() {
        String compression = "snappy";
        String partitionColumnPath = "";
        String tablePath = "/tmp/delta/data/test_v1";

        // table path end without "/" and partitionColumnPath is empty.
        String suffix = "-c000." + compression.toLowerCase(Locale.ROOT) + ".parquet";
        String prefix = tablePath + "/" + partitionColumnPath;
        String path = DeltaParquetFileWriter.generateNextFilePath(partitionColumnPath, tablePath, compression);
        assertTrue(path.startsWith(prefix));
        assertTrue(path.endsWith(suffix));

        // table path end without "/", and partitionColumnPath is not empty.
        tablePath = "/tmp/delta/data/test_v1";
        partitionColumnPath = "a=1/b=2";
        prefix = tablePath + "/" + partitionColumnPath;
        path = DeltaParquetFileWriter.generateNextFilePath(partitionColumnPath, tablePath, compression);
        assertTrue(path.startsWith(prefix));
        assertTrue(path.endsWith(suffix));

        // table path end with "/" and partitionColumnPath is empty.
        tablePath = "/tmp/delta/data/test_v1/";
        partitionColumnPath = "";
        prefix = tablePath + partitionColumnPath;
        path = DeltaParquetFileWriter.generateNextFilePath(partitionColumnPath, tablePath, compression);
        assertTrue(path.startsWith(prefix));
        assertTrue(path.endsWith(suffix));

        // table path end with "/" and partitionColumnPath is not empty;
        tablePath = "/tmp/delta/data/test_v1/";
        partitionColumnPath = "a=1/b=2";
        prefix = tablePath + partitionColumnPath;
        path = DeltaParquetFileWriter.generateNextFilePath(partitionColumnPath, tablePath, compression);
        assertTrue(path.startsWith(prefix));
        assertTrue(path.endsWith(suffix));
    }

    @Test
    public void testOpenNewFile() {
        String path = "/tmp/test_delta-" + UUID.randomUUID();
        Configuration configuration = new Configuration();
        String compression = "snappy";

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

        try {
            ParquetWriter<org.apache.avro.generic.GenericRecord> writer = DeltaParquetFileWriter
                .openNewFile(path, schema, configuration, compression);
            assertEquals(writer.getDataSize(), 0);
            writer.write((org.apache.avro.generic.GenericRecord) record.getValue().getNativeObject());
            writer.close();
            Map<String, String> metadata = writer.getFooter().getFileMetaData().getKeyValueMetaData();
            assertEquals(metadata.get("parquet.avro.schema"), record.getSchema().getSchemaInfo().getSchemaDefinition());
            assertEquals(metadata.get("writer.model.name"), "avro");

        } catch (IOException e) {
            fail();
        }

        new File(path).deleteOnExit();
    }

    @Test
    public void testWriter() {
        String path = "/tmp/test_delta-" + UUID.randomUUID();
        Configuration configuration = new Configuration();
        String compression = "snappy";

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

        List<Record<GenericObject>> recordList = new ArrayList<>();
        try {
            Record<GenericObject> record = SinkConnectorUtils.generateRecord(schemaMap, recordMap,
                SchemaType.AVRO, "MyRecord");
            Schema schema = new Schema.Parser().parse(record.getSchema().getSchemaInfo().getSchemaDefinition());

            ParquetWriter<org.apache.avro.generic.GenericRecord> writer = DeltaParquetFileWriter
                .openNewFile(path, schema, configuration, compression);

            for (int i = 0; i < 100; ++i) {
                recordMap.put("age", i);
                recordMap.put("score", 59.9 + i);
                record = SinkConnectorUtils.generateRecord(schemaMap, recordMap,
                    SchemaType.AVRO, "MyRecord");
                recordList.add(record);
                writer.write((org.apache.avro.generic.GenericRecord) record.getValue().getNativeObject());
            }
            writer.close();

            // open the parquet file to check value.
            GroupReadSupport readSupport = new GroupReadSupport();
            ParquetReader<Group> reader = ParquetReader.builder(readSupport, new Path(path)).build();
            Group line;
            int cnt = 0;
            while ((line = reader.read()) != null) {
                Record<GenericObject> record1 = recordList.get(cnt);
                assertEquals(line.getDouble(0, 0), ((GenericRecord) record1.getValue()).getField("score"));
                assertEquals(line.getString(1, 0), ((GenericRecord) record1.getValue()).getField("address"));
                assertEquals(line.getString(2, 0), ((GenericRecord) record1.getValue()).getField("phone"));
                assertEquals(line.getString(3, 0), ((GenericRecord) record1.getValue()).getField("name"));
                assertEquals(line.getInteger(4, 0), ((GenericRecord) record1.getValue()).getField("age"));
                cnt++;
            }
        } catch (IOException e) {
            fail();
        }

        new File(path).deleteOnExit();
    }

    @Test
    public void testGetFileSize() {
        String path = "/tmp/test_delta-" + UUID.randomUUID();
        Configuration configuration = new Configuration();
        String compression = "snappy";

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

        try {
            ParquetWriter<org.apache.avro.generic.GenericRecord> writer = DeltaParquetFileWriter
                    .openNewFile(path, schema, configuration, compression);
            assertEquals(writer.getDataSize(), 0);
            writer.write((org.apache.avro.generic.GenericRecord) record.getValue().getNativeObject());
            writer.close();

            DeltaParquetFileWriter deltaParquetFileWriter =
                    new DeltaParquetFileWriter(configuration, "", compression, null);
            Assert.assertEquals(new File(path).length(), deltaParquetFileWriter.getFileSize(path));
        } catch (IOException e) {
            fail();
        } finally {
            new File(path).deleteOnExit();
        }
    }

    private static void parquetReader(String path) throws IOException {
        GroupReadSupport readSupport = new GroupReadSupport();
        ParquetReader<Group> reader = ParquetReader.builder(readSupport, new Path(path)).build();
        Group line = null;
        while ((line = reader.read()) != null) {
            log.info("line: {}", line);
        }
    }
}
