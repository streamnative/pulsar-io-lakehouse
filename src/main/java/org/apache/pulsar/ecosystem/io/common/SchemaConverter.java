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

import io.delta.standalone.types.ArrayType;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.FloatType;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.MapType;
import io.delta.standalone.types.NullType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

/**
 * Schema converter.
 */
@Slf4j
public class SchemaConverter {
    public static StructField convertOneAvroFieldToDeltaField(
        String name, Schema avroSchema) throws UnsupportedOperationException {
        StructField newField = null;
        switch(avroSchema.getType()) {
            case RECORD:
                List<StructField> fields = new ArrayList<>();
                avroSchema.getFields().forEach(field -> {
                    fields.add(convertOneAvroFieldToDeltaField(field.name(), field.schema()));
                });
                newField = new StructField(avroSchema.getName(),
                    new StructType(fields.toArray(new StructField[0])), true);
                break;
            case MAP:
                Schema valueSchema = avroSchema.getValueType();
                StructField deltaValueField = convertOneAvroFieldToDeltaField(valueSchema.getName(), valueSchema);
                log.info("value schema {} deltaSchema {}", valueSchema, deltaValueField);
                StructField keyField = new StructField("key", new StringType(), false);
                StructField valueField = new StructField("value", deltaValueField.getDataType(), false);
                List<StructField> structFields = new ArrayList<>();
                structFields.add(keyField);
                structFields.add(valueField);
                ArrayType arrayType = new ArrayType(new StructType(structFields.toArray(new StructField[0])), false);
                MapType mapType = new MapType(new StringType(), arrayType, false);
                newField = new StructField(name, mapType, true);
                break;
            case ARRAY:
                Schema itemSchema = avroSchema.getElementType();
                StructField deltaItemField = convertOneAvroFieldToDeltaField(itemSchema.getName(), itemSchema);
                ArrayType arrayType1 = new ArrayType(deltaItemField.getDataType(), true);
                newField = new StructField(name, arrayType1, true);
                break;
            case UNION:
                List<Schema> types = avroSchema.getTypes();
                boolean isNullable = types.stream().anyMatch(Schema::isNullable);
                for (Schema schema : types) {
                    if (!schema.isNullable()) {
                        newField = new StructField(name, convertSchemaTypeToDeltaType(schema), isNullable);
                        break;
                    }
                }
                break;
            case FIXED:
                throw new UnsupportedOperationException("not support fixed in delta schema");
            case STRING:
                newField = new StructField(name, new StringType(), avroSchema.isNullable());
                break;
            case BYTES:
                newField = new StructField(name, new StringType(), avroSchema.isNullable());
                break;
            case INT:
                newField = new StructField(name, new IntegerType(), avroSchema.isNullable());
                break;
            case LONG:
                newField = new StructField(name, new LongType(), avroSchema.isNullable());
                break;
            case FLOAT:
                newField = new StructField(name, new FloatType(), avroSchema.isNullable());
                break;
            case DOUBLE:
                newField = new StructField(name, new DoubleType(), avroSchema.isNullable());
                break;
            case BOOLEAN:
                newField = new StructField(name, new BooleanType(), avroSchema.isNullable());
                break;
            case NULL:
                newField = new StructField(name, new NullType(), avroSchema.isNullable());
                break;
            default:
                log.error("not support schema type {} in convert ", avroSchema.getType());
                break;
        }
        return newField;
    }

    public static StructType convertAvroSchemaToDeltaSchema(Schema pulsarAvroSchema) {
        log.info("pulsar schema: {}, {}", pulsarAvroSchema, pulsarAvroSchema.isNullable());
        StructField field = convertOneAvroFieldToDeltaField(pulsarAvroSchema.getName(), pulsarAvroSchema);
        return (StructType) field.getDataType();
    }

    public static Schema convertPulsarAvroSchemaToNonNullSchema(Schema schema) {
        List<Schema.Field> newFields = new ArrayList<>();
        schema.getFields().forEach(f->{
            Schema fieldSchema = convertOneField(f.name(), f.schema());
            Schema.Field field = new Schema.Field(f.name(),
                fieldSchema, fieldSchema.getDoc(), null);
            newFields.add(field);
        });

        Schema newSchema = Schema.createRecord(
            schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError());

        newSchema.setFields(newFields);
        return newSchema;
    }

    public static Schema convertOneField(String name, Schema f) {
        Schema newField = null;
        switch(f.getType()) {
            case UNION:
                List<Schema> validTypes = new ArrayList<>();
                f.getTypes().forEach(t->{
                    if (t.getType() != Schema.Type.NULL) {
                        validTypes.add(t);
                    }
                });
                if (validTypes.size() == 1) {
                    newField = convertOneField(name, validTypes.get(0));
                } else {
                    log.error("not support this kind of union types {}", f.getTypes());
                    throw new UnsupportedOperationException("not support this kind of field");
                }
                break;
            case RECORD:
                List<Schema.Field> newFields = new ArrayList<>();
                f.getFields().forEach(field -> {
                    Schema schema = convertOneField(field.name(), field.schema());
                    newFields.add(new Schema.Field(field.name(), schema, schema.getDoc(), null));
                });

                newField = Schema.createRecord(name, f.getDoc(), f.getNamespace(), f.isError());
                newField.setFields(newFields);
                break;
            case ARRAY:
                if (f.getElementType().getType() == Schema.Type.RECORD) {
                    // Map will be an array whose items is a RECORD with 'key' and 'value', but this kind of format
                    // is different from the pulsar json encoding
                    Schema.Field keyField = f.getElementType().getField("key");
                    Schema.Field valueField = f.getElementType().getField("value");
                    if (keyField != null && valueField != null) {
                        throw new UnsupportedOperationException("not support this kind of map datatype");
                    }
                }
                Schema newItem = convertOneField(f.getElementType().getName(), f.getElementType());
                newField = Schema.createArray(newItem);
                break;
            case MAP:
                newItem = convertOneField(name, f.getValueType());
                newField = Schema.createMap(newItem);
                break;
            default:
                newField = f;
        }
        return newField;
    }

    public static DataType convertSchemaTypeToDeltaType(Schema schema) {
        DataType type;
        switch (schema.getType()) {
            case STRING:
            case BYTES:
                type = new StringType();
                break;
            case INT:
                type = new IntegerType();
                break;
            case LONG:
                type = new LongType();
                break;
            case DOUBLE:
                type = new DoubleType();
                break;
            case BOOLEAN:
                type = new BooleanType();
                break;
            case NULL:
                type = new NullType();
                break;
            default:
                log.error("not support schema type {} in convert", schema.getType());
                throw new UnsupportedOperationException("Not support schema type in union");
        }
        return type;
    }
}
