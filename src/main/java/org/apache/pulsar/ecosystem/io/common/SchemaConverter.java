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

import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

/**
 * Schema converter.
 */
@Slf4j
public class SchemaConverter {
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
}
