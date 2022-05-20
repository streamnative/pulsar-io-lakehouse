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
package org.apache.pulsar.ecosystem.io.lakehouse.sink.iceberg;

import java.lang.reflect.Array;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.StructLike;

/**
 * Avro record wrapper.
 */
public class AvroRecordWrapper implements StructLike {
    private final LogicalType[] types;

    private GenericRecord record = null;


    public AvroRecordWrapper(Schema schema) {
        int size = schema.getFields().size();
        types = (LogicalType[]) Array.newInstance(LogicalType.class, size);

        for (int i = 0; i < size; ++i) {
            types[i] = schema.getFields().get(i).schema().getLogicalType();
        }

    }

    public AvroRecordWrapper wrap(GenericRecord record) {
        this.record = record;
        return this;
    }

    @Override
    public int size() {
        return types.length;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
        if (record.get(pos) == null) {
            return null;
        }

        return javaClass.cast(record.get(pos));
    }

    @Override
    public <T> void set(int pos, T value) {
        throw new UnsupportedOperationException("Could not set a field in the GenericRecordWrapper "
            + "because record is read-only");
    }

}
