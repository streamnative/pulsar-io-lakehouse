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

import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedFanoutWriter;

/**
 * Message partitioned fanout writer.
 */
public class MessagePartitionedFanoutWriter extends PartitionedFanoutWriter<GenericRecord> {
    private final PartitionKey partitionKey;
    private final AvroRecordWrapper wrapper;
    MessagePartitionedFanoutWriter(PartitionSpec spec, FileFormat format,
                                   FileAppenderFactory<GenericRecord> appenderFactory, OutputFileFactory fileFactory,
                                   FileIO io, long targetFileSize, Schema schema, org.apache.avro.Schema pulsarSchema) {
        super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
        this.partitionKey = new PartitionKey(spec, schema);
        this.wrapper = new AvroRecordWrapper(pulsarSchema);

    }

    @Override
    protected PartitionKey partition(GenericRecord record) {
        partitionKey.partition(wrapper.wrap(record));
        return partitionKey;
    }
}
