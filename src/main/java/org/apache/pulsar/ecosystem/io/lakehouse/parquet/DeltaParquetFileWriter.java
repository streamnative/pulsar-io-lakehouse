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

import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;


/**
 * DeltaParquetFile writer.
 */
@Slf4j
@Data
public class DeltaParquetFileWriter implements DeltaParquetWriter {
    private String tablePath;
    private Schema schema;
    private ParquetWriter<GenericRecord> writer;
    private String partitionColumnPath;
    private Map<String, String> partitionValues;
    private Configuration configuration;
    private long lastRollFileTimestamp;
    private String currentFileFullPath;
    private final AtomicBoolean isClosed = new AtomicBoolean(true);
    private final String compression;

    public DeltaParquetFileWriter(Configuration configuration, String tablePath, String compression, Schema schema) {
        this.configuration = configuration;
        this.tablePath = tablePath;
        this.schema = schema;
        this.lastRollFileTimestamp = System.currentTimeMillis();
        this.partitionColumnPath = null;
        this.currentFileFullPath = "";
        this.compression = compression;
        this.partitionValues = Collections.emptyMap();
    }

    public DeltaParquetFileWriter(Configuration configuration, String tablePath, String compression, Schema schema,
                                  String partitionColumnPath, Map<String, String> partitionValues) {
        this(configuration, tablePath, compression, schema);
        this.partitionColumnPath = partitionColumnPath;
        this.partitionValues = partitionValues;
    }

    @Override
    public void writeToParquetFile(GenericRecord record) throws IOException {
        if (isClosed.get() || StringUtils.isBlank(currentFileFullPath)) {
            currentFileFullPath = generateNextFilePath(partitionColumnPath, tablePath, compression);
            writer = openNewFile(currentFileFullPath, schema, configuration, compression);
            isClosed.set(false);
        }
        writer.write(record);
    }

    @VisibleForTesting
    public long getFileSize(String fileFullPath) throws IOException {
        Path path = new Path(fileFullPath);
        HadoopInputFile hadoopInputFile = HadoopInputFile.fromPath(path, configuration);
        return hadoopInputFile.getLength();
    }

    @Override
    public List<FileStat> closeAndFlush() throws IOException {
        if (isClosed.get()) {
            return null;
        }
        String closedFileFullPath = currentFileFullPath;
        String filePath = currentFileFullPath.substring(
                currentFileFullPath.indexOf(tablePath) + tablePath.length() + 1);
        close();
        lastRollFileTimestamp = System.currentTimeMillis();
        FileStat fileStat = new FileStat(filePath, getFileSize(closedFileFullPath), partitionValues);
        return Collections.singletonList(fileStat);
    }

    @Override
    public void updateSchema(Schema schema) {
        this.schema = schema;
    }

    private void close() throws IOException {
        if (isClosed.get()) {
            return;
        }

        if (writer != null) {
            try {
                log.info("start to close internal parquet writer");
                writer.close();
            } catch (IOException e) {
                log.error("close internal parquet writer failed. filePath: {}", currentFileFullPath, e);
                throw e;
            } finally {
                isClosed.set(true);
                writer = null;
                currentFileFullPath = "";
            }
        }
    }

    public static String generateNextFilePath(String partitionColumnPath, String tablePath, String compression) {
        StringBuilder sb = new StringBuilder();
        String suffix = new StringBuilder()
            .append("part-0000-")
            .append(UUID.randomUUID())
            .append("-c000.")
            .append(compression.toLowerCase(Locale.ROOT))
            .append(".parquet")
            .toString();

        sb.append(tablePath);
        if (!tablePath.endsWith("/")) {
            sb.append("/");
        }

        if (!StringUtils.isBlank(partitionColumnPath)) {
            sb.append(partitionColumnPath)
                .append("/");
        }

        return sb.append(suffix).toString();
    }

    protected static ParquetWriter<GenericRecord> openNewFile(String currentFileFullPath,
                                                       Schema schema,
                                                       Configuration configuration,
                                                       String compression) throws IOException {
        ParquetWriter<GenericRecord> writer = AvroParquetWriter
            .<GenericRecord>builder(new Path(currentFileFullPath))
            .withRowGroupSize(DEFAULT_BLOCK_SIZE)
            .withPageSize(DEFAULT_PAGE_SIZE)
            .withSchema(schema)
            .withConf(configuration)
            .withCompressionCodec(CompressionCodecName.valueOf(compression.toUpperCase(Locale.ROOT)))
            .withValidation(false)
            .withDictionaryEncoding(false)
            .build();


        log.info("open: {} parquet writer succeed. {}", currentFileFullPath, writer);
        return writer;
    }

}
