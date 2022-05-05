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
package org.apache.pulsar.ecosystem.io.sink.iceberg;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.WriteResult;

/**
 * Pulsar file committer.
 */
@Slf4j
public class PulsarFileCommitter {
    private final TableLoader tableLoader;
    private final boolean replacePartitions;

    private transient Table table;

    PulsarFileCommitter(TableLoader tableLoader, boolean replacePartitions) {
        this.tableLoader = tableLoader;
        this.replacePartitions = replacePartitions;
    }

    public void initialize() {
        String jobId = "test";
        int taskId = 1;
        long attemptId = 1;
        this.tableLoader.open();
        this.table = tableLoader.loadTable();
    }

    public void commit(WriteResult result) {
        RowDelta rowDelta = table.newRowDelta()
            .validateDataFilesExist(ImmutableList.copyOf(result.referencedDataFiles()))
            .validateDeletedFiles();

        int numDataFiles = result.dataFiles().length;
        Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);

        int numDeleteFiles = result.deleteFiles().length;
        Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);

        log.info("Committing {} with {} data files and {} delete files to table {}",
            "rowDelta", numDataFiles, numDeleteFiles, table);
        long start = System.currentTimeMillis();
        rowDelta.commit();
        long duration = System.currentTimeMillis() - start;
        log.info("Committed in {} ms", duration);
    }
}
