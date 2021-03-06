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
package org.apache.pulsar.ecosystem.io.lakehouse.sink.hudi;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestUtils {

    public static List<String> walkThroughCloudDir(FileSystem fileSystem, String path) throws IOException {
        List<String> files = new LinkedList<>();
        Queue<Path> dirs = new LinkedList<>();
        dirs.add(new Path(path));
        Path dir;
        while ((dir = dirs.poll()) != null) {
            for (FileStatus fileStatus : fileSystem.listStatus(dir)) {
                if (fileStatus.isDirectory()) {
                    dirs.add(fileStatus.getPath());
                    continue;
                }
                files.add(fileStatus.getPath().toString());
            }
        }
        return files;
    }

    public static String randomString(int len) {
        return new RandomString(len).nextString();
    }

    private static class RandomString {
        private final Random random;
        private final char[] buf;
        private final char[] symbols;
        private static final String lower = "abcdefghijklmnopqrstuvwxyz";

        public RandomString(int len) {
            this.random = new SecureRandom();
            this.buf = new char[len];
            this.symbols = lower.toCharArray();
        }

        public String nextString() {
            for (int i = 0; i < buf.length; i++) {
                buf[i] = symbols[random.nextInt(symbols.length)];
            }
            return new String(buf);
        }
    }
}
