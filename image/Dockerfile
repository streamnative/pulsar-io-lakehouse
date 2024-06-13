#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

ARG PULSAR_VERSION
FROM alpine AS snappy-java

ARG SNAPPY_VERSION=1.1.8.4
RUN apk add git alpine-sdk util-linux cmake autoconf automake libtool openjdk17 maven curl bash tar
ENV JAVA_HOME=/usr

# todo: before the 1.1.9.1, the snappy tag is not start with 'v', after that version, the tag is start with 'v'
# we need to update the download link once the version is updated
RUN curl -Ls https://github.com/xerial/snappy-java/archive/refs/tags/$SNAPPY_VERSION.tar.gz | tar zxf - && cd snappy-java-$SNAPPY_VERSION && make clean-native native

FROM streamnative/pulsar-functions-java-runner:${PULSAR_VERSION}
USER 0
COPY --from=snappy-java /tmp/libsnappyjava.so /usr/lib/libsnappyjava.so
# need to configure -Dorg.xerial.snappy.use.systemlib=true in the jvm options to use the system lib
USER $UID
COPY --chown=$UID:$GID target/pulsar-io-lakehouse-*-cloud.nar /pulsar/connectors/
