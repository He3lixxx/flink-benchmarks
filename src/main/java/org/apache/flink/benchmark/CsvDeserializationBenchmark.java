/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.benchmark;

import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.api.common.functions.MapFunction;

import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Scope;

import java.nio.charset.StandardCharsets;


@State(Scope.Thread)
public class CsvDeserializationBenchmark extends DeserializationBenchmarkBase {
    @Override
    public byte[] serializeNativeTuple(NativeTuple tup) {
        String csv = "" + tup.id + "," + tup.timestamp + "," + tup.load + "," + tup.load_avg_1 + "," + tup.load_avg_5 + "," + tup.load_avg_15 + "," + tup.containerIdStr();
        return csv.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public DataStream<NativeTuple> addDeserializationOperations(DataStream<byte[]> stream) {
        return stream.map(new MapFunction<byte[], NativeTuple>() {
            @Override
            public NativeTuple map(byte[] bytes) throws Exception {
                // Flink provides sample code in various situations where users should simply use String.split, e.g. here:
                // https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sourcessinks/#runtime-1
                // Flink does have CsvInputFormat classes, but they only add overhead, see
                // https://github.com/apache/flink/blob/99c2a415e9eeefafacf70762b6f54070f7911ceb/flink-java/src/main/java/org/apache/flink/api/java/io/RowCsvInputFormat.java#L168
                // Thus, this should give a good upper bound on performance of csv parsing in flink
                final String[] columns = new String(bytes, StandardCharsets.UTF_8).split(",");
                NativeTuple tup = new NativeTuple();
                tup.id = Long.parseLong(columns[0]);
                tup.timestamp = Long.parseLong(columns[1]);
                tup.load = Float.parseFloat(columns[2]);
                tup.load_avg_1 = Float.parseFloat(columns[3]);
                tup.load_avg_5 = Float.parseFloat(columns[4]);
                tup.load_avg_15 = Float.parseFloat(columns[5]);
                tup.setContainerIdFromStr(columns[6]);

                return tup;
            }
        });
    }
}
