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
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;

import java.nio.charset.StandardCharsets;
import java.util.List;


@State(Scope.Thread)
public class NullDeserializationBenchmark extends DeserializationBenchmarkBase {
    private static final byte[] bytestring = "a".repeat(64).getBytes(StandardCharsets.UTF_8);
    private static final NativeTuple tup = new NativeTuple();

    @Override
    public byte[] serializeNativeTuple(NativeTuple tup) {
        return bytestring;
    }

    @Override
    public DataStream<NativeTuple> addDeserializationOperations(DataStream<List<Byte>> stream) {
        return stream.map(new MapFunction<List<Byte>, NativeTuple>() {
            @Override
            public NativeTuple map(List<Byte> bytes) {
                return tup;
            }
        });
    }

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
                        .include(CsvDeserializationBenchmark.class.getSimpleName())
                        .forks(1)
                        .build();

        new Runner(options).run();
    }
}
