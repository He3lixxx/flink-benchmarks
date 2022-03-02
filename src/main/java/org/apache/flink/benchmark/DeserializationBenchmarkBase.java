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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import org.apache.flink.api.common.functions.MapFunction;

import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;

import java.util.ArrayList;
import java.util.List;


@State(Scope.Thread)
public abstract class DeserializationBenchmarkBase extends BenchmarkBase {
    public static final int RECORDS_PER_INVOCATION = 10_000_000;
    public static final int TUPLE_BYTES = 2_000_000;

    protected SerializedTuplesRingSource source;

    @Setup
    public void prepare() throws Exception {
        ArrayList<Byte> serializedTuples = new ArrayList<Byte>(TUPLE_BYTES);
        ArrayList<Integer> serializedTupleSizes = new ArrayList<Integer>();

        while(true) {
            NativeTuple tup = NativeTuple.withRandomValues();
            byte[] tupBytes = serializeNativeTuple(tup);
            List<Byte> tupBytesList = new ArrayList<Byte>(tupBytes.length);

            if(serializedTuples.size() + tupBytesList.size() >= TUPLE_BYTES) {
                break;
            }

            for(byte b : tupBytes) tupBytesList.add(b);
            serializedTuples.addAll(tupBytesList);
            serializedTupleSizes.add(tupBytesList.size());
        }

        source = new SerializedTuplesRingSource(serializedTuples, serializedTupleSizes, RECORDS_PER_INVOCATION);
    }

    public abstract byte[] serializeNativeTuple(NativeTuple tup) throws Exception;

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void benchDeserialization(FlinkEnvironmentContext context) throws Exception {
        executeBenchmark(context);
    }


    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void benchSetup(FlinkEnvironmentContext context) throws Exception {
        long oldTargetCount = source.targetCount;
        source.targetCount = 0;

        executeBenchmark(context);

        source.targetCount = oldTargetCount;
    }

    private void executeBenchmark(FlinkEnvironmentContext context) throws Exception {
        // similar to what flink does in their own benchmarks, see
        // https://github.com/apache/flink-benchmarks/blob/ffcdbb45c88e9453bd815229eebaec1e776722cb/src/main/java/org/apache/flink/benchmark/SerializationFrameworkMiniBenchmarks.java#L129
        StreamExecutionEnvironment env = context.env;
        env.setParallelism(1);

        DataStream<byte[]> rawStream = env.addSource(source);
        DataStream<NativeTuple> tupleStream = addDeserializationOperations(rawStream);

        tupleStream
        .map(new MapFunction<NativeTuple, Integer>() {
            @Override
            public Integer map(NativeTuple tup) throws Exception {
                return tup.readAccess();
            }
        })
        .addSink(new DiscardingSink<>());
        // The DataStream API can not give us a sum of all numbers on a stream without keying and windowing. Well, then we have to do it.
        // .keyBy(value -> 0)
        // .countWindow(RECORDS_PER_INVOCATION)
        // .sum(0)
        // .printToErr();

        env.execute();
    }

    public abstract DataStream<NativeTuple> addDeserializationOperations(DataStream<byte[]> stream);
}
