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
import org.openjdk.jmh.annotations.OperationsPerInvocation;

import java.util.ArrayList;
import java.util.List;


@State(Scope.Thread)
public abstract class DeserializationBenchmarkBase extends BenchmarkBase {
    public static final int RECORDS_PER_INVOCATION = 1_000_000;
    public static final int TUPLE_BYTES = 2_000_000;

    private SerializedTuplesRingSource source;

    @Setup
    public void prepare() {
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

    public abstract byte[] serializeNativeTuple(NativeTuple tup);

    @Benchmark
    @OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
    public void bench(FlinkEnvironmentContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;
        env.setParallelism(1);

        DataStream<List<Byte>> rawStream = env.addSource(source);
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

        // System.err.println("One execution done");
        env.execute();
    }

    public abstract DataStream<NativeTuple> addDeserializationOperations(DataStream<List<Byte>> stream);
}
