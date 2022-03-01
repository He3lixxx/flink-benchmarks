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

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.ExecutionConfig;

import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Scope;


@State(Scope.Thread)
public class PojoDeserializationBenchmark extends DeserializationBenchmarkBase {
    private final DataOutputSerializer dataOutput = new DataOutputSerializer(128);
    // Passing the "wrong" execution config is ok here, the PojoSerializer only uses it to find registered subclasses of the POJO. We don't have any.
    private PojoSerializer<NativeTuple> serializer =
        (PojoSerializer<NativeTuple>) TypeInformation.of(NativeTuple.class).createSerializer(new ExecutionConfig());

    private final PojoDeserializationMapper mapper = new PojoDeserializationMapper();

    static class PojoDeserializationMapper implements MapFunction<byte[], NativeTuple> {
        private final PojoSerializer<NativeTuple> serializer =
            (PojoSerializer<NativeTuple>) TypeInformation.of(NativeTuple.class).createSerializer(new ExecutionConfig());
        private final DataInputDeserializer dataInput = new DataInputDeserializer();

        @Override
        public NativeTuple map(byte[] bytes) throws Exception {
            dataInput.setBuffer(bytes);
            return serializer.deserialize(dataInput);
        }
    }

    @Override
    public byte[] serializeNativeTuple(NativeTuple tup) throws Exception {
        dataOutput.clear();
        serializer.serialize(tup, dataOutput);
        return dataOutput.getCopyOfBuffer();
    }

    @Override
    public DataStream<NativeTuple> addDeserializationOperations(DataStream<byte[]> stream) {
        return stream.map(mapper);
    }
}
