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

import java.util.List;

import org.apache.flink.streaming.api.functions.source.SourceFunction;


public class SerializedTuplesRingSource implements SourceFunction<List<Byte>> {
    private volatile boolean running = true;

    private int byteOffset = 0;
    private int sizeOffset = 0;

    List<Byte> bytes;
    List<Integer> sizes;

    long targetCount;

    public SerializedTuplesRingSource(List<Byte> bytes, List<Integer> sizes, long targetCount) {
        this.bytes = bytes;
        this.sizes = sizes;
        this.targetCount = targetCount;
    }

    @Override
    public void run(SourceContext<List<Byte>> ctx) throws Exception {
        long counter = 0;

        running = true;
        while (running) {
            int currentSize = this.sizes.get(this.sizeOffset);
            int nextByteOffset = this.byteOffset + currentSize;
            ctx.collect(this.bytes.subList(this.byteOffset, nextByteOffset));

            counter++;
            this.sizeOffset++;
            this.byteOffset = nextByteOffset;

            if (this.sizeOffset >= this.sizes.size()) {
                this.sizeOffset = 0;
                this.byteOffset = 0;
            }

            if (counter >= this.targetCount) {
                cancel();
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
