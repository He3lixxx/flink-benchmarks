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

import java.util.concurrent.ThreadLocalRandom;

import java.nio.charset.StandardCharsets;

public class NativeTuple {
    public long id;
    public long timestamp;
    public float load;
    public float load_avg_1;
    public float load_avg_5;
    public float load_avg_15;
    public byte[] container_id = new byte[32];

    public static NativeTuple withRandomValues() {
        ThreadLocalRandom r = ThreadLocalRandom.current();

        NativeTuple tup = new NativeTuple();
        tup.id = r.nextLong();
        tup.timestamp = r.nextLong();
        tup.load = r.nextFloat();
        tup.load_avg_1 = r.nextFloat();
        tup.load_avg_5 = r.nextFloat();
        tup.load_avg_15 = r.nextFloat();
        r.nextBytes(tup.container_id);

        return tup;
    }

    // https://stackoverflow.com/a/9855338/12345551
    private static final byte[] HEX_ARRAY = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);

    public String containerIdStr() {
        // HexFormat can do this from java17 on, but it's not widely available yet.
        byte[] hexChars = new byte[container_id.length * 2];
        for (int j = 0; j < container_id.length; j++) {
            int v = container_id[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars, StandardCharsets.UTF_8);
    }

    public void setContainerIdFromStr(String idStr) throws IllegalArgumentException {
        if(idStr.length() != 64) {
            throw new IllegalArgumentException("String of length 64 expected. Got '" + idStr + "'");
        }

        for (int i = 0; i < 64; i += 2) {
            container_id[i / 2] = (byte) ((Character.digit(idStr.charAt(i), 16) << 4) + Character.digit(idStr.charAt(i+1), 16));
        }
    }

    public int readAccess() {
        return (int)id ^ (int)timestamp ^ (int)load ^ (int)load_avg_1 ^ (int)load_avg_5 ^ (int)load_avg_15 ^ (int)container_id[0];
    }
}

