/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.util;

import org.apache.arrow.memory.ArrowBuf;

import com.google.common.primitives.UnsignedBytes;

/**
 * Comparator to be used while comparing {@link ValueListFilter} comprising of variable length data types.
 */
public class ValueListVarWidthFilterComparator implements ArrowCrossBufComparator {
    private final int blockSize;

    public ValueListVarWidthFilterComparator(int blockSize) {
        this.blockSize = blockSize;
    }

    @Override
    public int compare(ArrowBuf buf1, int idx1, ArrowBuf buf2, int idx2) {
        // First byte in the block tells the length of value; remaining blocks tell the value.
        idx1 *= blockSize;
        idx2 *= blockSize;

        byte[] val1Bytes = new byte[buf1.getByte(idx1)];
        byte[] val2Bytes = new byte[buf2.getByte(idx2)];

        buf1.getBytes(idx1 + blockSize - val1Bytes.length, val1Bytes);
        buf2.getBytes(idx2 + blockSize - val2Bytes.length, val2Bytes);

        return UnsignedBytes.lexicographicalComparator().compare(val1Bytes, val2Bytes);
    }
}
