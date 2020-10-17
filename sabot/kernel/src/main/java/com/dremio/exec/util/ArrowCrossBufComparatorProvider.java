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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.util.BasicTypeHelper;

/**
 * Provider for {@link ArrowCrossBufComparator} on default data types
 */
public class ArrowCrossBufComparatorProvider {

    public static ArrowCrossBufComparator getByteComparator() {
        return (buf1, idx1, buf2, idx2) -> Byte.compare(buf1.getByte(idx1), buf2.getByte(idx2));
    }

    public static ArrowCrossBufComparator get2ByteNumComparator() {
        return (buf1, idx1, buf2, idx2) -> Short.compare(buf1.getShort(idx1 * 2), buf2.getShort(idx2 * 2));
    }

    public static ArrowCrossBufComparator get4ByteNumComparator() {
        return (buf1, idx1, buf2, idx2) -> Integer.compare(buf1.getInt(idx1 * 4), buf2.getInt(idx2 * 4));
    }

    public static ArrowCrossBufComparator get8ByteNumComparator() {
        return (buf1, idx1, buf2, idx2) -> Long.compare(buf1.getLong(idx1 * 8), buf2.getLong(idx2 * 8));
    }

    public static ArrowCrossBufComparator getCustomByteNumComparator(int blockSize) {
        return (buf1, idx1, buf2, idx2) -> {
            final byte[] bytes1 = new byte[blockSize];
            final byte[] bytes2 = new byte[blockSize];
            buf1.getBytes(idx1 * blockSize, bytes1);
            buf2.getBytes(idx2 * blockSize, bytes2);

            return new BigInteger(bytes1).compareTo(new BigInteger(bytes2));
        };
    }

    public static ArrowCrossBufComparator getDecimalComparator(int blockSize) {
        return (buf1, idx1, buf2, idx2) -> {
            final byte[] bytes1 = new byte[DecimalUtils.DECIMAL_WIDTH];

            // Expecting same scale across the complete buf. Hence, just comparing unscaled value.
            BigDecimal decimal1 = DecimalUtils.getBigDecimalFromLEBytes(buf1.memoryAddress() + (idx1 * blockSize), bytes1, 0);
            BigDecimal decimal2 = DecimalUtils.getBigDecimalFromLEBytes(buf1.memoryAddress() + (idx2 * blockSize), bytes1, 0);

            return decimal1.compareTo(decimal2);
        };
    }

    public static ArrowCrossBufComparator get(Types.MinorType type) {
        if (!comparators.containsKey(type)) {
            initializeComparator(type);
        }

        return comparators.get(type);
    }

    private static final Map<Types.MinorType, ArrowCrossBufComparator> comparators =
            new HashMap<>(Types.MinorType.values().length);

    private static void initializeComparator(final Types.MinorType type) {
        switch (BasicTypeHelper.getSize(type)) {
            case 1:
                comparators.put(type, getByteComparator());
                break;
            case 2:
                comparators.put(type, get2ByteNumComparator());
                break;
            case 4:
                comparators.put(type, get4ByteNumComparator());
                break;
            case 8:
                comparators.put(type, get8ByteNumComparator());
                break;
            case 16:
                if (Types.MinorType.DECIMAL.equals(type)) {
                    comparators.put(type, getDecimalComparator(16));
                    break;
                }
                comparators.put(type, getCustomByteNumComparator(16));
                break;
            default:
                throw new IllegalArgumentException("Comparator is not implemented for " + type);
        }
    }
}
