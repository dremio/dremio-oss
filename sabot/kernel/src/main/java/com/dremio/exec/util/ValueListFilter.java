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

import static org.apache.arrow.util.Preconditions.checkArgument;
import static org.apache.arrow.util.Preconditions.checkNotNull;
import static org.apache.arrow.util.Preconditions.checkState;

import java.nio.charset.StandardCharsets;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.types.Types;

import com.dremio.sabot.op.common.ht2.Copier;

/**
 * Used for runtime filtering at joins. Contains list of unique and sorted join key values
 */
public class ValueListFilter implements AutoCloseable {
    public static final int META_SIZE = 33;

    private ArrowBuf fullBuffer;
    private ArrowBuf valueListSlice;

    private int valueCount;
    private byte blockSize;
    private String name;
    private boolean isFixedWidth = true;
    private boolean isBoolField;
    private boolean containsNull;
    private Types.MinorType fieldType;
    private byte precision;
    private byte scale;
    private String fieldName;

    // Applicable only for boolean datasets
    private boolean containsTrue;
    private boolean containsFalse;

    ValueListFilter(ArrowBuf fullBuffer) {
        checkNotNull(fullBuffer);
        checkArgument(fullBuffer.capacity() >= META_SIZE);

        this.fullBuffer = fullBuffer;
        this.valueListSlice = fullBuffer.slice(META_SIZE, fullBuffer.capacity() - META_SIZE);
    }

    public void initializeMetaFromBuffer() {
        // Meta information is set up against the byte positions as follows -
        // [0:24 - string name|24:28 - int valCount| 28:29 - byte blockSize|29:30 - arrow type|30:31 - precision|31:32 - scale|32:33 - control bytes]
        byte[] nameBytes = new byte[24];
        fullBuffer.getBytes(0, nameBytes);
        this.name = new String(nameBytes, StandardCharsets.UTF_8).trim();

        valueCount = fullBuffer.getInt(24);
        blockSize = fullBuffer.getByte(28);

        final byte arrowTypeId = fullBuffer.getByte(29);
        fieldType = Types.MinorType.values()[arrowTypeId];
        this.precision = fullBuffer.getByte(30);
        this.scale = fullBuffer.getByte(31);

        // Control bits are organized as - 0:isFixedWidth, 1:containsNull, 2:isBoolField, 3:containsTrue, 4:containsFalse
        // 3 & 4 are applicable only if vector is representing a boolean value set. Set to zero otherwise.
        final byte controlByte = fullBuffer.getByte(32);
        this.isFixedWidth = getBit(controlByte, 0);
        this.containsNull = getBit(controlByte, 1);
        this.isBoolField = getBit(controlByte, 2);
        this.containsTrue = getBit(controlByte, 3);
        this.containsFalse = getBit(controlByte, 4);

        fullBuffer.readerIndex(0);
        fullBuffer.writerIndex(META_SIZE + (blockSize * valueCount));
    }

    public void writeMetaToBuffer() {
        // Meta information is set up against the byte positions as follows -
        // [0:24 - string name|24:28 - int valCount| 28:29 - byte blockSize|29:30 - arrow type|30:31 - control bytes]

        byte[] nameBytesTrimmed = new byte[24];
        byte[] nameBytesAll = name.getBytes(StandardCharsets.UTF_8);
        System.arraycopy(name.getBytes(StandardCharsets.UTF_8), Math.max(0, nameBytesAll.length - 24), nameBytesTrimmed, 0, Math.min(24, nameBytesAll.length));
        this.name = new String(nameBytesTrimmed, StandardCharsets.UTF_8).trim();
        this.fullBuffer.setBytes(0, nameBytesTrimmed);

        this.fullBuffer.setInt(24, valueCount);
        this.fullBuffer.setByte(28, blockSize);
        this.fullBuffer.setByte(29, fieldType.ordinal());
        this.fullBuffer.setByte(30, precision);
        this.fullBuffer.setByte(31, scale);

        byte controlByte = 0;
        controlByte = copySetBit(controlByte, 0, isFixedWidth);
        controlByte = copySetBit(controlByte, 1, containsNull);
        controlByte = copySetBit(controlByte, 2, isBoolField);
        controlByte = copySetBit(controlByte, 3, containsTrue);
        controlByte = copySetBit(controlByte, 4, containsFalse);
        this.fullBuffer.setByte(32, controlByte);

        fullBuffer.readerIndex(0);
        fullBuffer.writerIndex(META_SIZE + (blockSize * valueCount));
    }

    public ArrowBuf buf() {
        return fullBuffer;
    }

    public ArrowBuf valOnlyBuf() {
        return valueListSlice;
    }

    public int getValueCount() {
        return valueCount;
    }

    public void setValueCount(int valueCount) {
        this.valueCount = valueCount;
    }

    public byte getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(byte blockSize) {
        this.blockSize = blockSize;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isFixedWidth() {
        return isFixedWidth;
    }

    public void setFixedWidth(boolean fixedWidth) {
        isFixedWidth = fixedWidth;
    }

    public boolean isBoolField() {
        return isBoolField;
    }

    public void setBoolField(boolean boolField) {
        isBoolField = boolField;
    }

    public boolean isContainsNull() {
        return containsNull;
    }

    public void setContainsNull(boolean containsNull) {
        this.containsNull = containsNull;
    }

    public Types.MinorType getFieldType() {
        return fieldType;
    }

    public void setFieldType(Types.MinorType fieldType, byte precision, byte scale) {
        this.fieldType = fieldType;
        this.precision = precision;
        this.scale = scale;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public boolean isCompatible(ValueListFilter that) {
        return (this.blockSize == that.blockSize)
                && (this.fieldType.equals(that.fieldType));
    }

    private void copyMetaProperties(ValueListFilter that) {
        this.setBlockSize(that.getBlockSize());
        this.setFieldType(that.fieldType, that.precision, that.scale);
        this.setBoolField(that.isBoolField);
        this.setFixedWidth(that.isFixedWidth);
        this.setName(that.getName());
    }

    protected static byte copySetBit(byte b, int pos, boolean value) {
        return (byte) (value ? b | 1 << pos : b & ~(1 << pos));
    }

    protected static boolean getBit(byte b, int pos) {
        return (b & (1L << pos)) != 0;
    }

    protected ArrowCrossBufComparator getComparator() {
        return this.isFixedWidth() ? ArrowCrossBufComparatorProvider.get(this.getFieldType()) : new ValueListVarWidthFilterComparator(blockSize);
    }

    private boolean containsAllBoolCombinations() {
        return isBoolField && containsNull && containsTrue && containsFalse;
    }

    /**
     * Performs a merge of two value lists and puts in the supplied mergedVal list. Assumes that both incoming value lists
     * are compatible, and contain distinct values in sorted fashion.
     *
     * @param valList1
     * @param valList2
     * @param mergedValList
     */
    public static void merge(final ValueListFilter valList1, final ValueListFilter valList2, final ValueListFilter mergedValList) {
        checkArgument(valList1.isCompatible(valList2), "Incompatible value list filters %s %s", valList1, valList2);
        mergedValList.copyMetaProperties(valList1);
        mergedValList.setValueCount(0);
        mergedValList.setContainsNull(valList1.isContainsNull() || valList2.isContainsNull());

        if (valList1.isBoolField()) {
            // Value merge not required
            mergedValList.setContainsFalse(valList1.isContainsFalse() || valList2.isContainsFalse());
            mergedValList.setContainsTrue(valList1.isContainsTrue() || valList2.isContainsTrue());
            checkState(!mergedValList.containsAllBoolCombinations(), "Merged buffer contains all boolean combinations. " +
                    "Unlikely to filter anything.");
            return;
        }

        final ArrowCrossBufComparator comparator = valList1.getComparator();
        long mergeIdxCap = mergedValList.valOnlyBuf().capacity() / mergedValList.getBlockSize();
        int idx1 = 0, idx2 = 0, mergedIdx = 0;
        while (idx1 < valList1.getValueCount() || idx2 < valList2.getValueCount()) {
            checkState(mergedIdx < mergeIdxCap, "Merged buffer overflown.");
            if (idx1 < valList1.getValueCount() &&
                    // If idx2 is saturated or idx2 value loses in comparison.
                    (idx2 == valList2.getValueCount() ||
                            comparator.compare(valList1.valOnlyBuf(), idx1, valList2.valOnlyBuf(), idx2) < 0)) {
                // Insert if not duplicate of last inserted value, skip otherwise
                if ((mergedIdx == 0) ||
                        comparator.compare(mergedValList.valOnlyBuf(), mergedIdx - 1, valList1.valOnlyBuf(), idx1) != 0) {
                    copyValue(valList1, idx1, mergedValList, mergedIdx++);
                }
                idx1++;
            } else if (idx2 < valList2.getValueCount()) {
                // Insert if not duplicate of last inserted value, skip otherwise
                if ((mergedIdx == 0) ||
                        comparator.compare(mergedValList.valOnlyBuf(), mergedIdx - 1, valList2.valOnlyBuf(), idx2) != 0) {
                    copyValue(valList2, idx2, mergedValList, mergedIdx++);
                }
                idx2++;
            }
        }
        mergedValList.setValueCount(mergedIdx);
        mergedValList.writeMetaToBuffer();
    }

    public boolean isContainsTrue() {
        return containsTrue;
    }

    public void setContainsTrue(boolean containsTrue) {
        this.containsTrue = containsTrue;
    }

    public boolean isContainsFalse() {
        return containsFalse;
    }

    public void setContainsFalse(boolean containsFalse) {
        this.containsFalse = containsFalse;
    }

    public byte getPrecision() {
        return precision;
    }

    public void resetPrecision(byte newPrecision) {
        this.precision = newPrecision;
    }

    public byte getScale() {
        return scale;
    }

    public void resetScale(byte newScale) {
        this.scale = newScale;
    }

    private static void copyValue(ValueListFilter src, int srcIdx, ValueListFilter dst, int dstIdx) {
        Copier.copy(src.valOnlyBuf().memoryAddress() + (srcIdx * src.getBlockSize()),
                dst.valOnlyBuf().memoryAddress() + (dstIdx * dst.getBlockSize()),
                src.getBlockSize());
    }

    @Override
    public String toString() {
        return "ValueListFilter{" +
                "valueCount=" + valueCount +
                ", blockSize=" + blockSize +
                ", name='" + name + '\'' +
                ", fieldType=" + fieldType +
                ", fixedWidth=" + isFixedWidth() +
                '}';
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.close(fullBuffer);
    }

    public long getSizeInBytes() {
        return META_SIZE + (valueCount * blockSize);
    }

    /**
     * Increases the ref count of the underlying buffer
     */
    public void retainRef() {
        this.buf().retain();
    }
}
