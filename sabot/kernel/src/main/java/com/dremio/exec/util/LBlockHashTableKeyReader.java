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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;

import com.dremio.common.AutoCloseables;
import com.dremio.sabot.op.common.ht2.Copier;
import com.dremio.sabot.op.common.ht2.PivotBuilder.FieldMode;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.common.ht2.VectorPivotDef;

import io.netty.util.internal.PlatformDependent;

/**
 * Prepares the key for join runtime filter by parsing keys in the HashTable, only for the required fields.
 * The filter can be used only in the HashJoin scenarios where the keys are present in {@link com.dremio.sabot.op.common.hashtable.HashTable}.
 * The functionality uses a fixed ArrowBuf and uses the same for delivering all parsed keys. The keys are loaded in same space.
 */
public class LBlockHashTableKeyReader implements AutoCloseable {
    private ArrowBuf keyBuf;
    private int keyBufSize = 32; // max
    private long tableFixedAddresses[];
    private long tableVarAddresses[];
    private int bitsInChunk;
    private int chunkOffsetMask;
    private boolean isCompositeKey;
    private List<KeyPosition> keyPositions;
    private int keyValidityBytes;
    private int fixedBlockWidth;
    private int varBlockWidth;
    private int totalNumOfRecords = -1;
    private int rowIndex = 0;
    private int dataChunkIndex = -1;
    private boolean isKeyTrimmedToFitSize = false;
    private boolean isVarWidthColPresent = false;
    private long currentFixedAddr = -1L;
    private long currentVarAddr = -1L;
    private boolean setVarFieldLenInFirstByte = false;

    private LBlockHashTableKeyReader() {
        // Always use builder
    }

    public int getKeyBufSize() {
        return keyBufSize;
    }

    /**
     * Size after removing the validity bytes
     *
     * @return
     */
    public byte getEffectiveKeySize() {
        return (byte) (getKeyBufSize() - keyValidityBytes);
    }

    public boolean isCompositeKey() {
        return isCompositeKey;
    }

    public boolean isKeyTrimmedToFitSize() {
        return isKeyTrimmedToFitSize;
    }

    /**
     * Populates next composite key in the key buffer. The keys are trimmed in case total size exceeds the limit according
     * to the logic described in {@link KeyFairSliceCalculator}.
     *
     * @return true if a new key is loaded, false if there are no more keys to read
     */
    public boolean loadNextKey() {
        /*
         * The hashtable maintains two sets of addresses, one for fixed width columns and one for variable width columns.
         * Each address type stores the data in form of continuous memory chunks; there could be multiple chunks possible.
         * Fixed width chunk starting addresses are stored at "tableFixedAddresses" and respective variable width address
         * are stored in "tableVarAddresses".
         *
         * This class tracks running block's starting addresses via "currentFixedAddr" and "currentVarAddr". The two step
         * operation includes -
         * step a: Move "currentFixedAddr" and "currentVarAddr" to the starting of next block respectively.
         * step b: Iterate over keys of interest in required order, and use block specific offset position, against currentAddr index, to read the values.
         */
        if (rowIndex >= totalNumOfRecords) {
            return false;
        }

        moveCurrentAddressesToNextRow();

        // zero out the key buf
        keyBuf.setBytes(0, new byte[this.keyBufSize]);

        int keyBufValueOffset = keyValidityBytes;
        int keyBufValidityBitOffset = 0;
        int keyBufValidityByteOffset = 0;
        if (isVarWidthColPresent) {
          // we need to update varBlockWidth even if the keys are not valid
          varBlockWidth = PlatformDependent.getInt(currentVarAddr) + 4;
        }

        for(KeyPosition keyPosition : keyPositions) {
            boolean valid = ((PlatformDependent.getInt(currentFixedAddr + keyPosition.nullByteOffset) >>> keyPosition.nullBitOffset) & 1) == 1;
            if (valid) {
                // write validity bit
                PlatformDependent.putByte(keyBuf.memoryAddress() + keyBufValidityByteOffset, (byte) (PlatformDependent.getByte(keyBuf.memoryAddress() + keyBufValidityByteOffset) | (byte) (1 << keyBufValidityBitOffset)));
                // write value
                switch (keyPosition.fieldType) {
                  case BIT:
                    Preconditions.checkArgument(keyPosition.offset == keyPosition.nullBitOffset + 1);
                    int value = (PlatformDependent.getInt(currentFixedAddr + keyPosition.nullByteOffset) >>> keyPosition.offset) & 1;

                    keyBufValidityByteOffset += (keyBufValidityBitOffset + 1) / Byte.SIZE;
                    keyBufValidityBitOffset = (keyBufValidityBitOffset + 1) % Byte.SIZE;
                    PlatformDependent.putByte(keyBuf.memoryAddress() + keyBufValidityByteOffset, (byte) (PlatformDependent.getByte(keyBuf.memoryAddress() + keyBufValidityByteOffset) | (byte) (value << keyBufValidityBitOffset)));
                    break;
                  case FIXED:
                    Copier.copy(currentFixedAddr + keyPosition.offset, keyBuf.memoryAddress() + keyBufValueOffset, keyPosition.allocatedLength);
                    break;
                  case VARIABLE:
                    // Data is arranged in following fashion -
                    // [row_len|key1_len|key1|key2_len|key2|...] // Each length field is a 4 byte int
                    // Navigate to the required key position within the block
                    long keyAddr = currentVarAddr + 4;
                    int keySequenceNo = keyPosition.offset;
                    while (--keySequenceNo > 0) {
                      keyAddr += (4 + PlatformDependent.getInt(keyAddr));
                    }
                    int keyFieldLength = PlatformDependent.getInt(keyAddr);
                    int allocatedLen = keyPosition.allocatedLength;
                    // prefix pad with 0 bytes if data value is shorter than allocation
                    int keySizeForCopy = Math.min(allocatedLen, keyFieldLength);
                    int offset = keyBufValueOffset;
                    if (setVarFieldLenInFirstByte) {
                        keySizeForCopy = (keySizeForCopy==allocatedLen) ? keySizeForCopy - 1:keySizeForCopy;
                        keyBuf.setByte(offset++, (byte) keySizeForCopy);
                        allocatedLen--;
                    }
                    Copier.copy(keyAddr + 4, keyBuf.memoryAddress() + offset + allocatedLen - keySizeForCopy, keySizeForCopy);
                    break;
                }
            }
            keyBufValueOffset += keyPosition.allocatedLength;
            // If invalid boolean value advance by 2 bits else by 1 bit
            keyBufValidityByteOffset += (keyBufValidityBitOffset + ((!valid && keyPosition.fieldType == FieldMode.BIT) ? 2 : 1)) / Byte.SIZE;
            keyBufValidityBitOffset = (keyBufValidityBitOffset + ((!valid && keyPosition.fieldType == FieldMode.BIT) ? 2 : 1)) % Byte.SIZE;
        }
        rowIndex++;
        return true;
    }

    private void moveCurrentAddressesToNextRow() {
        /*
         * Moves the current indexes (currentFixedAddr, currentVarAddr) to the next block value.
         * If data chunk has ended, indexes are moved to the first block of the next data chunk. If not, next block
         * indices are calculated in following fashion -
         *
         * a. For variable width block address, move the address by num of positions equal to last block's width.
         * b. For fixed width block address, block width is constant, so calculate the position based on offset.
         */
        int newDataChunkIndex = rowIndex >>> bitsInChunk;
        if (newDataChunkIndex != dataChunkIndex) {
            dataChunkIndex = newDataChunkIndex;

            if (isVarWidthColPresent) {
                currentVarAddr = tableVarAddresses[dataChunkIndex];
                varBlockWidth = 0;
            }
        }

        if (isVarWidthColPresent) {
            currentVarAddr += varBlockWidth;
        }

        final int offsetInChunk = rowIndex & chunkOffsetMask;
        currentFixedAddr = tableFixedAddresses[dataChunkIndex] + (offsetInChunk * fixedBlockWidth);
    }

    public ArrowBuf getKeyHolder() {
        return keyBuf;
    }

    /**
     * Key without validity byte
     * @return
     */
    public ArrowBuf getKeyValBuf() {
        return keyBuf.slice(keyValidityBytes, keyBufSize - keyValidityBytes);
    }

    /**
     * Checks the validity byte if all values are null.
     *
     * @return
     */
    public boolean areAllValuesNull() {
        byte[] validityBytes = new byte[keyValidityBytes];
        keyBuf.getBytes(0, validityBytes);
        for (byte validityByte : validityBytes) {
            if (validityByte != 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.close(keyBuf);
    }

    public static class Builder {
        private LBlockHashTableKeyReader keyReader = new LBlockHashTableKeyReader();
        private List<String> fieldNames = new ArrayList<>();
        private PivotDef pivot;
        private int maxValuesPerBatch = -1;
        private BufferAllocator bufferAllocator;
        private int maxKeySize = 32;

        public Builder setFieldsToRead(List<String> fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder setMaxKeySize(int maxKeySize) {
            this.maxKeySize = maxKeySize;
            return this;
        }

        public Builder setPivot(PivotDef pivot) {
            this.pivot = pivot;
            return this;
        }

        public Builder setMaxValuesPerBatch(int maxValuesPerBatch) {
            this.maxValuesPerBatch = maxValuesPerBatch;
            return this;
        }

        public Builder setTableFixedAddresses(long[] tableFixedAddresses) {
            keyReader.tableFixedAddresses = tableFixedAddresses;
            return this;
        }

        public Builder setTableVarAddresses(long[] tableVarAddresses) {
            keyReader.tableVarAddresses = tableVarAddresses;
            return this;
        }

        public Builder setTotalNumOfRecords(int size) {
            keyReader.totalNumOfRecords = size;
            return this;
        }

        public Builder setBufferAllocator(BufferAllocator bufferAllocator) {
            this.bufferAllocator = bufferAllocator;
            return this;
        }

        public Builder setSetVarFieldLenInFirstByte(boolean setVarFieldLenInFirstByte) {
            keyReader.setVarFieldLenInFirstByte = setVarFieldLenInFirstByte;
            return this;
        }

        public LBlockHashTableKeyReader build() {
            checkArgument(keyReader.totalNumOfRecords > 0, "Total number of records not set");
            checkArgument(CollectionUtils.isNotEmpty(fieldNames), "Fields to read cannot be empty");
            checkNotNull(pivot);
            checkArgument((maxValuesPerBatch!=-1), "MaxHashTableBatchSize not set");

            keyReader.bitsInChunk = Long.numberOfTrailingZeros(maxValuesPerBatch);
            keyReader.chunkOffsetMask = (1 << keyReader.bitsInChunk) - 1;

            keyReader.fixedBlockWidth = pivot.getBlockWidth();
            keyReader.keyPositions = new ArrayList<>(fieldNames.size());
            keyReader.isCompositeKey = fieldNames.size() > 1;

            prepareKeyPositions(fieldNames);
            checkArgument(!keyReader.isVarWidthColPresent || (keyReader.isVarWidthColPresent && !ArrayUtils.isEmpty(keyReader.tableVarAddresses)), "TableVarAddresses not set");
            checkArgument(!ArrayUtils.isEmpty(keyReader.tableFixedAddresses), "TableFixedAddresses not set");
            sliceKeyLenToFitInMaxSize();
            try {
                // If you want to initialise another closeable here, ensure closure at error cases when resources are partially initialised.
                keyReader.keyBuf = bufferAllocator.buffer(keyReader.keyBufSize);
                return keyReader;
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        private void sliceKeyLenToFitInMaxSize() {
            // Take slices of keys so total key fits in 16 bytes
            final Map<String, Integer> keySizes = keyReader.keyPositions.stream()
                    .collect(Collectors.toMap(KeyPosition::getKey, KeyPosition::getAllocatedLength, (x, y) -> y, LinkedHashMap::new));
            final KeyFairSliceCalculator fairSliceCalculator = new KeyFairSliceCalculator(keySizes, maxKeySize);
            keyReader.keyPositions.stream().forEach(k -> k.setAllocatedLength(fairSliceCalculator.getKeySlice(k.getKey())));
            keyReader.keyBufSize = fairSliceCalculator.getTotalSize();
            keyReader.isKeyTrimmedToFitSize = fairSliceCalculator.keysTrimmed();
            keyReader.keyValidityBytes = fairSliceCalculator.numValidityBytes();
        }

        private void prepareKeyPositions(List<String> fieldNames) {
            for (String fieldName : fieldNames) {
                // Search in fixed width fields
                Optional<KeyPosition> fixedWidthColKeyPosition = findKeyPositionInFixedWidthCols(fieldName);
                if (fixedWidthColKeyPosition.isPresent()) {
                    keyReader.keyPositions.add(fixedWidthColKeyPosition.get());
                    continue;
                }

                // Search in variable width columns.
                keyReader.keyPositions.add(
                  findKeyPositionInVarWidthCols(fieldName).orElseThrow(() -> new IllegalArgumentException("Field name " + fieldName + " not find in pivot.")));
                keyReader.isVarWidthColPresent = true;
            }
        }

        private Optional<KeyPosition> findKeyPositionInVarWidthCols(String fieldName) {
            // Search in variable width fields
            int sequencePosition = 1;
            for (VectorPivotDef vectorPivotDef : this.pivot.getVariablePivots()) {
                final Field field = vectorPivotDef.getIncomingVector().getField();
                if (field.getName().equalsIgnoreCase(fieldName)) {
                    checkArgument(!field.getType().isComplex(), "Runtime filter not supported on complex type fields");
                    return Optional.of(new KeyPosition(fieldName, sequencePosition, vectorPivotDef.getNullByteOffset(), vectorPivotDef.getNullBitOffset(), Integer.MAX_VALUE, FieldMode.VARIABLE));
                }
                sequencePosition++;
            }
            return Optional.empty();
        }

        private Optional<KeyPosition> findKeyPositionInFixedWidthCols(String fieldName) {
            for (VectorPivotDef vectorPivotDef : this.pivot.getFixedPivots()) {
                final Field field = vectorPivotDef.getIncomingVector().getField();
                if (field.getName().equalsIgnoreCase(fieldName)) {
                    checkArgument(!field.getType().isComplex(), "Runtime filter not supported on complex type fields");
                    return Optional.of(new KeyPosition(fieldName, vectorPivotDef.getOffset(), vectorPivotDef.getNullByteOffset(),
                            vectorPivotDef.getNullBitOffset(), vectorPivotDef.getType().byteSize, vectorPivotDef.getType().mode));
                }
            }

            return Optional.empty();
        }
    }

    private static class KeyPosition {
        private final String key;
        private final int offset;
        private final int nullByteOffset;
        private final int nullBitOffset;

        public void setAllocatedLength(int allocatedLength) {
            this.allocatedLength = allocatedLength;
        }

        private int allocatedLength;
        private FieldMode fieldType;

        private KeyPosition(String key, int offset, int nullByteOffset, int nullBitOffset, int allocatedLength, FieldMode fieldType) {
            this.key = key;
            this.offset = offset;
            this.nullByteOffset = nullByteOffset;
            this.nullBitOffset = nullBitOffset;
            this.allocatedLength = allocatedLength;
            this.fieldType = fieldType;
        }

        public String getKey() {
            return key;
        }

        public int getAllocatedLength() {
            return allocatedLength;
        }

        @Override
        public String toString() {
            return "KeyPosition{" +
                    "offset=" + offset +
                    ", length=" + allocatedLength +
                    '}';
        }
    }
}
