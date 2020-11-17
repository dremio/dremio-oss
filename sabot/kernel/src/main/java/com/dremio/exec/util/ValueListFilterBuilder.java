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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.Types;

import com.dremio.common.expression.fn.impl.MurmurHash3;
import com.dremio.sabot.op.common.ht2.Copier;

/**
 * Helps to prepare value list with unique values in a sorted fashion.
 */
public class ValueListFilterBuilder implements AutoCloseable{
    // Arranged as hash buckets pointing to a linked list of values. hashBuckets contains hashKey positions,
    // valueList contains data at index, hashKeyNextIndexes contains next pointer for the data node, -1 otherwise.
    private ArrowBuf hashBuckets;
    private ArrowBuf hashKeyNextIndexes;
    private ArrowBuf valuesList;
    private BufferAllocator allocator;

    private ValueListFilter valueListFilter;

    private int capacity;
    private byte blockSize;
    private int nextEmptyIndex = 0;
    private int maxHashBuckets;
    private boolean isBoolean;

    private List<AutoCloseable> closeables = new ArrayList<>();

    public ValueListFilterBuilder(final BufferAllocator allocator, final int capacity, final byte blockSize,
                                  final boolean isBoolean) {
        checkNotNull(allocator, "Allocator is null");
        checkArgument(capacity > 0, "Invalid capacity");
        if (isBoolean) {
            checkArgument(blockSize == 0, "Block size should be zero for boolean fields");
        } else {
            checkArgument(blockSize > 0, "Block size should be greater than zero for non-boolean fields");
        }
        this.allocator = allocator;
        this.capacity = capacity;
        this.maxHashBuckets = capacity;
        this.blockSize = blockSize;
        this.isBoolean = isBoolean;
    }

    public void setup() {
        // Indexes are int - 4 bytes
        final int indexBufByteSize = 4 * capacity;
        hashBuckets = allocator.buffer(indexBufByteSize);
        closeables.add(hashBuckets);
        hashKeyNextIndexes = allocator.buffer(indexBufByteSize);
        closeables.add(hashKeyNextIndexes);

        // Set no key at all positions.
        for (int i = 0; i < indexBufByteSize; i += 4) {
            hashBuckets.setInt(i, -1);
            hashKeyNextIndexes.setInt(i, -1);
        }

        // Buffer with actual keys
        long sizeReqd = ValueListFilter.META_SIZE + ((isBoolean) ? 0 : (blockSize * capacity));
        final ArrowBuf fullBuffer = allocator.buffer(sizeReqd);
        this.valueListFilter = new ValueListFilter(fullBuffer);
        this.valueListFilter.setBoolField(isBoolean);
        closeables.add(valueListFilter);
        this.valuesList = valueListFilter.valOnlyBuf();
    }

    public boolean insert(final ArrowBuf keyBuf) {
        checkArgument(!isBoolean, "Insertion for boolean should be done via insertTrue() / insertFalse()");
        checkArgument(keyBuf.capacity() == blockSize, "Invalid key size %s. Compatible key size is %s",
                keyBuf.capacity(), blockSize);
        final long hashIndex = hash(keyBuf);
        int keyIndex = hashBuckets.getInt(hashIndex);

        if (keyIndex == -1) {
            // new entry
            final int insertedValIndex = insertNewElement(keyBuf);
            hashBuckets.setInt(hashIndex, insertedValIndex);
            return true;
        }

        byte[] incomingVal = new byte[blockSize];
        keyBuf.getBytes(0, incomingVal);
        byte[] storedVal = new byte[blockSize];
        // Traverse linked list until key is identified as duplicate or the new value is inserted at the tail.
        while (true) {
            valuesList.getBytes((keyIndex * blockSize), storedVal);
            if (Arrays.equals(storedVal, incomingVal)) {
                return false; // duplicate key
            }
            int nextValIndex = hashKeyNextIndexes.getInt(keyIndex * 4);
            if (nextValIndex == -1) {
                // At tail node, this is a distinct new key.
                final int insertedValIndex = insertNewElement(keyBuf);
                hashKeyNextIndexes.setInt(keyIndex * 4, insertedValIndex);
                return true;
            }

            keyIndex = nextValIndex;
        }
    }

    private int insertNewElement(final ArrowBuf keyBuf) {
        checkState(isNotFull(), "Store is full.");
        final int insertionIndex = nextEmptyIndex;
        Copier.copy(keyBuf.memoryAddress(), valuesList.memoryAddress() + (insertionIndex * blockSize), blockSize);
        nextEmptyIndex ++;
        return insertionIndex;
    }

    public void insertNull() {
        this.valueListFilter.setContainsNull(true);
        checkBooleanCombinationsLeft();
    }

    public void insertBooleanVal(final boolean val) {
        checkArgument(isBoolean, "Not a boolean value list.");
        if (val) {
            this.valueListFilter.setContainsTrue(true);
        } else {
            this.valueListFilter.setContainsFalse(true);
        }
        checkBooleanCombinationsLeft();
    }

    private void checkBooleanCombinationsLeft() {
        boolean allBooleanCombinationsPresent = this.valueListFilter.isContainsNull() && this.valueListFilter.isContainsFalse()
                && this.valueListFilter.isContainsTrue() && this.isBoolean;
        checkState(!allBooleanCombinationsPresent, "Filter has all boolean combinations. " +
                "Unlikely to filter any entries.");
    }

    public void setFieldType(Types.MinorType fieldType) {
        setFieldType(fieldType, (byte) 0, (byte) 0);
    }

    public ValueListFilterBuilder setFieldType(Types.MinorType fieldType, byte precision, byte scale) {
        this.valueListFilter.setFieldType(fieldType, precision, scale);
        return this;
    }

    public ValueListFilterBuilder setName(String name) {
        this.valueListFilter.setName(name);
        return this;
    }

    public ValueListFilterBuilder setFieldName(String name) {
        this.valueListFilter.setFieldName(name);
        return this;
    }

    public ValueListFilterBuilder setFixedWidth(boolean isFixedWidth) {
        this.valueListFilter.setFixedWidth(isFixedWidth);
        return this;
    }

    public ValueListFilter build() {
        this.valueListFilter.setBlockSize(this.blockSize);
        this.valueListFilter.setValueCount(this.nextEmptyIndex);

        checkNotNull(this.valueListFilter.getName());
        checkNotNull(this.valueListFilter.getFieldType());

        this.valueListFilter.writeMetaToBuffer();
        sortValList();

        // After building, it is the responsibility of the caller to manage valueListFilter.
        closeables.remove(valueListFilter);
        return this.valueListFilter;
    }

    /**
     * Get quick plain instance for non continuous insertion cases.
     * @param allocator
     * @param blockSize
     * @param maxElements
     * @return
     */
    public static ValueListFilter buildPlainInstance(BufferAllocator allocator, byte blockSize, long maxElements, boolean isBoolean) {
        if (isBoolean) {
            checkArgument(blockSize == 0, "Block size should be zero for boolean fields");
        } else {
            checkArgument(blockSize > 0, "Block size should be greater than zero for non-boolean fields");
        }

        long valueBufferSize = isBoolean ? 0 : (maxElements * blockSize);
        long minRequiredCapacity = valueBufferSize + ValueListFilter.META_SIZE;
        ValueListFilter valueListFilter = new ValueListFilter(allocator.buffer(minRequiredCapacity));
        valueListFilter.setBoolField(isBoolean);
        valueListFilter.setBlockSize(blockSize);
        return valueListFilter;
    }

    public static ValueListFilter fromBuffer(ArrowBuf arrowBuf) {
        ValueListFilter valueListFilter = new ValueListFilter(arrowBuf);
        valueListFilter.initializeMetaFromBuffer();
        return valueListFilter;
    }

    private void sortValList() {
        if (this.valueListFilter.isBoolField()) {
            return;
        }

        final ArrowInPlaceMergeSorter sorter = new ArrowInPlaceMergeSorter(valuesList, blockSize,
                this.valueListFilter.getComparator());
        sorter.sort(0, this.valueListFilter.getValueCount());
    }

    private long hash(ArrowBuf key) {
        return Math.abs((MurmurHash3.murmur3_128(0, key.capacity(), key, 0).getHash1() % maxHashBuckets)) * 4;
    }

    @VisibleForTesting
    void overrideMaxBuckets(int maxHashBuckets) {
        this.maxHashBuckets = maxHashBuckets;
    }

    private boolean isNotFull() {
        return nextEmptyIndex < capacity;
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.close(closeables);
    }
}
