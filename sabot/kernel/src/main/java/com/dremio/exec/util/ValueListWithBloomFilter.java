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

import static com.google.common.base.Preconditions.checkArgument;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.util.MajorTypeHelper;
import org.apache.arrow.memory.ArrowBuf;

public class ValueListWithBloomFilter extends ValueListFilter {
  protected ArrowBuf bloomFilterSlice;

  public ValueListWithBloomFilter(ArrowBuf fullBuffer) {
    super(fullBuffer);
    checkArgument(fullBuffer.capacity() >= META_SIZE + ValueListFilter.BLOOM_FILTER_SIZE);

    // Override the value list slice
    this.valueListSlice =
        fullBuffer.slice(
            META_SIZE + BLOOM_FILTER_SIZE, fullBuffer.capacity() - META_SIZE - BLOOM_FILTER_SIZE);
    this.bloomFilterSlice = fullBuffer.slice(META_SIZE, BLOOM_FILTER_SIZE);
  }

  @Override
  public void initializeMetaFromBuffer() {
    super.initializeMetaFromBuffer();
    fullBuffer.writerIndex(META_SIZE + BLOOM_FILTER_SIZE + (blockSize * valueCount));
  }

  @Override
  public boolean mightBePresent(int key) {
    int hash = hashInt(key);
    return getBit(hash);
  }

  @Override
  public boolean mightBePresent(long val) {
    int hash = hashInt((int) (val ^ val >>> 32));
    return getBit(hash);
  }

  @Override
  public void buildBloomFilter() {
    long elements = 0, buffIndex = 0;
    Object value;

    CompleteType type =
        CompleteType.fromMinorType(MajorTypeHelper.getMinorTypeFromArrowMinorType(getFieldType()));

    while (elements < valueCount) {
      switch (type.toMinorType()) {
        case DATE:
        case TIMESTAMP:
        case BIGINT:
          value = valueListSlice.getLong(buffIndex);
          insertIntoBloomFilter((long) value);
          buffIndex = buffIndex + 8;
          break;
        case TIME:
        case INT:
          value = valueListSlice.getInt(buffIndex);
          insertIntoBloomFilter((int) value);
          buffIndex = buffIndex + 4;
          break;
      }
      elements++;
    }
  }

  private void insertIntoBloomFilter(long val) {
    int hashValue = hashInt((int) (val ^ val >>> 32));
    setBit(hashValue);
  }

  private void insertIntoBloomFilter(int val) {
    int hashValue = hashInt(val);
    setBit(hashValue);
  }

  private int hashInt(int val) {
    return val ^ (val >>> 16) & 0xFFFF;
  }

  private void setBit(int pos) {
    pos = Math.abs(pos);
    pos = pos % (BLOOM_FILTER_SIZE * 8);
    long bytePos = pos / 8;
    long bitOffset = pos % 8;
    byte byteAtPos = bloomFilterSlice.getByte(bytePos);

    byteAtPos |= 1 << bitOffset;
    bloomFilterSlice.writerIndex(bytePos).writeByte(byteAtPos);
  }

  private boolean getBit(long pos) {
    pos = Math.abs(pos);
    pos = pos % (BLOOM_FILTER_SIZE * 8);
    long bytePos = pos / 8;
    long bitOffset = pos % 8;

    byte byteAtPos = bloomFilterSlice.getByte(bytePos);
    return ((byteAtPos >> bitOffset) & 1) == 1;
  }
}
