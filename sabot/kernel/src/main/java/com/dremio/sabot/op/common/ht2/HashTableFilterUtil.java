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
package com.dremio.sabot.op.common.ht2;

import java.util.List;
import java.util.Optional;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.dremio.common.AutoCloseables;
import com.dremio.common.util.CloseableIterator;
import com.dremio.exec.util.BloomFilter;
import com.dremio.exec.util.ValueListFilter;
import com.dremio.exec.util.ValueListFilterBuilder;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/** Creation of bloom filter and value list filter like functions for the HashTable.*/
public class HashTableFilterUtil {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashTableFilterUtil.class);
  static final long BLOOMFILTER_MAX_SIZE = 2 * 1024 * 1024;
  static final int MAX_VAL_LIST_FILTER_KEY_SIZE = 17;

  private static HashTableKeyReader.Builder getKeyReaderBuilder(List<String> fieldNames, BufferAllocator allocator, PivotDef pivot) {
    return new HashTableKeyReader.Builder()
      .setBufferAllocator(allocator)
      .setFieldsToRead(fieldNames)
      .setPivot(pivot);
  }

  /**
   * Prepares a bloomfilter from the selective field keys. Since this is an optimisation, errors are not propagated to
   * the consumer. Instead, they get an empty optional.
   * @param fieldNames
   * @param sizeDynamically Size the filter according to the number of entries in table.
   * @return
   */
  public static Optional<BloomFilter> prepareBloomFilter(HashTable hashTable, BufferAllocator allocator, PivotDef pivot,
                                                         List<String> fieldNames, boolean sizeDynamically, int maxKeySize) {
    if (CollectionUtils.isEmpty(fieldNames)) {
      return Optional.empty();
    }

    final int hashTableSize = hashTable.size();
    // Not dropping the filter even if expected size is more than max possible size since there could be repeated keys.
    long bloomFilterSize = sizeDynamically ?
      Math.min(BloomFilter.getOptimalSize(hashTableSize), BLOOMFILTER_MAX_SIZE) : BLOOMFILTER_MAX_SIZE;

    final BloomFilter bloomFilter = new BloomFilter(allocator, Thread.currentThread().getName(), bloomFilterSize);
    try (AutoCloseables.RollbackCloseable closeOnError = new AutoCloseables.RollbackCloseable();
         CloseableIterator<HashTable.HashTableKeyAddress> hashTableKeyAddressIterator = hashTable.keyIterator();
         HashTableKeyReader keyReader = getKeyReaderBuilder(fieldNames, allocator, pivot).setMaxKeySize(maxKeySize).build()) {
      closeOnError.add(bloomFilter);
      bloomFilter.setup();

      final ArrowBuf keyHolder = keyReader.getKeyHolder();

      while (hashTableKeyAddressIterator.hasNext()) {
        HashTable.HashTableKeyAddress hashTableKeyAddress = hashTableKeyAddressIterator.next();
        keyReader.loadNextKey(hashTableKeyAddress.getFixedKeyAddress(), hashTableKeyAddress.getVarKeyAddress());
        bloomFilter.put(keyHolder, keyReader.getKeyBufSize());
      }

      Preconditions.checkState(!bloomFilter.isCrossingMaxFPP(), "Bloom filter overflown over its capacity.");
      closeOnError.commit();
      return Optional.of(bloomFilter);
    } catch (Exception e) {
      logger.warn("Unable to prepare bloomfilter for " + fieldNames, e);
      return Optional.empty();
    }
  }

  private static boolean isBoolField(PivotDef pivot, final String fieldName) {
    return pivot.getBitPivots().stream().anyMatch(p -> p.getIncomingVector().getField().getName()
      .equalsIgnoreCase(fieldName));
  }

  private static ArrowType getFieldType(final List<VectorPivotDef> vectorPivotDefs, final String fieldName) {
    return vectorPivotDefs.stream()
      .map(p -> p.getIncomingVector().getField())
      .filter(f -> f.getName().equalsIgnoreCase(fieldName))
      .map(f -> f.getType())
      .findAny().orElse(null);
  }

  private static void setFieldType(PivotDef pivot, final ValueListFilterBuilder filterBuilder, final String fieldName) {
    // Check fixed and variable width vectorPivotDefs one by one
    ArrowType fieldType = getFieldType(pivot.getFixedPivots(), fieldName);
    byte precision = 0;
    byte scale = 0;
    if (fieldType == null) {
      fieldType = getFieldType(pivot.getVariablePivots(), fieldName);
      filterBuilder.setFixedWidth(false);
    }
    Preconditions.checkNotNull(fieldType, "Not able to find %s in build pivot", fieldName);
    if (fieldType instanceof ArrowType.Decimal) {
      precision = (byte) ((ArrowType.Decimal) fieldType).getPrecision();
      scale = (byte) ((ArrowType.Decimal) fieldType).getScale();
    }

    filterBuilder.setFieldType(Types.getMinorTypeForArrowType(fieldType), precision, scale);
  }

  private static boolean readBoolean(final ArrowBuf key) {
    // reads the first column
    return (key.getByte(0) & (1L << 1)) != 0;
  }

  public static Optional<ValueListFilter> prepareValueListFilter(HashTable hashTable, BufferAllocator allocator, PivotDef pivot,
                                                                 String fieldName, int maxElements) {
    if (StringUtils.isEmpty(fieldName)) {
      return Optional.empty();
    }

    final boolean isBooleanField = isBoolField(pivot, fieldName);

    try (final HashTableKeyReader keyReader = getKeyReaderBuilder(ImmutableList.of(fieldName), allocator, pivot)
        .setSetVarFieldLenInFirstByte(true) // Set length at first byte, to avoid comparison issues for different size values.
        .setMaxKeySize(MAX_VAL_LIST_FILTER_KEY_SIZE).build(); // Max key size 16, excluding one byte for validity bits.
         CloseableIterator<HashTable.HashTableKeyAddress> hashTableKeyAddressIterator = hashTable.keyIterator();
         ValueListFilterBuilder filterBuilder = new ValueListFilterBuilder(allocator, maxElements,
           isBooleanField ? 0 : keyReader.getEffectiveKeySize(), isBooleanField)) {
      filterBuilder.setup();
      filterBuilder.setFieldName(fieldName);
      filterBuilder.setName(Thread.currentThread().getName());
      setFieldType(pivot, filterBuilder, fieldName);

      final ArrowBuf key = keyReader.getKeyValBuf();

      while (hashTableKeyAddressIterator.hasNext()) {
        HashTable.HashTableKeyAddress hashTableKeyAddress = hashTableKeyAddressIterator.next();
        keyReader.loadNextKey(hashTableKeyAddress.getFixedKeyAddress(), hashTableKeyAddress.getVarKeyAddress());

        if (keyReader.areAllValuesNull()) {
          filterBuilder.insertNull();
        } else if (isBooleanField) {
          filterBuilder.insertBooleanVal(readBoolean(keyReader.getKeyHolder()));
        } else {
          filterBuilder.insert(key);
        }
      }
      return Optional.of(filterBuilder.build());
    } catch (Exception e) {
      logger.info("Unable to prepare value list filter for {} because {}", fieldName, e.getMessage());
      return Optional.empty();
    }
  }
}
