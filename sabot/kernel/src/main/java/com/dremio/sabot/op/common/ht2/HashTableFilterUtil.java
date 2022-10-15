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

import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.physical.config.RuntimeFilterProbeTarget;
import com.dremio.exec.util.BloomFilter;
import com.dremio.exec.util.ValueListFilterBuilder;
import com.google.common.base.Preconditions;

public class HashTableFilterUtil {
  private static Logger logger = LoggerFactory.getLogger(HashTable.class);

  /**
   * Prepares bloomFilter for the given probe target. BloomFilter and corresponding
   * KeyReader for each probe target are passed as parameters. The updated bloomfilter
   * returned on success otherwise null if encountered any error, by closing the input
   * bloomfilter.
   *
   * This function can be called just before spilling the partition as well as at the end
   * of build (right) side data so that bloomfilter's will have the info about all the keys.
   *
   * Since this is an optimisation, errors are not propagated to the consumer. Instead,
   * the corresponding bloom filter is freed and set with an empty Optional.
   */
  public static Optional<BloomFilter> prepareBloomFilters(RuntimeFilterProbeTarget probeTarget,
                                                          Optional<BloomFilter> inputBloomFilter,
                                                          HashTableKeyReader keyReader, HashTable hashTable) {
    Preconditions.checkState(inputBloomFilter.isPresent());
    BloomFilter bloomFilter = inputBloomFilter.get();

    List<String> fieldNames = probeTarget.getPartitionBuildTableKeys();
    Preconditions.checkArgument(!CollectionUtils.isEmpty(fieldNames));

    try (AutoCloseables.RollbackCloseable closeOnError = new AutoCloseables.RollbackCloseable()) {
      /* On error, close the BloomFilter */
      closeOnError.add(bloomFilter);

      Iterator<HashTable.HashTableKeyAddress> hashTableKeyAddressIterator = hashTable.keyIterator();
      Preconditions.checkState(hashTableKeyAddressIterator != null,
        "Failed to create hashtable key iterator");

      final ArrowBuf keyHolder = keyReader.getKeyHolder();

      while (hashTableKeyAddressIterator.hasNext()) {
        HashTable.HashTableKeyAddress hashTableKeyAddress = hashTableKeyAddressIterator.next();
        keyReader.loadNextKey(hashTableKeyAddress.getFixedKeyAddress(), hashTableKeyAddress.getVarKeyAddress());
        bloomFilter.put(keyHolder, keyReader.getKeyBufSize());
      }

      Preconditions.checkState(!bloomFilter.isCrossingMaxFPP(), "Bloomfilter has overflown its capacity.");

      closeOnError.commit();
      return Optional.of(bloomFilter);
    } catch (Exception e) {
      logger.warn("Unable to prepare bloomfilter for " + fieldNames, e);
      return Optional.empty();
    }
  }

  static boolean readBoolean(final ArrowBuf key) {
    // reads the first column
    return (key.getByte(0) & (1L << 1)) != 0;
  }

  /**
   * Prepare ValueListFilters for given probe target (i.e for each field for composite keys).
   * ValueListFilterBuilder and corresponding KeyReader for given probe target were passed as
   * parameters. This function can be called just before spilling the partition as well as at
   * the end of build (right) side data so that valuelistfilter will have the info about all
   * the keys.
   * Since this is an optimisation, errors are not propagated to the consumer. Instead,
   * they marked as an empty optional.
   */
  public static void prepareValueListFilters(List<HashTableKeyReader> keyReaderList,
                                             List<ValueListFilterBuilder> valueListFilterBuilderList,
                                             PivotDef pivot, HashTable hashTable) {
    Preconditions.checkState(keyReaderList.size() == valueListFilterBuilderList.size());

    if (CollectionUtils.isEmpty(keyReaderList)) {
      Preconditions.checkState(CollectionUtils.isEmpty(valueListFilterBuilderList));
      return;
    }

    ListIterator<HashTableKeyReader> keyReaderListIterator = keyReaderList.listIterator();
    ListIterator<ValueListFilterBuilder> valueListFilterBuilderListIterator = valueListFilterBuilderList.listIterator();

    while (keyReaderListIterator.hasNext()) {
      Preconditions.checkState(valueListFilterBuilderListIterator.hasNext());
      HashTableKeyReader keyReader = keyReaderListIterator.next();
      ValueListFilterBuilder valueListFilterBuilder = valueListFilterBuilderListIterator.next();

      try (AutoCloseables.RollbackCloseable closeOnError = new AutoCloseables.RollbackCloseable()) {
        closeOnError.add(keyReader);
        closeOnError.add(valueListFilterBuilder);

        final boolean isBooleanField = pivot.isBoolField(valueListFilterBuilder.getFieldName());

        Iterator<HashTable.HashTableKeyAddress> hashTableKeyAddressIterator = hashTable.keyIterator();
        Preconditions.checkState(hashTableKeyAddressIterator != null,
          "Failed to create hashtable key iterator");

        final ArrowBuf key = keyReader.getKeyValBuf();

        while (hashTableKeyAddressIterator.hasNext()) {
          HashTable.HashTableKeyAddress hashTableKeyAddress = hashTableKeyAddressIterator.next();
          keyReader.loadNextKey(hashTableKeyAddress.getFixedKeyAddress(), hashTableKeyAddress.getVarKeyAddress());

          if (keyReader.areAllValuesNull()) {
            valueListFilterBuilder.insertNull();
          } else if (isBooleanField) {
            valueListFilterBuilder.insertBooleanVal(readBoolean(keyReader.getKeyHolder()));
          } else {
            valueListFilterBuilder.insert(key);
          }
        }
        closeOnError.commit();
      } catch (Exception e) {
        logger.warn("Unable to prepare value list filter for {} because {}", valueListFilterBuilder.getFieldName(), e.getMessage());
        /* Since an error encountered preparing valList this col, remove it from processing */
        keyReaderListIterator.remove();
        valueListFilterBuilderListIterator.remove();
      }
    }
  }
}
