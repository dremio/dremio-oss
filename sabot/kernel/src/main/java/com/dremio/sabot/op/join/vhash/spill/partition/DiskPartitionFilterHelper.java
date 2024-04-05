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
package com.dremio.sabot.op.join.vhash.spill.partition;

import static com.dremio.sabot.op.common.ht2.LBlockHashTable.VAR_OFFSET_SIZE;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.physical.config.RuntimeFilterProbeTarget;
import com.dremio.exec.util.BloomFilter;
import com.dremio.exec.util.ValueListFilterBuilder;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.HashTableKeyReader;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;
import com.dremio.sabot.op.join.vhash.spill.SV2UnsignedUtil;
import com.google.common.base.Preconditions;
import io.netty.util.internal.PlatformDependent;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiskPartitionFilterHelper {
  private static final Logger logger = LoggerFactory.getLogger(DiskPartitionFilterHelper.class);

  public static Optional<BloomFilter> prepareBloomFilters(
      RuntimeFilterProbeTarget probeTarget,
      Optional<BloomFilter> inputBloomFilter,
      HashTableKeyReader keyReader,
      FixedBlockVector fixed,
      VariableBlockVector var,
      int pivotShift,
      int records,
      ArrowBuf sv2) {
    Preconditions.checkState(inputBloomFilter.isPresent());
    BloomFilter bloomFilter = inputBloomFilter.get();

    List<String> fieldNames = probeTarget.getPartitionBuildTableKeys();
    Preconditions.checkArgument(!CollectionUtils.isEmpty(fieldNames));

    try (AutoCloseables.RollbackCloseable closeOnError = new AutoCloseables.RollbackCloseable()) {
      /* On error, close the BloomFilter */
      closeOnError.add(bloomFilter);

      final ArrowBuf keyHolder = keyReader.getKeyHolder();
      iterateOverRecords(
          fixed,
          var,
          pivotShift,
          records,
          sv2,
          (keyFixedAddr, keyVarAddr) -> {
            keyReader.loadNextKey(keyFixedAddr, keyVarAddr);
            bloomFilter.put(keyHolder, keyReader.getKeyBufSize());
          });
      Preconditions.checkState(
          !bloomFilter.isCrossingMaxFPP(), "Bloomfilter has overflown its capacity.");
      closeOnError.commit();
      return Optional.of(bloomFilter);
    } catch (Exception e) {
      logger.warn("Unable to prepare bloomfilter for " + fieldNames, e);
      return Optional.empty();
    }
  }

  public static void prepareValueListFilters(
      List<HashTableKeyReader> keyReaderList,
      List<ValueListFilterBuilder> valueListFilterBuilderList,
      PivotDef pivot,
      FixedBlockVector fixed,
      VariableBlockVector var,
      int pivotShift,
      int records,
      ArrowBuf sv2) {
    Preconditions.checkState(keyReaderList.size() == valueListFilterBuilderList.size());

    if (CollectionUtils.isEmpty(keyReaderList)) {
      Preconditions.checkState(CollectionUtils.isEmpty(valueListFilterBuilderList));
      return;
    }
    // same logic like for adding values to hashTable
    ListIterator<HashTableKeyReader> keyReaderListIterator = keyReaderList.listIterator();
    ListIterator<ValueListFilterBuilder> valueListFilterBuilderListIterator =
        valueListFilterBuilderList.listIterator();

    while (keyReaderListIterator.hasNext()) {
      Preconditions.checkState(valueListFilterBuilderListIterator.hasNext());
      HashTableKeyReader keyReader = keyReaderListIterator.next();
      ValueListFilterBuilder valueListFilterBuilder = valueListFilterBuilderListIterator.next();

      try (AutoCloseables.RollbackCloseable closeOnError = new AutoCloseables.RollbackCloseable()) {
        closeOnError.add(keyReader);
        closeOnError.add(valueListFilterBuilder);
        final boolean isBooleanField = pivot.isBoolField(valueListFilterBuilder.getFieldName());
        final ArrowBuf key = keyReader.getKeyValBuf();
        iterateOverRecords(
            fixed,
            var,
            pivotShift,
            records,
            sv2,
            (keyFixedAddr, keyVarAddr) -> {
              keyReader.loadNextKey(keyFixedAddr, keyVarAddr);
              if (keyReader.areAllValuesNull()) {
                valueListFilterBuilder.insertNull();
              } else if (isBooleanField) {
                valueListFilterBuilder.insertBooleanVal(readBoolean(keyReader.getKeyHolder()));
              } else {
                valueListFilterBuilder.insert(key);
              }
            });
        closeOnError.commit();
      } catch (Exception e) {
        logger.warn(
            "Unable to prepare value list filter for {} because {}",
            valueListFilterBuilder.getFieldName(),
            e.getMessage());
        /* Since an error encountered preparing valList this col, remove it from processing */
        keyReaderListIterator.remove();
        valueListFilterBuilderListIterator.remove();
      }
    }
  }

  private static boolean readBoolean(final ArrowBuf key) {
    // reads the first column
    return (key.getByte(0) & (1L << 1)) != 0;
  }

  private static void iterateOverRecords(
      FixedBlockVector fixed,
      VariableBlockVector var,
      int pivotShift,
      int records,
      ArrowBuf sv2,
      BiConsumer<Long, Long> consumer) {
    ArrowBuf keyFixed = fixed.getBuf();
    ArrowBuf keyVar = var == null ? null : var.getBuf();
    final long keyFixedVectorAddr = keyFixed.memoryAddress();
    final long keyVarVectorAddr = keyVar == null ? 0 : keyVar.memoryAddress();
    final int blockWidth = fixed.getBlockWidth();
    for (int index = 0; index < records; index++) {
      final int keyIndex = SV2UnsignedUtil.readAtIndex(sv2, index) - pivotShift;
      assert keyIndex >= 0;
      final long keyFixedAddr = keyFixedVectorAddr + (blockWidth * keyIndex);
      final long keyVarAddr;
      if (keyVar != null) {
        final int keyVarOffset =
            PlatformDependent.getInt(keyFixedAddr + blockWidth - VAR_OFFSET_SIZE);
        keyVarAddr = keyVarVectorAddr + keyVarOffset;
      } else {
        keyVarAddr = -1;
      }
      consumer.accept(keyFixedAddr, keyVarAddr);
    }
  }
}
