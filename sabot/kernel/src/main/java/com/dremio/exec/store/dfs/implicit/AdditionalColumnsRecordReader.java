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
package com.dremio.exec.store.dfs.implicit;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.ValueVector;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.store.CompositeColumnFilter;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RuntimeFilter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.util.BloomFilter;
import com.dremio.exec.util.KeyFairSliceCalculator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class AdditionalColumnsRecordReader implements RecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AdditionalColumnsRecordReader.class);

  private final OperatorContext context;
  private final RecordReader inner;
  private final List<NameValuePair<?>> nameValuePairs;
  private final Populator[] populators;
  private final BufferAllocator allocator;
  private final SplitAndPartitionInfo splitAndPartitionInfo;

  private boolean skipPartition;

  public AdditionalColumnsRecordReader(OperatorContext context, RecordReader inner, List<NameValuePair<?>> pairs, BufferAllocator allocator) {
    this(context, inner, pairs, allocator, null);
  }
  public AdditionalColumnsRecordReader(OperatorContext context, RecordReader inner, List<NameValuePair<?>> pairs, BufferAllocator allocator, SplitAndPartitionInfo splitAndPartitionInfo) {
    super();
    this.context = context;
    this.inner = inner;
    this.nameValuePairs = pairs;
    this.allocator = allocator;
    this.splitAndPartitionInfo = splitAndPartitionInfo;
    this.populators = new Populator[pairs.size()];
    for(int i = 0; i < pairs.size(); i++){
      populators[i] = pairs.get(i).createPopulator();
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(Iterables.concat(Arrays.asList(populators), Collections.singleton(inner)));
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    inner.setup(output);
    for(Populator p : populators){
      p.setup(output);
    }
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    inner.allocate(vectorMap);
    for(Populator p : populators){
      p.allocate();
    }
  }

  @Override
  public int next() {
     if (skipPartition) {
      return 0;
     }

    final int count = inner.next();
    try {
      for (Populator p : populators) {
        p.populate(count);
      }
    } catch (Throwable t) {
      throw userExceptionWithDiagnosticInfo(t, count);
    }
    return count;
  }

  @Override
  public void addRuntimeFilter(RuntimeFilter runtimeFilter) {
    if (skipPartition) {
      return;
    }
    CompositeColumnFilter partitionColumnFilter = runtimeFilter.getPartitionColumnFilter();
    if (partitionColumnFilter != null && partitionColumnFilter.getFilterType() == CompositeColumnFilter.RuntimeFilterType.BLOOM_FILTER) {
      List<String> partionColumnsInFilter = partitionColumnFilter.getColumnsList().stream().map(String::toLowerCase).collect(Collectors.toList());
      Set<String> columnSet = Sets.newHashSet(partionColumnsInFilter);
      List<NameValuePair<?>> partitionValuesToCheck = nameValuePairs.stream()
        .filter(pair -> columnSet.contains(pair.getName().toLowerCase()))
        .sorted(Comparator.comparingInt(pair -> partionColumnsInFilter.indexOf(pair.getName().toLowerCase())))
        .collect(Collectors.toList());

      if (partitionValuesToCheck.size() != partionColumnsInFilter.size()) {
        logger.warn("PartitionColumnList received in runtime filter contains extra columns." +
          "PartitionColumnList in runtime filter : {} and Actual partitionColumnList : {}. RuntimeFilter sender info : {}",
          partitionColumnFilter.getColumnsList(), nameValuePairs.stream().map(NameValuePair::getName).collect(Collectors.toList()),
          runtimeFilter.getSenderInfo());
        return;
      }

      try (BloomFilterKeyBuilder keyBuilder = new BloomFilterKeyBuilder(partitionValuesToCheck, allocator)) {
        BloomFilter bloomFilter = partitionColumnFilter.getBloomFilter();
        if (!bloomFilter.mightContain(keyBuilder.getKey(), keyBuilder.getTotalSize())) {
          skipPartition = true;
          context.getStats().addLongStat(ScanOperator.Metric.NUM_PARTITIONS_PRUNED, 1);
          logger.debug("Skipping current partition. PartitionInfo: {}. PartitionColumnsList: {}",
            Optional.ofNullable(splitAndPartitionInfo).map(p -> p.getPartitionInfo().toString()).orElse("<Not available>"),
            nameValuePairs.stream().map(NameValuePair::getName).collect(Collectors.toList()));
          return;
        }
      } catch (Exception ex) {
        logger.warn("Failed to add runtime filter. RuntimeFilter sender info: {}. ", runtimeFilter.getSenderInfo(), ex);
      }
    }
  }

  @VisibleForTesting
  boolean skipPartition() {
    return skipPartition;
  }

  @Override
  public List<SchemaPath> getColumnsToBoost() {
    return inner.getColumnsToBoost();
  }

  private UserException userExceptionWithDiagnosticInfo(final Throwable t, final int count) {
    return UserException.dataReadError(t)
        .message("Failed to populate partition column values")
        .addContext("Partition value characteristics", populators != null ? Joiner.on(",").join(populators) : "null")
        .addContext("Number of rows trying to populate", count)
        .build(logger);
  }

  public interface Populator extends AutoCloseable {
    void setup(OutputMutator output);
    void populate(final int count);
    void allocate();
  }

  /**
   * BloomFilterKeyBuilder create a key for bloom filter from partition values
   * The key should be created with the same logic as in the join side.
   */
  private static class BloomFilterKeyBuilder implements AutoCloseable {
    private static final int MAX_KEY_SIZE = 32;

    private ArrowBuf keyBuf;
    private int totalSize;

    public BloomFilterKeyBuilder(List<NameValuePair<?>> nameValuePairs, BufferAllocator allocator) {
      Map<String, Integer> keySizes = nameValuePairs.stream().collect(Collectors.toMap(NameValuePair::getName, NameValuePair::getValueTypeSize));
      final KeyFairSliceCalculator keyFairSliceCalculator = new KeyFairSliceCalculator(keySizes, MAX_KEY_SIZE);
      keySizes = nameValuePairs.stream().collect(Collectors.toMap(NameValuePair::getName, pair -> keyFairSliceCalculator.getKeySlice(pair.getName())));
      this.totalSize = keyFairSliceCalculator.getTotalSize();

      try (AutoCloseables.RollbackCloseable rollbackCloseable = new AutoCloseables.RollbackCloseable()) {
        ArrowBuf keyBuf = rollbackCloseable.add(allocator.buffer(totalSize));
        keyBuf.setBytes(0, new byte[totalSize]);

        int offset = keyFairSliceCalculator.numValidityBytes();
        int validityBitOffset = 0;
        int validityByteOffset = 0;
        for (NameValuePair<?> pair : nameValuePairs) {
          int len = keySizes.get(pair.getName());
          if (pair.getValue() != null) {
            keyBuf.setByte(validityByteOffset, (byte) (keyBuf.getByte(validityByteOffset) | (1 << validityBitOffset)));
            if (pair instanceof ConstantColumnPopulators.BitNameValuePair) {
              validityByteOffset += (validityBitOffset + 1) / Byte.SIZE;
              validityBitOffset = (validityBitOffset + 1) % Byte.SIZE;
              keyBuf.setByte(validityByteOffset, (byte) (keyBuf.getByte(validityByteOffset) | (((ConstantColumnPopulators.BitNameValuePair) pair).getValue() ? 1 : 0) << validityBitOffset));
            } else {
              byte[] valueBytes = pair.getValueBytes();
              int copySize = Math.min(len, valueBytes.length);
              keyBuf.setBytes(offset + len - copySize, valueBytes, 0, copySize);
            }
          }
          offset += len;
          int advanceBit = (pair.getValue() == null && pair instanceof ConstantColumnPopulators.BitNameValuePair) ? 2 : 1;
          validityByteOffset += (validityBitOffset + advanceBit) / Byte.SIZE;
          validityBitOffset = (validityBitOffset + advanceBit) % Byte.SIZE;
        }
        this.keyBuf = keyBuf;
        rollbackCloseable.commit();
      } catch (Exception ex) {
        throw new RuntimeException("Failed to create key for BloomFilter", ex);
      }
    }

    public ArrowBuf getKey() {
      return keyBuf;
    }

    public int getTotalSize() {
      return totalSize;
    }

    @Override
    public void close() throws Exception {
      keyBuf.close();
    }
  }
}
