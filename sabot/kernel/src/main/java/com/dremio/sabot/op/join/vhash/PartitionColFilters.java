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

package com.dremio.sabot.op.join.vhash;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.physical.config.RuntimeFilterProbeTarget;
import com.dremio.exec.util.BloomFilter;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.HashTable;
import com.dremio.sabot.op.common.ht2.HashTableFilterUtil;
import com.dremio.sabot.op.common.ht2.HashTableKeyReader;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;
import com.dremio.sabot.op.join.vhash.spill.partition.DiskPartitionFilterHelper;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.collections4.CollectionUtils;

public class PartitionColFilters implements AutoCloseable {
  private final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(PartitionColFilters.class);
  public static final long BLOOMFILTER_MAX_SIZE = 2 * 1024 * 1024;

  private final List<PartitionColFilter> partitionColFilters;

  private final BufferAllocator allocator;
  private final List<RuntimeFilterProbeTarget> probeTargets;
  private final PivotDef pivotDef;
  private final long bloomFilterSize;
  private final int maxKeySize;

  public PartitionColFilters(
      BufferAllocator allocator,
      List<RuntimeFilterProbeTarget> probeTargets,
      PivotDef pivotDef,
      long bloomFilterSize,
      int maxKeySize) {
    this.allocator = allocator.newChildAllocator("partition-col-filters", 0, allocator.getLimit());
    this.probeTargets = probeTargets;
    this.pivotDef = pivotDef;
    this.bloomFilterSize = bloomFilterSize;
    this.maxKeySize = maxKeySize;
    this.partitionColFilters = build();
  }

  private List<PartitionColFilter> build() {
    final List<PartitionColFilter> partitionColFilters = new ArrayList<>();

    for (int i = 0; i < probeTargets.size(); i++) {
      RuntimeFilterProbeTarget probeTarget = probeTargets.get(i);

      if (CollectionUtils.isEmpty(probeTarget.getPartitionBuildTableKeys())) {
        PartitionColFilter partitionColFilter =
            new PartitionColFilter(probeTarget, Optional.empty(), null);
        partitionColFilters.add(partitionColFilter);
        logger.warn("Ignoring empty partition col filter(" + i + ")");
        continue;
      }

      final BloomFilter bloomFilter =
          new BloomFilter(allocator, Thread.currentThread().getName(), bloomFilterSize);
      HashTableKeyReader.Builder keyReaderBuilder =
          new HashTableKeyReader.Builder()
              .setBufferAllocator(allocator)
              .setFieldsToRead(probeTarget.getPartitionBuildTableKeys())
              .setPivot(pivotDef)
              .setMaxKeySize(maxKeySize);

      try (AutoCloseables.RollbackCloseable closeOnError = new AutoCloseables.RollbackCloseable()) {
        bloomFilter.setup();
        closeOnError.add(bloomFilter);
        HashTableKeyReader keyReader = keyReaderBuilder.build();
        closeOnError.add(keyReader);

        closeOnError.commit();

        PartitionColFilter partitionColFilter =
            new PartitionColFilter(probeTarget, Optional.of(bloomFilter), keyReader);
        partitionColFilters.add(partitionColFilter);
      } catch (Exception e) {
        logger.warn(
            "Unable to setup bloomfilter for " + probeTarget.getPartitionBuildTableKeys(), e);
        PartitionColFilter partitionColFilter =
            new PartitionColFilter(probeTarget, Optional.empty(), null);
        partitionColFilters.add(partitionColFilter);
      }
    }
    Preconditions.checkState(partitionColFilters.size() == probeTargets.size());
    return partitionColFilters;
  }

  // For in-memory partition
  public void prepareBloomFilters(HashTable hashTable) {
    for (int i = 0; i < partitionColFilters.size(); i++) {
      PartitionColFilter partitionColFilter = partitionColFilters.get(i);
      Optional<BloomFilter> bloomFilter = partitionColFilter.getBloomFilter();
      if (!bloomFilter.isPresent()) {
        continue;
      }

      RuntimeFilterProbeTarget probeTarget = partitionColFilter.getProbeTarget();
      HashTableKeyReader hashTableKeyReader = partitionColFilter.getHashTableKeyReader();

      bloomFilter =
          HashTableFilterUtil.prepareBloomFilters(
              probeTarget, bloomFilter, hashTableKeyReader, hashTable);
      partitionColFilter.setBloomFilter(bloomFilter);
    }
  }

  // For disk partition
  public void prepareBloomFilters(
      FixedBlockVector pivotedFixedBlockVector,
      VariableBlockVector pivotedVariableBlockVector,
      int pivotShift,
      int records,
      ArrowBuf sv2) {
    for (int i = 0; i < partitionColFilters.size(); i++) {
      PartitionColFilter partitionColFilter = partitionColFilters.get(i);
      Optional<BloomFilter> bloomFilter = partitionColFilter.getBloomFilter();
      if (!bloomFilter.isPresent()) {
        continue;
      }

      RuntimeFilterProbeTarget probeTarget = partitionColFilter.getProbeTarget();
      HashTableKeyReader hashTableKeyReader = partitionColFilter.getHashTableKeyReader();

      bloomFilter =
          DiskPartitionFilterHelper.prepareBloomFilters(
              probeTarget,
              bloomFilter,
              hashTableKeyReader,
              pivotedFixedBlockVector,
              pivotedVariableBlockVector,
              pivotShift,
              records,
              sv2);
      partitionColFilter.setBloomFilter(bloomFilter);
    }
  }

  public List<RuntimeFilterProbeTarget> getProbeTargets() {
    return probeTargets;
  }

  public Optional<BloomFilter> getBloomFilter(int index, RuntimeFilterProbeTarget probeTarget) {
    PartitionColFilter partitionColFilter = partitionColFilters.get(index);
    Preconditions.checkState(partitionColFilter.getProbeTarget() == probeTarget);
    return partitionColFilter.getBloomFilter();
  }

  public void setBloomFilter(
      int index, RuntimeFilterProbeTarget probeTarget, Optional<BloomFilter> bloomFilter) {
    PartitionColFilter partitionColFilter = partitionColFilters.get(index);
    Preconditions.checkState(partitionColFilter.getProbeTarget() == probeTarget);
    partitionColFilter.setBloomFilter(bloomFilter);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(partitionColFilters);
    AutoCloseables.close(allocator);
  }

  private class PartitionColFilter implements AutoCloseable {
    private final RuntimeFilterProbeTarget probeTarget;
    private Optional<BloomFilter> bloomFilter;
    private HashTableKeyReader hashTableKeyReader;

    PartitionColFilter(
        RuntimeFilterProbeTarget probeTarget,
        Optional<BloomFilter> bloomFilter,
        HashTableKeyReader hashTableKeyReader) {
      this.probeTarget = probeTarget;
      this.bloomFilter = bloomFilter;
      this.hashTableKeyReader = hashTableKeyReader;
    }

    public Optional<BloomFilter> getBloomFilter() {
      return bloomFilter;
    }

    public void setBloomFilter(Optional<BloomFilter> bloomFilter) {
      /* No need to close/free old bloomfilter as caller will close, if needed */
      this.bloomFilter = bloomFilter;
    }

    public HashTableKeyReader getHashTableKeyReader() {
      return hashTableKeyReader;
    }

    public RuntimeFilterProbeTarget getProbeTarget() {
      return probeTarget;
    }

    @Override
    public void close() throws Exception {
      AutoCloseables.closeNoChecked(bloomFilter.orElse(null));
      AutoCloseables.closeNoChecked(hashTableKeyReader);
    }
  }
}
