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
package com.dremio.exec.store;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.store.dfs.implicit.ConstantColumnPopulators;
import com.dremio.exec.store.dfs.implicit.NameValuePair;
import com.dremio.exec.util.BloomFilter;
import com.dremio.exec.util.KeyFairSliceCalculator;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.scan.ScanOperator;
import com.google.common.collect.Sets;

/**
 * Helper class for evaluating partition column filter against a split.
 */
public class RuntimeFilterEvaluator {
    private static final Logger logger = LoggerFactory.getLogger(RuntimeFilterEvaluator.class);
    private final BufferAllocator allocator;
    private final RuntimeFilter runtimeFilter;
    private final OperatorStats stats;

    public RuntimeFilterEvaluator(final BufferAllocator allocator,
                                  final OperatorStats stats,
                                  final RuntimeFilter runtimeFilter) {
        this.runtimeFilter = runtimeFilter;
        this.allocator = allocator;
        this.stats = stats;
    }

    public boolean canBeSkipped(final SplitAndPartitionInfo split, final List<NameValuePair<?>> partitionValues) {
        return !mightContain(split, partitionValues);
    }

    private boolean mightContain(final SplitAndPartitionInfo split, final List<NameValuePair<?>> partitionValues) {
        final CompositeColumnFilter partitionColumnFilter = runtimeFilter.getPartitionColumnFilter();
        if (partitionColumnFilter == null
                || CollectionUtils.isEmpty(partitionValues)
                || partitionColumnFilter.getFilterType() != CompositeColumnFilter.RuntimeFilterType.BLOOM_FILTER) {
            return true;
        }

        final List<String> partionColumnsInFilter = partitionColumnFilter.getColumnsList().stream().map(String::toLowerCase).collect(Collectors.toList());
        final Set<String> columnSet = Sets.newHashSet(partionColumnsInFilter);
        final List<NameValuePair<?>> partitionValuesToCheck = partitionValues.stream()
                .filter(pair -> columnSet.contains(pair.getName().toLowerCase()))
                .sorted(Comparator.comparingInt(pair -> partionColumnsInFilter.indexOf(pair.getName().toLowerCase())))
                .collect(Collectors.toList());

        if (partitionValuesToCheck.size()!=partionColumnsInFilter.size()) {
            logger.warn("PartitionColumnList received in runtime filter contains extra columns." +
                            "PartitionColumnList in runtime filter : {} and Actual partitionColumnList : {}. RuntimeFilter sender info : {}",
                    partitionColumnFilter.getColumnsList(), partitionValues.stream().map(NameValuePair::getName).collect(Collectors.toList()),
                    runtimeFilter.getSenderInfo());
            return true;
        }

        try (BloomFilterKeyBuilder keyBuilder = new BloomFilterKeyBuilder(partitionValuesToCheck, allocator)) {
            BloomFilter bloomFilter = partitionColumnFilter.getBloomFilter();
            if (!bloomFilter.mightContain(keyBuilder.getKey(), keyBuilder.getTotalSize())) {
                stats.addLongStat(ScanOperator.Metric.NUM_PARTITIONS_PRUNED, 1);
                logSkippedPartition(split, partitionColumnFilter.getColumnsList());
                return false;
            }
        } catch (Exception ex) {
            logger.warn("Failed to add runtime filter. RuntimeFilter sender info: {}. ", runtimeFilter.getSenderInfo(), ex);
        }

        return true;
    }

    private void logSkippedPartition(final SplitAndPartitionInfo split, final List<String> filterColumns) {
        if (logger.isDebugEnabled()) {
            // Shows up as - Skipping current partition. PartitionInfo: [jc2=14, jc1=jc1v1, jc3=33]. PartitionColumnsList: [jc2, jc1].
            logger.debug("Skipping current partition. PartitionInfo: {}. FilterColumns: {}",
                    Optional.ofNullable(split)
                            .map(p -> p.getPartitionInfo().getValuesList()
                                    .stream()
                                    .map(v -> v.getAllFields().entrySet()
                                            .stream()
                                            .filter(vv -> !vv.getKey().getName().equalsIgnoreCase("type"))
                                            .map(vv -> vv.getValue().toString())
                                            .collect(Collectors.joining("=")))
                                    .collect(Collectors.joining(", ", "[", "]")))
                            .orElse("<Not available>"),
                    filterColumns);
        }
    }

    /**
     * BloomFilterKeyBuilder create a key for bloom filter from partition values
     * The key should be created with the same logic as in the join side.
     */
    private static class BloomFilterKeyBuilder implements AutoCloseable {
        private static final int MAX_KEY_SIZE = 32;

        private ArrowBuf keyBuf;
        private int totalSize;

        private BloomFilterKeyBuilder(List<NameValuePair<?>> nameValuePairs, BufferAllocator allocator) {
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
