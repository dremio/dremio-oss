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
import static com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.ExecProtos.CompositeColumnFilter;
import com.dremio.exec.proto.ExecProtos.RuntimeFilter;
import com.dremio.exec.proto.ExecProtos.RuntimeFilterType;


/**
 * Intended to serve as a temporary cache for the runtime filters until assembly is completed from all minor fragments
 * in shuffle join case.
 */
@NotThreadSafe
public class RuntimeFilterManager implements AutoCloseable {
    private static Logger logger = LoggerFactory.getLogger(RuntimeFilterManager.class);
    private List<RuntimeFilterManagerEntry> filterEntries = new ArrayList<>();
    private final Set<Integer> allMinorFragments;
    private long filterDropCount = 0L;
    private long subFilterDropCount = 0L;
    private final BufferAllocator allocator;
    private final int valFilterMaxSize;

    public RuntimeFilterManager(final BufferAllocator allocator, final int valFilterMaxSize, final Set<Integer> allMinorFragments) {
      this.allMinorFragments = allMinorFragments;
      this.allocator = allocator.newChildAllocator("runtimefilter-manager", 0, allocator.getLimit());
      this.valFilterMaxSize = valFilterMaxSize;
    }

    /**
     * Adds the filter to the manager entries if not present; merges the filter if already exists.
     *
     * @param filter
     * @param partitionColFilter
     * @throws Exception
     */
    public RuntimeFilterManagerEntry coalesce(RuntimeFilter filter, Optional<BloomFilter> partitionColFilter,
                                              List<ValueListFilter> nonPartitionColFilters, int minorFragmentId) {
      Optional<RuntimeFilterManagerEntry> filterEntry = filterEntries.stream().filter(f -> f.isTargetedToSameScan(filter)).findAny();
      if (filterEntry.isPresent()) {
        return merge(filter, partitionColFilter, nonPartitionColFilters,
          filterEntry.get(), minorFragmentId);
      } else {
        // Make a copy and refer it as base to merge into.
        final Optional<BloomFilter> partitionColFilterCopy = partitionColFilter.map(f -> f.createCopy(allocator));
        final List<ValueListFilter> nonPartitionColFiltersCopy = nonPartitionColFilters.stream()
          .map(v -> v.createCopy(allocator)).collect(Collectors.toList());
        final RuntimeFilterManagerEntry newEntry = new RuntimeFilterManagerEntry(filter, allMinorFragments,
          partitionColFilterCopy, nonPartitionColFiltersCopy);
        newEntry.remainingMinorFragments.remove(minorFragmentId);
        logger.debug("New filter entry created. remaining fragments {}", newEntry.remainingMinorFragments);
        filterEntries.add(newEntry);
        return newEntry;
      }
    }

    /**
     * Merges the filter piece with the respective entry present in the RuntimeFilterManagerEntry. The matching is done
     * on the basis of probe scan coordinates (operator id, major fragment id).
     *
     * @param filterPiece
     * @param partitionColFilter
     * @param minorFragmentId
     */
    private RuntimeFilterManagerEntry merge(RuntimeFilter filterPiece,
                                            Optional<BloomFilter> partitionColFilter,
                                            List<ValueListFilter> nonPartitionColFilters,
                                            RuntimeFilterManagerEntry baseEntry,
                                            int minorFragmentId) {
      try {
        checkArgument(baseEntry.getRemainingMinorFragments().contains(minorFragmentId),
          "Not expecting filter piece from " + minorFragmentId +
            ", remaining minor fragments: " + baseEntry.getRemainingMinorFragments());
        baseEntry.remainingMinorFragments.remove(minorFragmentId);

        if (baseEntry.isDropped()) {
          logger.info("Skipping merge of filter piece from {} in {}", minorFragmentId, toCoordinates(filterPiece));
          return baseEntry;
        }

        logger.debug("Merging incoming filter from minor fragment {}, targeted to {}. Remaining fragments {}",
          minorFragmentId, baseEntry.getProbeScanCoordinates(), baseEntry.getRemainingMinorFragments());
        baseEntry.merge(partitionColFilter);
        baseEntry.mergeAll(nonPartitionColFilters);
        baseEntry.resetValueCounts();
        return baseEntry;
      } catch (Exception e) {
        logger.error("Error while merging the filter from " + minorFragmentId + ", target " + baseEntry.getProbeScanCoordinates(), e);
        baseEntry.drop();
      }
      return baseEntry;
    }

    private static void quietClose(List<AutoCloseable> closeables) {
      try {
        AutoCloseables.close(closeables);
      } catch (Exception e) {
        logger.warn("Error on close", e);
      }
    }

    /**
     * Removes the entry from the list and free its resources.
     *
     * @param filterManagerEntry
     */
    public void remove(RuntimeFilterManagerEntry filterManagerEntry) {
      filterEntries.remove(filterManagerEntry);
      try {
        filterManagerEntry.close();
      } catch (Exception e) {
        logger.warn("Error on close for RuntimeFilterManagerEntry", e);
      }
    }

    private String toCoordinates(RuntimeFilter filter) {
        return String.format("OperatorId: %s, MajorFragmentID: %s", filter.getProbeScanOperatorId(), filter.getProbeScanMajorFragmentId());
    }

    /**
     * Number of filters dropped.
     *
     * @return
     */
    public long getFilterDropCount() {
        return this.filterDropCount;
    }

    /**
     * Number of individual column filters dropped
     * @return
     */
    public long getSubFilterDropCount() {
        return subFilterDropCount;
    }

    public void incrementDropCount() {
        this.filterDropCount++;
    }

    public void addDropCount(int dropCount) {
      this.filterDropCount += dropCount;
    }

    public void incrementColFilterDropCount() {
        this.subFilterDropCount++;
    }

    @Override
    public void close() throws Exception {
        // Wrap up remaining entries
        AutoCloseables.close(filterEntries);
        filterEntries.clear();
        AutoCloseables.close(allocator);
    }

    public class RuntimeFilterManagerEntry implements AutoCloseable {
        private RuntimeFilter compositeFilter;
        private Set<Integer> remainingMinorFragments = new HashSet<>();
        private BloomFilter partitionColFilter;
        private Map<String, ValueListFilter> nonPartitionColFilters;
        private boolean isDroppedFromProcessing = false;

        private RuntimeFilterManagerEntry(RuntimeFilter compositeFilter, Set<Integer> remainingMinorFragments,
                                          Optional<BloomFilter> partitionColFilter, List<ValueListFilter> nonPartitionColFilters) {
            this.compositeFilter = compositeFilter;
            this.remainingMinorFragments.addAll(remainingMinorFragments);
            this.partitionColFilter = partitionColFilter.orElse(null);
            this.nonPartitionColFilters = nonPartitionColFilters.stream()
                    .collect(Collectors.toMap(v -> v.getFieldName(), v -> v));
        }

        public RuntimeFilter getCompositeFilter() {
            return compositeFilter;
        }

        public Set<Integer> getRemainingMinorFragments() {
            return remainingMinorFragments;
        }

        public BloomFilter getPartitionColFilter() {
            return partitionColFilter;
        }

        public ValueListFilter getNonPartitionColFilter(final String colName) {
            return nonPartitionColFilters.get(colName);
        }

        /**
         * Returns ValueListFilters for non partition columns ordered according to the payload.
         *
         * @return
         */
        public List<ValueListFilter> getNonPartitionColFilters() {
            return compositeFilter.getNonPartitionColumnFilterList()
                    .stream()
                    .map(c -> nonPartitionColFilters.get(c.getColumns(0)))
                    .collect(Collectors.toList());
        }

        public boolean isComplete() {
            return getRemainingMinorFragments().isEmpty();
        }

        public String getProbeScanCoordinates() {
            return String.format("OperatorId: %s, MajorFragmentID: %s",
              compositeFilter.getProbeScanOperatorId(), compositeFilter.getProbeScanMajorFragmentId());
        }

        public boolean isDropped() {
            return isDroppedFromProcessing;
        }

        public void drop() {
          isDroppedFromProcessing = true;
          filterDropCount++;
        }

        public boolean isTargetedToSameScan(RuntimeFilter that) {
            return this.compositeFilter.getProbeScanMajorFragmentId() == that.getProbeScanMajorFragmentId() &&
              this.compositeFilter.getProbeScanOperatorId() == that.getProbeScanOperatorId();
        }

        public void merge(Optional<BloomFilter> incomingFilter) {
          if (partitionColFilter == null) {
            return;
          }
          checkArgument(incomingFilter.isPresent());

          final String incomingName = incomingFilter.get().getName();
          String existingName = partitionColFilter.getName();
          try {
            logger.debug("Merging incoming filter {} into existing filter {}", incomingName, partitionColFilter);
            partitionColFilter.merge(incomingFilter.get());
            if (partitionColFilter.isCrossingMaxFPP()) {
              dropPartitionColFilter();
            }
          } catch (Exception e) {
            logger.warn("Error while merging " + incomingName + " into " + existingName, e);
            dropPartitionColFilter();
          }
        }

        public void mergeAll(final List<ValueListFilter> incomingFilters) {
          final List<CompositeColumnFilter> baseEntryFilters = compositeFilter.getNonPartitionColumnFilterList();
          for (int i = 0; i < baseEntryFilters.size(); i++) {
            CompositeColumnFilter nonPartitionColFilterProto = baseEntryFilters.get(i);
            checkArgument(nonPartitionColFilterProto.getColumnsCount() == 1,
              "Non partition column filter should have single column");
            checkArgument(nonPartitionColFilterProto.getFilterType().equals(RuntimeFilterType.VALUE_LIST),
              "All non partition column filters should be of same value");
            final String colName = nonPartitionColFilterProto.getColumns(0);
            Optional<ValueListFilter> incomingFilter = incomingFilters.stream()
              .filter(f -> f.getFieldName().equalsIgnoreCase(colName)).findAny();
            if (incomingFilter.isPresent()) {
              merge(colName, incomingFilter.get());
            } else {
              // It is already dropped by the incoming module. Drop in the base entry as the piece is not available.
              dropNonPartitionColFilter(colName);
            }
          }
        }

        public void merge(String fieldName, ValueListFilter thatFilter) {
          List<AutoCloseable> closeables = new ArrayList<>(3);
          try {
            if (isDropped() || getNonPartitionColFilter(fieldName) == null) {
              logger.info("Skipping merge of non-partition col filter for the field {}", fieldName);
              return;
            }

            final ValueListFilter thisFilter = getNonPartitionColFilter(fieldName);
            checkArgument(thisFilter.isBoolField() == thatFilter.isBoolField(), "Cannot merge a boolean filter from a non-boolean one.");
            ValueListFilter mergedFilter = ValueListFilterBuilder.buildPlainInstance(allocator,
              thatFilter.getBlockSize(), valFilterMaxSize, thisFilter.isBoolField());
            closeables.add(mergedFilter);

            ValueListFilter.merge(thisFilter, thatFilter, mergedFilter);
            checkState(mergedFilter.getValueCount() <= valFilterMaxSize, "Merged valuelistfilter overflown for %s.", fieldName);
            logger.debug("Merged value list filter for column {}", fieldName);
            closeables.remove(mergedFilter);
            closeables.add(thisFilter);
            nonPartitionColFilters.put(fieldName, mergedFilter);
          } catch (Exception e) {
            logger.warn("Error while merging non-partition column filter for field " + fieldName, e);
            dropNonPartitionColFilter(fieldName);
          } finally {
            quietClose(closeables);
          }
        }

        private void dropPartitionColFilter() {
          try {
            partitionColFilter.close();

            // Remove from proto message. Since object is immutable we have copy -> edit -> re-store.
            compositeFilter = RuntimeFilter.newBuilder(compositeFilter).clearPartitionColumnFilter().build();
          } catch (Exception e) {
            logger.warn("Error while closing partition col filter for " + getProbeScanCoordinates(), e.getMessage());
          } finally {
            partitionColFilter = null;
            subFilterDropCount++;
            evaluateDropStatus();
          }
        }

        private void dropNonPartitionColFilter(String name) {
          ValueListFilter valFilter = getNonPartitionColFilter(name);
          if (valFilter == null) {
            logger.debug("Filter for {} is already dropped.", name);
            return;
          }

          try {
            valFilter.close();

            // Remove from proto message. Since object is immutable we have copy -> edit -> store
            final List<CompositeColumnFilter> compositeColumnFilters = compositeFilter.getNonPartitionColumnFilterList();
            RuntimeFilter.Builder msgFilterBuilder = RuntimeFilter.newBuilder();
            if (msgFilterBuilder.hasPartitionColumnFilter()) {
              msgFilterBuilder.setPartitionColumnFilter(compositeFilter.getPartitionColumnFilter());
            }
            compositeColumnFilters.stream().filter(c -> !c.getColumnsList().contains(name))
              .forEach(msgFilterBuilder::addNonPartitionColumnFilter);
            compositeFilter = msgFilterBuilder
              .setProbeScanOperatorId(compositeFilter.getProbeScanOperatorId())
              .setProbeScanMajorFragmentId(compositeFilter.getProbeScanMajorFragmentId())
              .build();
          } catch (Exception e) {
            logger.warn(String.format("Error while closing non-partition col filter for field %s, target %s",
              name, getProbeScanCoordinates()), e.getMessage());
          } finally {
            nonPartitionColFilters.put(name, null);
            subFilterDropCount++;
            evaluateDropStatus();
          }
        }

        private void evaluateDropStatus() {
          final boolean allNonPartitionColFiltersDropped =
            nonPartitionColFilters.entrySet().stream().allMatch(e -> e.getValue() == null);
          if (partitionColFilter == null && allNonPartitionColFiltersDropped) {
            drop();
          }
        }

        private void resetValueCounts() {
            RuntimeFilter.Builder protoFilterBuilder = RuntimeFilter.newBuilder(this.compositeFilter);
            if (this.partitionColFilter != null) {
                final CompositeColumnFilter partitionColFilter = CompositeColumnFilter.newBuilder(compositeFilter.getPartitionColumnFilter())
                        .setValueCount(this.partitionColFilter.getNumBitsSet()).build();
                protoFilterBuilder.setPartitionColumnFilter(partitionColFilter);
            }
            for (int i = 0; i < this.compositeFilter.getNonPartitionColumnFilterCount(); i++) {
                final CompositeColumnFilter current = this.compositeFilter.getNonPartitionColumnFilter(i);
                final ValueListFilter valueListFilter = this.getNonPartitionColFilter(current.getColumns(0));
                final CompositeColumnFilter nonPartitionColFilter = CompositeColumnFilter
                        .newBuilder(current)
                        .setValueCount(valueListFilter.getValueCount())
                        .setSizeBytes(valueListFilter.getSizeInBytes())
                        .build();
                protoFilterBuilder.setNonPartitionColumnFilter(i, nonPartitionColFilter);
            }
            this.compositeFilter = protoFilterBuilder.build();
        }

        @Override
        public void close() throws Exception {
            List<AutoCloseable> allCloseables = new ArrayList<>(nonPartitionColFilters.size() + 1);
            allCloseables.addAll(nonPartitionColFilters.values());
            allCloseables.add(partitionColFilter);
            AutoCloseables.close(allCloseables);
        }
    }
}
