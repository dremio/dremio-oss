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
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.ExecProtos.RuntimeFilter;


/**
 * Intended to serve as a temporary cache for the runtime filters until assembly is completed from all minor fragments
 * in shuffle join case.
 */
@NotThreadSafe
public class RuntimeFilterManager implements AutoCloseable {
    // TODO: Extend for non-partitoned column cases and value filters.

    private static Logger logger = LoggerFactory.getLogger(RuntimeFilterManager.class);
    private List<RuntimeFilterManagerEntry> filterEntries = new ArrayList<>();
    private final Set<Integer> allMinorFragments;
    private long filterDropCount = 0L;
    public RuntimeFilterManager(Set<Integer> allMinorFragments) {
        this.allMinorFragments = allMinorFragments;
    }

    /**
     * Adds the filter to the manager entries if not present; merges the filter if already exists.
     *
     * @param filter
     * @param partitionColFilter
     * @throws Exception
     */
    public RuntimeFilterManagerEntry coalesce(RuntimeFilter filter, BloomFilter partitionColFilter, int minorFragmentId) {
        Optional<RuntimeFilterManagerEntry> filterEntry = filterEntries.stream().filter(f -> f.isTargetedToSameScan(filter)).findAny();
        if (filterEntry.isPresent()) {
            return merge(filter, partitionColFilter, filterEntry.get(), minorFragmentId);
        } else {
            RuntimeFilterManagerEntry newEntry = new RuntimeFilterManagerEntry(filter, allMinorFragments, partitionColFilter);
            newEntry.remainingMinorFragments.remove(minorFragmentId);
            logger.debug("New filter entry created {}, remaining fragments {}", partitionColFilter.getName(), newEntry.remainingMinorFragments);
            filterEntries.add(newEntry);
            return newEntry;
        }
    }

    /**
     * Merges the filter piece with the respective entry present in the RuntimeFilterManagerEntry. The matching is done
     * on the basis of probe scan coordinates (operator id, major fragment id).
     *
     * @param filterPiece
     * @param partitionColFilterPiece
     * @param minorFragmentId
     */
    private RuntimeFilterManagerEntry merge(RuntimeFilter filterPiece, BloomFilter partitionColFilterPiece, RuntimeFilterManagerEntry baseEntry, int minorFragmentId) {
        final String incomingName = partitionColFilterPiece.getName();
        final String existingName = baseEntry.getPartitionColFilter().getName();
        try {
            checkNotNull(partitionColFilterPiece);
            checkArgument(baseEntry.getRemainingMinorFragments().contains(minorFragmentId), "Not expecting filter piece from "
                    + minorFragmentId + ", remaining minor fragments: " + baseEntry.getRemainingMinorFragments());
            baseEntry.remainingMinorFragments.remove(minorFragmentId);

            if (baseEntry.isNotDropped() && partitionColFilterPiece!=null && baseEntry.getPartitionColFilter()!=null) {
                logger.debug("Merging incoming filter {} into existing filter {}. Remaining fragments {}", incomingName, existingName, baseEntry.getRemainingMinorFragments());
                baseEntry.getPartitionColFilter().merge(partitionColFilterPiece);
                baseEntry.evaluateFppTolerance();
                filterDropCount = baseEntry.isNotDropped() ? filterDropCount: filterDropCount + 1;
            } else {
                logger.info("Skipping merge of filter piece from {} in {}", minorFragmentId, toCoordinates(filterPiece));
            }

            return baseEntry;
        } catch (Exception e) {
            logger.error("Error while merging " + incomingName + " into " + existingName, e);
            baseEntry.drop();
            filterDropCount = filterDropCount + 1;
        } finally {
            partitionColFilterPiece.close();
        }
        return baseEntry;
    }

    /**
     * Removes the entry from the list. The resources are not closed here before removal.
     * It is the responsibility of the caller to gracefully close the resources.
     *
     * @param filterManagerEntry
     */
    public void remove(RuntimeFilterManagerEntry filterManagerEntry) {
        filterEntries.remove(filterManagerEntry);
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

    @Override
    public void close() throws Exception {
        // Wrap up remaining entries
        List<BloomFilter> allCloseables = filterEntries.stream().map(RuntimeFilterManagerEntry::getPartitionColFilter).collect(Collectors.toList());
        AutoCloseables.close(allCloseables);
        filterEntries.clear();
    }

    public class RuntimeFilterManagerEntry {
        private RuntimeFilter compositeFilter;
        private Set<Integer> remainingMinorFragments = new HashSet<>();
        private BloomFilter partitionColFilter;
        private boolean isDroppedFromProcessing = false;

        private RuntimeFilterManagerEntry(RuntimeFilter compositeFilter, Set<Integer> remainingMinorFragments, BloomFilter partitionColFilter) {
            this.compositeFilter = compositeFilter;
            this.remainingMinorFragments.addAll(remainingMinorFragments);
            this.partitionColFilter = partitionColFilter;
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

        public boolean isComplete() {
            return getRemainingMinorFragments().isEmpty();
        }

        public String getProbeScanCoordinates() {
            return String.format("OperatorId: %s, MajorFragmentID: %s", compositeFilter.getProbeScanOperatorId(), compositeFilter.getProbeScanMajorFragmentId());
        }

        public boolean isNotDropped() {
            return !isDroppedFromProcessing;
        }

        public void evaluateFppTolerance() {
            if (partitionColFilter.isCrossingMaxFPP()) {
                logger.info("The error rate of combined filter {} is dropped below 5% at {}. " +
                        "Hence, dropping the filter.", partitionColFilter.getName(), partitionColFilter.getExpectedFPP());
                drop();
            }
        }

        public void drop() {
            isDroppedFromProcessing = true;
        }

        public boolean isTargetedToSameScan(RuntimeFilter that) {
            return this.compositeFilter.getProbeScanMajorFragmentId()==that.getProbeScanMajorFragmentId()
                    && this.compositeFilter.getProbeScanOperatorId()==that.getProbeScanOperatorId();
        }
    }
}


