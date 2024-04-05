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
package com.dremio.exec.store.parquet;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.options.OptionManager;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.calcite.util.Pair;

public class SplitsPathRowGroupsMap {
  private final Map<String, Set<Pair<Long, Long>>> pathRanges;
  private final int projectedColumnsLen;
  private long pageSizeBytes, totalHeapObjects;

  SplitsPathRowGroupsMap(
      final List<ParquetBlockBasedSplit> splits, int projectedColumnsLen, OptionManager options) {
    pathRanges = new HashMap<>();
    this.projectedColumnsLen = Math.max(projectedColumnsLen, 1);
    this.totalHeapObjects = options.getOption(ExecConstants.TOTAL_HEAP_OBJ_SIZE);
    this.pageSizeBytes = options.getOption(ExecConstants.PAGE_SIZE_IN_BYTES);

    for (final ParquetBlockBasedSplit split : splits) {
      final String path = split.getPath();
      Set<Pair<Long, Long>> splitRanges = pathRanges.get(path);
      if (splitRanges == null) {
        splitRanges = new TreeSet<>();
        pathRanges.put(path, splitRanges);
      }
      splitRanges.add(Pair.of(split.getStart(), split.getLength()));
    }
  }

  // getMaxRowGroupsToRetain api return the rowgroups number based on the total heap object.
  int getMaxRowGroupsToRetain(final MutableParquetMetadata footer) {
    long blockSize = footer.getBlocks().get(0).getTotalByteSize();
    long estimatedPageCount =
        blockSize / (footer.getBlocks().get(0).getColumns().size() * this.pageSizeBytes);
    if (estimatedPageCount < 10) {
      estimatedPageCount = 10;
    }

    long heapObjectsPerRowGroup = this.projectedColumnsLen * estimatedPageCount;
    int rowGroupsRequired = (int) (this.totalHeapObjects / heapObjectsPerRowGroup);
    // Read footer for a minimum 5 row groups at a time.
    if (rowGroupsRequired < 5) {
      rowGroupsRequired = 5;
    }
    return rowGroupsRequired;
  }

  Set<Integer> getPathRowGroups(
      final String path, final MutableParquetMetadata footer, int rowGroupIndexToStartFrom) {
    final Set<Pair<Long, Long>> ranges = pathRanges.get(path);
    if (ranges == null) {
      return null;
    }
    List<Integer> rowGroups = Lists.newLinkedList();
    ranges.stream()
        .forEach(
            r -> {
              try {
                rowGroups.addAll(
                    ParquetReaderUtility.getRowGroupNumbersFromFileSplit(r.left, r.right, footer));
              } catch (IOException e) {
                throw UserException.ioExceptionError(e).buildSilently();
              }
            });
    int rowGroupSize = rowGroups.size();
    if (rowGroupSize < 5 || this.totalHeapObjects == 0) {
      pathRanges.remove(path);
      return rowGroups.stream().collect(Collectors.toSet());
    }

    int rowGroupsRequired = getMaxRowGroupsToRetain(footer);
    final Set<Integer> usedRowGroups = new HashSet<>();
    // Only keep rowgroups starting from rowGroupIndexToStartFrom till rowGroupsRequired
    Iterator<Integer> iterator = rowGroups.iterator();
    if (rowGroupSize > rowGroupsRequired) {
      int count = 0;
      while (iterator.hasNext()) {
        int currentRowGroup = iterator.next();
        if (currentRowGroup == rowGroupIndexToStartFrom) {
          usedRowGroups.add(currentRowGroup);
          count++;
          while (count < rowGroupsRequired && iterator.hasNext()) {
            usedRowGroups.add(iterator.next());
            count++;
          }
        }
        // Once all the required row groups are added to set, break from the loop.
        if (count == rowGroupsRequired) {
          break;
        }
      }
    } else {
      usedRowGroups.addAll(rowGroups);
    }
    // if all element of the list has been read, remove path from path range.
    if (rowGroupSize <= rowGroupsRequired || !iterator.hasNext()) {
      pathRanges.remove(path);
    }
    return usedRowGroups;
  }
}
