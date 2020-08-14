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
package com.dremio.exec.store.hive.exec;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.calcite.util.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.store.parquet.MutableParquetMetadata;
import com.dremio.exec.store.parquet.ParquetReaderUtility;

/**
 * Utility class to maintain mapping of file {@link Path} to all the row groups
 * from the same file across {@link FileSplit}s in a Hive subscan processing.
 * Introduced to support reduction of Parquet footer heap size in Hive parquet scans.
 */
public class HiveSplitsPathRowGroupsMap {
  private final Map<Path, Set<Pair<Long, Long>>> pathRanges;

  HiveSplitsPathRowGroupsMap(final List<HiveParquetSplit> splits) {
    pathRanges = new HashMap<>();
    for (final HiveParquetSplit split : splits) {
      final FileSplit fileSplit = split.getFileSplit();
      final Path path = fileSplit.getPath();
      Set<Pair<Long, Long>> splitRanges = pathRanges.get(path);
      if (splitRanges == null) {
        splitRanges = new TreeSet<>();
        pathRanges.put(path, splitRanges);
      }
      splitRanges.add(Pair.of(fileSplit.getStart(), fileSplit.getLength()));
    }
  }

  Set<Integer> getPathRowGroups(final Path path, final MutableParquetMetadata footer) {
    final Set<Pair<Long, Long>> ranges = pathRanges.get(path);
    if (ranges == null) {
      return null;
    }
    final Set<Integer> rowGroups = new HashSet<>();
    ranges.stream().forEach(r -> {
        try {
          rowGroups.addAll(ParquetReaderUtility.getRowGroupNumbersFromFileSplit(r.left, r.right, footer));
        } catch (IOException e) {
          throw UserException.ioExceptionError(e).buildSilently();
        }
      }
    );
    pathRanges.remove(path);
    return rowGroups;
  }
}
