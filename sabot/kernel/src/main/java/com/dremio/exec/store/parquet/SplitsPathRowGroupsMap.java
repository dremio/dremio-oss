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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.calcite.util.Pair;

import com.dremio.common.exceptions.UserException;

public class SplitsPathRowGroupsMap {
  private final Map<String, Set<Pair<Long, Long>>> pathRanges;

  SplitsPathRowGroupsMap(final List<ParquetBlockBasedSplit> splits) {
    pathRanges = new HashMap<>();
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

  Set<Integer> getPathRowGroups(final String path, final MutableParquetMetadata footer) {
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
