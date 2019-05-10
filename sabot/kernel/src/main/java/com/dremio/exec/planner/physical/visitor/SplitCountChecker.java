/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.planner.physical.visitor;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.physical.LeafPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ScanPrelBase;

/**
 * Check the number of splits used by the scans in the query
 */
public class SplitCountChecker extends BasePrelVisitor<Prel, Void, UserException> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SplitCountChecker.class);

  private final int datasetMaxSplitLimit;

  private int querySplitCount;

  private SplitCountChecker(int datasetMaxSplitLimit) {
    this.datasetMaxSplitLimit = datasetMaxSplitLimit;
    this.querySplitCount = 0;
  }

  /**
   * Check the number of splits at both the query and dataset level
   * If the number of splits is exceeded, throws a {@link UserException#unsupportedError()}
   * @param root                   root of the query
   * @param queryMaxSplitLimit     per-query split count limit
   * @param datasetMaxSplitLimit   per-dataset split count limit
   * @return the transformed Prel node, always equal to 'root'
   */
  public static Prel checkNumSplits(Prel root, int queryMaxSplitLimit, int datasetMaxSplitLimit) {
    SplitCountChecker splitCountChecker = new SplitCountChecker(datasetMaxSplitLimit);
    Prel result = root.accept(splitCountChecker, null);
    if (splitCountChecker.querySplitCount > queryMaxSplitLimit) {
      throw UserException.unsupportedError()
        .message("Number of splits in the query (%d) exceeds the query split limit of %d",
          splitCountChecker.querySplitCount, queryMaxSplitLimit)
        .build(logger);
    }
    return result;
  }

  @Override
  public Prel visitLeaf(LeafPrel prel, Void value) throws UserException {
    if (!(prel instanceof ScanPrelBase)) {
      // Nothing to do for non-scans
      return prel;
    }
    ScanPrelBase scan = (ScanPrelBase) prel;
    final int scanSplitCount = scan.getTableMetadata().getSplitCount();
    if (scanSplitCount > datasetMaxSplitLimit) {
      throw UserException.unsupportedError()
          .message("Number of splits (%d) in dataset %s exceeds dataset split limit of %d",
              scanSplitCount, String.join(".", scan.getTable().getQualifiedName()), datasetMaxSplitLimit)
          .build(logger);
    }
    querySplitCount += scanSplitCount;
    return scan;
  }

  @Override
  public Prel visitPrel(Prel prel, Void value) throws UserException {
    for (Prel child : prel) {
      child.accept(this, value);
    }
    return prel;
  }
}
