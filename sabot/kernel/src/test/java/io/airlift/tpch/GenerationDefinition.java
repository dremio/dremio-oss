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
package io.airlift.tpch;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

public class GenerationDefinition {

  public static GenerationDefinition SCALE_01_SINGLE = new GenerationDefinition(1, Long.MAX_VALUE);

  private double scaleFactor;
  private long targetPartitionSize;

  public GenerationDefinition(double scaleFactor, long targetPartitionSize) {
    super();
    checkArgument(scaleFactor > 0, "scaleFactor must be greater than 0");
    this.scaleFactor = scaleFactor;
    this.targetPartitionSize = targetPartitionSize;
  }

  public long getStartIndex(TpchTable table, int partitionIndex) {
    return calculateStartIndex(
        table.scaleBase, scaleFactor, partitionIndex, getPartitionCount(table.scaleBase));
  }

  public long getRowCount(TpchTable table, int partitionIndex) {
    if (table.fixedSize) {
      return table.scaleBase;
    }

    return calculateRowCount(
        table.scaleBase, scaleFactor, partitionIndex, getPartitionCount(table.scaleBase));
  }

  private int getPartitionCount(int scaleBase) {
    long totalRowCount = (long) (scaleBase * scaleFactor);
    int partitionCount = (int) (totalRowCount / targetPartitionSize);
    if (partitionCount < 1) {
      partitionCount = 1;
    }
    return partitionCount;
  }

  private static long calculateRowCount(
      int scaleBase, double scaleFactor, int partitionIndex, int partitionCount) {
    checkArgument(partitionIndex >= 1, "part must be at least 1");
    checkArgument(
        partitionIndex <= partitionCount, "part must be less than or equal to part count");

    long totalRowCount = (long) (scaleBase * scaleFactor);
    long rowCount = totalRowCount / partitionCount;
    if (partitionIndex == partitionCount) {
      // for the last part, add the remainder rows
      rowCount = rowCount + (totalRowCount % partitionCount);
    }
    return rowCount;
  }

  private static long calculateStartIndex(
      int scaleBase, double scaleFactor, int partitionIndex, int partitionCount) {
    checkArgument(partitionIndex >= 1, "part must be at least 1");
    checkArgument(
        partitionIndex <= partitionCount, "part must be less than or equal to part count");

    long totalRowCount = (long) (scaleBase * scaleFactor);

    long rowsPerPart = totalRowCount / partitionCount;
    return rowsPerPart * (partitionIndex - 1);
  }

  public static enum TpchTable {
    CUSTOMER(150_000),
    TEMPERATURE(150_000),
    WORD_GROUPS(150_000),
    MIXED_GROUPS(150_000),
    CUSTOMER_LIMITED(1, true),
    SUPPLIER(10_000),
    NATION(Distributions.getDefaultDistributions().getNations().size(), true),
    REGION(Distributions.getDefaultDistributions().getRegions().size(), true),
    LIST_STRUCT(150_000);

    public final int scaleBase;
    public final boolean fixedSize;

    TpchTable(int scaleBase) {
      this(scaleBase, false);
    }

    TpchTable(int scaleBase, boolean fixedSize) {
      this.scaleBase = scaleBase;
      this.fixedSize = fixedSize;
    }

    public RelDataType getDataType(RelDataTypeFactory factory) {
      return null;
    }

    public long getTotalRecords(double scaleFactor) {
      if (fixedSize) {
        return scaleBase;
      }
      return (long) (scaleBase * scaleFactor);
    }
  }
}
