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
package com.dremio.exec.planner.cost;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.NumberUtil;

/**
 * Override some of the methods in base calcite class until we can submit upstream
 */
public class RelMdPercentageOriginalRows extends org.apache.calcite.rel.metadata.RelMdPercentageOriginalRows {
  private static final RelMdPercentageOriginalRows INSTANCE = new RelMdPercentageOriginalRows();

  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(BuiltInMethod.PERCENTAGE_ORIGINAL_ROWS.method, INSTANCE);

  public Double getPercentageOriginalRows(TableScan tablescan, RelMetadataQuery mq) {
    return quotientForPercentage(mq.getRowCount(tablescan), tablescan.getTable().getRowCount());
  }

  @Override
  public Double getPercentageOriginalRows(Aggregate rel, RelMetadataQuery mq) {
    // REVIEW jvs 28-Mar-2006: The assumption here seems to be that
    // aggregation does not apply any filtering, so it does not modify the
    // percentage.  That's very much oversimplified.
    Double inputPercentage = mq.getPercentageOriginalRows(rel.getInput());
    Double newPercentage = NumberUtil.divide(mq.getRowCount(rel), mq.getRowCount(rel.getInput()));
    return NumberUtil.multiply(inputPercentage, newPercentage);
  }

  @Override
  public Double getPercentageOriginalRows(Union rel, RelMetadataQuery mq) {
    double numerator = 0.0;
    double denominator = 0.0;

    for (RelNode input : rel.getInputs()) {
      double rowCount = mq.getRowCount(input);
      Double percentage = mq.getPercentageOriginalRows(input);
      if (percentage == null) {
        return null;
      }
      if (percentage != 0.0) {
        denominator += rowCount / percentage;
        numerator += rowCount;
      }
    }

    return quotientForPercentage(numerator, denominator);
  }

  @Override
  public Double getPercentageOriginalRows(Join rel, RelMetadataQuery mq) {
    Double left = mq.getPercentageOriginalRows(rel.getLeft());
    Double right = mq.getPercentageOriginalRows(rel.getRight());
    return NumberUtil.multiply(left, right);
  }
}
