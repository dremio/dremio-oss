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
package com.dremio.exec.planner.normalizer.aggregaterewrite;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.COUNT;

import com.dremio.exec.store.NamespaceTable;
import java.util.Set;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

/** Rule for writing a COUNT(DISTINCT x) to NDV(x) when possible */
public final class CountDistinctConvertlet extends AggregateCallConvertlet {
  public static final CountDistinctConvertlet INSTANCE = new CountDistinctConvertlet();

  private CountDistinctConvertlet() {}

  @Override
  public boolean matches(ConvertletContext context) {
    AggregateCall aggregateCall = context.getOldCall();
    if (aggregateCall.getAggregation() != COUNT) {
      return false;
    }

    if (!aggregateCall.isDistinct()) {
      return false;
    }

    // We can rewrite a COUNT(DISTINCT x) to NDV(x) if:

    // 1) The approximate flag is set
    if (aggregateCall.isApproximate()) {
      return true;
    }

    // 2) Or the user used "enable approximate statistics" to specify that aggregations against a
    // specific column can be treated as approximate aggregations
    Aggregate oldAggRel = context.getOldAggRel();
    if (approximateStatisticsEnabled(
        oldAggRel.getCluster().getMetadataQuery(),
        oldAggRel.getInput(),
        aggregateCall.getArgList().get(0))) {
      return true;
    }

    return false;
  }

  @Override
  public boolean matches(AggregateCall aggregateCall) {
    throw new UnsupportedOperationException(
        "This method is not needed, since we overloaded the ConvertletContext variant");
  }

  @Override
  public RexNode convertCall(ConvertletContext convertletContext) {
    AggregateCall oldCall = convertletContext.getOldCall();
    AggregateCall newCall =
        AggregateCallFactory.ndv(oldCall.getArgList().get(0), oldCall.filterArg, oldCall.getName());

    return convertletContext.addAggregate(newCall);
  }

  private static boolean approximateStatisticsEnabled(
      RelMetadataQuery query, RelNode input, int columnIndex) {
    Set<RelColumnOrigin> origins = query.getColumnOrigins(input, columnIndex);

    // see if any column origin allowed a transformation.
    for (RelColumnOrigin o : origins) {
      RelOptTable table = o.getOriginTable();
      NamespaceTable namespaceTable = table.unwrap(NamespaceTable.class);
      if (namespaceTable == null) {
        // unable to decide, no way to transform.
        return false;
      }

      if (namespaceTable.isApproximateStatsAllowed()) {
        return true;
      }
    }

    return false;
  }
}
