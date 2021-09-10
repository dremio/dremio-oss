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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Window.Group;
import org.apache.calcite.rel.core.Window.RexWinAggCall;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.BuiltInMetadata.ColumnOrigin;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;

import com.dremio.exec.planner.acceleration.ExpansionNode;
import com.dremio.exec.planner.common.JdbcRelBase;
import com.dremio.exec.planner.common.LimitRelBase;
import com.dremio.exec.planner.common.SampleRelBase;
import com.dremio.exec.planner.physical.CustomPrel;
import com.dremio.exec.planner.physical.DictionaryLookupPrel;
import com.dremio.exec.planner.physical.ExchangePrel;
import com.dremio.exec.planner.physical.SelectionVectorRemoverPrel;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.sabot.op.fromjson.ConvertFromJsonPOP.ConversionColumn;
import com.dremio.sabot.op.fromjson.ConvertFromJsonPrel;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

/**
 * Override {@link RelMdColumnOrigins} to provide column origins for:
 *
 * - {@link SampleRelBase}
 * - {@link JdbcRelBase}
 * - {@link TableScan}
 */
public class RelMdColumnOrigins implements MetadataHandler<BuiltInMetadata.ColumnOrigin> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.COLUMN_ORIGIN.method, new RelMdColumnOrigins());


  @Override
  public MetadataDef<ColumnOrigin> getDef() {
    return BuiltInMetadata.ColumnOrigin.DEF;
  }

  public Set<RelColumnOrigin> getColumnOrigins(RelSubset rel, RelMetadataQuery mq, int iOutputColumn) {
    return mq.getColumnOrigins(MoreObjects.firstNonNull(rel.getBest(), rel.getOriginal()), iOutputColumn);
  }

  public Set<RelColumnOrigin> getColumnOrigins(ConvertFromJsonPrel rel, RelMetadataQuery mq, int iOutputColumn) {
    final List<ConversionColumn> conversions = rel.getConversions();
    if (iOutputColumn < conversions.size()) {
      return null; // just return null for now, it should work for ConvertFromJsonConverter
    }
    return mq.getColumnOrigins(rel.getInput(), iOutputColumn);
  }

  public Set<RelColumnOrigin> getColumnOrigins(LimitRelBase rel, RelMetadataQuery mq, int iOutputColumn) {
    return mq.getColumnOrigins(rel.getInput(), iOutputColumn);
  }

  public Set<RelColumnOrigin> getColumnOrigins(ExpansionNode rel, RelMetadataQuery mq, int iOutputColumn) {
    if (rel.isDefault()) {
      return Collections.emptySet();
    }
    return mq.getColumnOrigins(rel.getInput(), iOutputColumn);
  }

  public Set<RelColumnOrigin> getColumnOrigins(CustomPrel rel, RelMetadataQuery mq, int iOutputColumn) {
    return mq.getColumnOrigins(rel.getOriginPrel(), iOutputColumn);
  }

  public Set<RelColumnOrigin> getColumnOrigins(SampleRelBase rel, RelMetadataQuery mq, int iOutputColumn) {
    return mq.getColumnOrigins(rel.getInput(), iOutputColumn);
  }

  public Set<RelColumnOrigin> getColumnOrigins(DictionaryLookupPrel rel, RelMetadataQuery mq, int iOutputColumn) {
    return mq.getColumnOrigins(rel.getInput(), iOutputColumn);
  }

  public Set<RelColumnOrigin> getColumnOrigins(JdbcRelBase jdbc, RelMetadataQuery mq, int iOutputColumn) {
    return mq.getColumnOrigins(jdbc.getSubTree(), iOutputColumn);
  }

  public Set<RelColumnOrigin> getColumnOrigins(ExchangePrel exchangePrel, RelMetadataQuery mq, int iOutputColumn) {
    return mq.getColumnOrigins(exchangePrel.getInput(), iOutputColumn);
  }

  public Set<RelColumnOrigin> getColumnOrigins(SelectionVectorRemoverPrel selectionVectorRemoverPrel, RelMetadataQuery mq, int iOutputColumn) {
    return mq.getColumnOrigins(selectionVectorRemoverPrel.getInput(), iOutputColumn);
  }

  @SuppressWarnings("unused") // Called through reflection
  public Set<RelColumnOrigin> getColumnOrigins(LogicalWindow window, RelMetadataQuery mq, int iOutputColumn) {
    final RelNode inputRel = window.getInput();
    final int numFieldsInInput = inputRel.getRowType().getFieldCount();
    if (iOutputColumn < numFieldsInInput) {
      return mq.getColumnOrigins(inputRel, iOutputColumn);
    }

    if (iOutputColumn >= window.getRowType().getFieldCount()) {
      return Collections.emptySet();
    }

    int startGroupIdx = iOutputColumn - numFieldsInInput;
    int curentIdx = 0;
    Group finalGroup = null;
    for (Group group : window.groups) {
      curentIdx += group.aggCalls.size();
      if (curentIdx > startGroupIdx) {
        // this is the group
        finalGroup = group;
        break;
      }
    }
    Preconditions.checkNotNull(finalGroup);
    // calculating index of the aggCall within a group
    // currentIdx = through idx within groups/aggCalls (max currentIdx = sum(groups size * aggCals_per_group) )
    // since currentIdx at this moment points to the end of the group substracting aggCals_per_group
    // to get to the beginning of the group and have startGroupIdx substract the diff
    final int aggCalIdx = startGroupIdx - (curentIdx - finalGroup.aggCalls.size());
    Preconditions.checkElementIndex(aggCalIdx, finalGroup.aggCalls.size());

    final Set<RelColumnOrigin> set = new HashSet<>();
    // Add aggregation column references
    final RexWinAggCall aggCall = finalGroup.aggCalls.get(aggCalIdx);
    for (RexNode operand : aggCall.operands) {
      if (operand instanceof RexInputRef) {
        final RexInputRef opInputRef = (RexInputRef) operand;
        if (opInputRef.getIndex() < numFieldsInInput) {
          Set<RelColumnOrigin> inputSet =
            mq.getColumnOrigins(inputRel, opInputRef.getIndex());
          inputSet = createDerivedColumnOrigins(inputSet);
          if (inputSet != null) {
            set.addAll(inputSet);
          }
        }
      }
    }

    return set;
  }

  public Set<RelColumnOrigin> getColumnOrigins(TableScan tableScan, RelMetadataQuery mq, int iOutputColumn) {
    final Set<RelColumnOrigin> set = new HashSet<>();
    RelOptTable table = tableScan.getTable();
    if (table == null) {
      return null;
    }

    if (table.getRowType() != tableScan.getRowType()) {
      String columnName = tableScan.getRowType().getFieldNames().get(iOutputColumn);
      int newOutputColumn = 0;
      for (String tableCol : table.getRowType().getFieldNames()) {
        if (tableCol.equals(columnName)) {
          set.add(new RelColumnOrigin(table, newOutputColumn, false));
          return set;
        }
        newOutputColumn++;
      }
    } else {
      set.add(new RelColumnOrigin(table, iOutputColumn, false));
    }
    return set;
  }

  public Set<RelColumnOrigin> getColumnOrigins(TableFunctionPrel tableScan, RelMetadataQuery mq, int iOutputColumn) {
    final Set<RelColumnOrigin> set = new HashSet<>();
    RelOptTable table = tableScan.getTable();
    if (table == null) {
      return null;
    }

    if (table.getRowType() != tableScan.getRowType()) {
      String columnName = tableScan.getRowType().getFieldNames().get(iOutputColumn);
      int newOutputColumn = 0;
      for (String tableCol : table.getRowType().getFieldNames()) {
        if (tableCol.equals(columnName)) {
          set.add(new RelColumnOrigin(table, newOutputColumn, false));
          return set;
        }
        newOutputColumn++;
      }
    } else {
      set.add(new RelColumnOrigin(table, iOutputColumn, false));
    }
    return set;
  }

  private Set<RelColumnOrigin> createDerivedColumnOrigins(
      Set<RelColumnOrigin> inputSet) {
    if (inputSet == null) {
      return null;
    }
    final Set<RelColumnOrigin> set = new HashSet<>();
    for (RelColumnOrigin rco : inputSet) {
      RelColumnOrigin derived =
          new RelColumnOrigin(
              rco.getOriginTable(),
              rco.getOriginColumnOrdinal(),
              true);
      set.add(derived);
    }
    return set;
  }
}
