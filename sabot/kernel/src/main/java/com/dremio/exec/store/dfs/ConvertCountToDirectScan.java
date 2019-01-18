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

package com.dremio.exec.store.dfs;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.common.JSONOptions;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.planner.logical.AggregateRel;
import com.dremio.exec.planner.logical.ProjectRel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.Prule;
import com.dremio.exec.planner.physical.ValuesPrel;
import com.dremio.exec.store.parquet.ParquetDatasetXAttrSerDe;
import com.dremio.exec.vector.complex.fn.ExtendedJsonOutput;
import com.dremio.exec.vector.complex.fn.JsonOutput;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.file.proto.ColumnValueCount;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.file.proto.ParquetDatasetSplitXAttr;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

/**
 * This rule will convert
 *   " select count(*)  as mycount from table "
 * or " select count( not-nullable-expr) as mycount from table "
 *   into
 *
 *    Project(mycount)
 *         \
 *    ValuesRel ((columnValueCount))
 *
 * or
 *    " select count(column) as mycount from table "
 *    into
 *      Project(mycount)
 *           \
 *            ValuesRel ((columnValueCount))
 *
 * Currently, only parquet group scan has the exact row count and column value count,
 * obtained from parquet row group info. This will save the cost to
 * scan the whole parquet files.
 */

public class ConvertCountToDirectScan extends Prule {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final SourceType type;
  private final int scanIndex;

  public static ConvertCountToDirectScan getAggProjOnScan(SourceType type){
    return new ConvertCountToDirectScan(
        RelOptHelper.some(AggregateRel.class, RelOptHelper.some(ProjectRel.class, RelOptHelper.any(FilesystemScanDrel.class))),
        type.value() + "Agg_on_proj_on_scan",
        2,
        type);
  }

  public static ConvertCountToDirectScan getAggOnScan(SourceType type){
    return new ConvertCountToDirectScan(
        RelOptHelper.some(AggregateRel.class, RelOptHelper.any(FilesystemScanDrel.class)),
        type.value() + "Agg_on_scan",
        1,
        type);
  }

  private ConvertCountToDirectScan(RelOptRuleOperand rule, String id, int scanIndex, SourceType type) {
    super(rule, "ConvertCountToDirectScan:" + id);
    this.type = type;
    this.scanIndex = scanIndex;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    FilesystemScanDrel scan = call.rel(scanIndex);
    if(scan.getFilter() != null){
      return false;
    }
    // we only support accurate counts when using Parquet, everything else is executed normally.
    return scan.getPluginId().getType().equals(type) && scan.getTableMetadata().getFormatSettings().getType() == FileType.PARQUET;
  }

  private static long getAccurateRowCount(Iterator<DatasetSplit> splits){
    long def = 0;
    while(splits.hasNext()){
      DatasetSplit split = splits.next();
      def += split.getRowCount();
    }
    return def;
  }

  private static long getAccurateColumnCount(String name, Iterator<DatasetSplit> splits){
    long def = 0;
    int splitCount = 0;
    int columnObservation = 0;
    while(splits.hasNext()){
      DatasetSplit split = splits.next();
      splitCount++;
      ParquetDatasetSplitXAttr xattr = ParquetDatasetXAttrSerDe.PARQUET_DATASET_SPLIT_XATTR_SERIALIZER.deserialize(split.getExtendedProperty().toByteArray());
      List<ColumnValueCount> counts = xattr.getColumnValueCountsList();
      for(ColumnValueCount c : counts){
        if(c.getColumn().equalsIgnoreCase(name)){
          def += c.getCount();
          columnObservation++;
          continue;
        }
      }
    }

    if(splitCount != columnObservation){
      // missing metadata observations, make sure to avoid wrong result.
      return GroupScan.NO_COLUMN_STATS;
    }
    return def;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final AggregateRel agg = (AggregateRel) call.rel(0);
    final FilesystemScanDrel scan = (FilesystemScanDrel) call.rel(call.rels.length -1);
    final ProjectRel proj = call.rels.length == 3 ? (ProjectRel) call.rel(1) : null;

    // Only apply the rule when :
    //    1) No GroupBY key,
    //    2) only one agg function (Check if it's count(*) below).
    //    3) No distinct agg call.
    if (!(agg.getGroupCount() == 0
        && agg.getAggCallList().size() == 1
        && !agg.containsDistinctCall())) {
      return;
    }

    AggregateCall aggCall = agg.getAggCallList().get(0);

    if (aggCall.getAggregation().getName().equals("COUNT") ) {

      long cnt = 0;
      //  count(*)  == >  empty arg  ==>  rowCount
      //  count(Not-null-input) ==> rowCount
      if (aggCall.getArgList().isEmpty() ||
          (aggCall.getArgList().size() == 1 &&
           ! agg.getInput().getRowType().getFieldList().get(aggCall.getArgList().get(0).intValue()).getType().isNullable())) {
        cnt = getAccurateRowCount(scan.getTableMetadata().getSplits());
      } else if (aggCall.getArgList().size() == 1) {
      // count(columnName) ==> Agg ( Scan )) ==> columnValueCount
        int index = aggCall.getArgList().get(0);

        if (proj != null) {
          // project in the middle of Agg and Scan : Only when input of AggCall is a RexInputRef in Project, we find the index of Scan's field.
          // For instance,
          // Agg - count($0)
          //  \
          //  Proj - Exp={$1}
          //    \
          //   Scan (col1, col2).
          // return count of "col2" in Scan's metadata, if found.

          if (proj.getProjects().get(index) instanceof RexInputRef) {
            index = ((RexInputRef) proj.getProjects().get(index)).getIndex();
          } else {
            return;  // do not apply for all other cases.
          }
        }

        String columnName = scan.getRowType().getFieldNames().get(index).toLowerCase();

        cnt = getAccurateColumnCount(columnName, scan.getTableMetadata().getSplits());
        if (cnt == GroupScan.NO_COLUMN_STATS) {
          // if column stats are not available don't apply this rule
          return;
        }
      } else {
        return; // do nothing.
      }

      RelDataType scanRowType = getCountRowType(agg.getCluster().getTypeFactory());
      final ValuesPrel values = new ValuesPrel(agg.getCluster(), scan.getTraitSet().plus(Prel.PHYSICAL).plus(DistributionTrait.SINGLETON), scanRowType, new JSONOptions(getResultsNode(cnt)));
      List<RexNode> exprs = Lists.newArrayList();
      exprs.add(RexInputRef.of(0, scanRowType));

      final ProjectPrel newProj = new ProjectPrel(agg.getCluster(), agg.getTraitSet().plus(Prel.PHYSICAL)
          .plus(DistributionTrait.SINGLETON), values, exprs, agg.getRowType());
      call.transformTo(newProj);
    }

  }

  private JsonNode getResultsNode(long count) {
    try{
      TokenBuffer out = new TokenBuffer(MAPPER.getFactory().getCodec(), false);
      JsonOutput json = new ExtendedJsonOutput(out);
      json.writeStartArray();
      json.writeStartObject();
      json.writeFieldName("count");
      json.writeBigInt(count);
      json.writeEndObject();
      json.writeEndArray();
      json.flush();
      return out.asParser().readValueAsTree();
    }catch(IOException ex){
      throw Throwables.propagate(ex);
    }
  }

  /**
   * Class to represent the count aggregate result.
   */
  public static class CountQueryResult {
    public long count;

    public CountQueryResult(long cnt) {
      this.count = cnt;
    }
  }

  private RelDataType getCountRowType(RelDataTypeFactory typeFactory) {
    List<RelDataTypeField> fields = Lists.newArrayList();
    fields.add(new RelDataTypeFieldImpl("count", 0, typeFactory.createSqlType(SqlTypeName.BIGINT)));
    return new RelRecordType(fields);
  }


}
