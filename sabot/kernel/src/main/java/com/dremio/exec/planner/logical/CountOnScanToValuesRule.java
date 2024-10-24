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
package com.dremio.exec.planner.logical;

import static com.dremio.service.namespace.DatasetHelper.isConvertedIcebergDataset;
import static com.dremio.service.namespace.DatasetHelper.isIcebergDataset;

import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.store.TableMetadata;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.dataset.proto.ScanStats;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * This rule will convert " select count(*) as mycount from table " or " select count(
 * not-nullable-expr) as mycount from table " into
 *
 * <p>Project(mycount) \ ValuesRel ((columnValueCount))
 *
 * <p>or " select count(column) as mycount from table " into Project(mycount) \ ValuesRel
 * ((columnValueCount))
 *
 * <p>Currently, only the native iceberg dataset is supported.
 */
public class CountOnScanToValuesRule extends RelOptRule {

  public static final CountOnScanToValuesRule AGG_ON_SCAN_INSTANCE =
      new CountOnScanToValuesRule(
          RelOptHelper.some(LogicalAggregate.class, RelOptHelper.any(ScanRelBase.class)),
          "Agg_on_scan",
          1);

  public static final CountOnScanToValuesRule AGG_ON_PROJ_ON_SCAN_INSTANCE =
      new CountOnScanToValuesRule(
          RelOptHelper.some(
              LogicalAggregate.class,
              RelOptHelper.some(LogicalProject.class, RelOptHelper.any(ScanRelBase.class))),
          "Agg_on_proj_on_scan",
          2);

  private final int scanIndex;

  public CountOnScanToValuesRule(RelOptRuleOperand operand, String description, int scanIndex) {
    super(operand, "CountOnScanToValuesRule:" + description);
    this.scanIndex = scanIndex;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalAggregate agg = call.rel(0);
    final ScanRelBase scan = call.rel(scanIndex);
    final RexBuilder rexBuilder = agg.getCluster().getRexBuilder();

    RelDataType bigIntType = agg.getCluster().getTypeFactory().createSqlType(SqlTypeName.BIGINT);

    RexLiteral tuple =
        (RexLiteral)
            rexBuilder.makeLiteral(
                scan.getTableMetadata().getReadDefinition().getScanStats().getRecordCount(),
                bigIntType,
                false);

    call.transformTo(
        LogicalProject.create(
            LogicalValues.create(
                agg.getCluster(),
                new RelRecordType(List.of(new RelDataTypeFieldImpl("count", 0, bigIntType))),
                ImmutableList.of(ImmutableList.of(tuple))),
            List.of(),
            List.of(rexBuilder.makeInputRef(bigIntType, 0)),
            agg.getRowType()));
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final ScanRelBase scan = call.rel(scanIndex);

    // Check to avoid NPEs.
    if (Optional.ofNullable(scan.getTableMetadata())
        .map(TableMetadata::getReadDefinition)
        .map(ReadDefinition::getScanStats)
        .map(ScanStats::getRecordCount)
        .isEmpty()) {
      return false;
    }

    final DatasetConfig datasetConfig = scan.getTableMetadata().getDatasetConfig();

    // we only want to support this for native iceberg.
    // Check out ConvertCountToDirectScan for unlimited splits.
    if (isConvertedIcebergDataset(datasetConfig) || !isIcebergDataset(datasetConfig)) {
      return false;
    }

    // This optimization cannot apply if there are position or equality
    // delete files present in this dataset. A full data scan would be
    // necessary since there is no way to accurately determine the record
    // count based on the manifest data.
    if (Optional.ofNullable(datasetConfig.getPhysicalDataset().getIcebergMetadata())
        .map(IcebergMetadata::getDeleteStats)
        .map(ScanStats::getRecordCount)
        .filter(recordCount -> recordCount > 0)
        .isPresent()) {
      return false;
    }

    final LogicalAggregate agg = call.rel(0);

    // Only apply the rule when :
    //    1) No GroupBY key,
    //    2) only one agg function (Check if it's count(*) below).
    //    3) No distinct agg call.
    if (!(agg.getGroupCount() == 0
        && agg.getAggCallList().size() == 1
        && !agg.containsDistinctCall())) {
      return false;
    }

    AggregateCall aggCall = agg.getAggCallList().get(0);

    //  count(*)  == >  empty arg  ==>  rowCount
    //  count(Not-null-input) ==> rowCount
    return aggCall.getAggregation().getName().equals("COUNT")
        && (aggCall.getArgList().isEmpty()
            || (aggCall.getArgList().size() == 1
                && !agg.getInput()
                    .getRowType()
                    .getFieldList()
                    .get(aggCall.getArgList().get(0))
                    .getType()
                    .isNullable()));
  }
}
