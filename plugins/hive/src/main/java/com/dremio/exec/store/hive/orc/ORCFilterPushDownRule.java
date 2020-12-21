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
package com.dremio.exec.store.hive.orc;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.logical.FilterRel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.hive.HiveRulesFactory.HiveScanDrel;
import com.dremio.exec.store.hive.ORCScanFilter;
import com.dremio.exec.store.hive.exec.HiveORCVectorizedReader;
import com.dremio.exec.store.hive.exec.HiveProxyingOrcScanFilter;
import com.dremio.exec.store.hive.exec.HiveReaderProtoUtil;
import com.dremio.exec.store.hive.proxy.HiveProxiedOrcScanFilter;
import com.dremio.exec.store.parquet.ParquetFilterCondition;
import com.dremio.hive.proto.HiveReaderProto;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattr;
import com.github.slugify.Slugify;
import com.google.common.base.Optional;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Pushes the filter into Hive ORC reader {@link HiveORCVectorizedReader}. We still retain the filter in rel tree as the
 * reader filters only based on the ORC stripe stats to avoid reading more data from disk.
 */
public class ORCFilterPushDownRule extends RelOptRule {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ORCFilterPushDownRule.class);
  private static final Slugify SLUGIFY = new Slugify();

  private final StoragePluginId pluginId;

  public ORCFilterPushDownRule(StoragePluginId pluginId) {
    // Note: matches to HiveScanDrel.class with this rule instance are guaranteed to be local to the same plugin
    // because this match implicitly ensures the classloader is the same.
    super(RelOptHelper.some(FilterRel.class, RelOptHelper.any(HiveScanDrel.class)),
      pluginId.getType().value() + "ORC.PushFilterIntoScan."
        + SLUGIFY.slugify(pluginId.getName()) + "." + UUID.randomUUID().toString());
    this.pluginId = pluginId;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final HiveScanDrel scan = call.rel(1);
    if (scan.getFilter() != null) {
      return false;
    }
    try {
      final HiveTableXattr tableXattr =
          HiveTableXattr.parseFrom(scan.getTableMetadata().getReadDefinition().getExtendedProperty().asReadOnlyByteBuffer());
      final Optional<String> inputFormat = HiveReaderProtoUtil.getTableInputFormat(tableXattr);
      return inputFormat.isPresent() && inputFormat.get().equals(OrcInputFormat.class.getCanonicalName());
    } catch (InvalidProtocolBufferException e) {
      logger.warn("Failure while attempting to deserialize hive table attributes.", e);
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final HiveScanDrel scan = call.rel(1);
    final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
    final RexNode originalFilter = filter.getCondition();

    try {
      final ORCFindRelevantFilters filterFinder = new ORCFindRelevantFilters(rexBuilder, scan.getRowType());
      RexNode filterThatCanBePushed = originalFilter.accept(filterFinder);
      if (filterThatCanBePushed == null) {
        return;
      }

      // Convert the filter expression that is just an input ref on bool column into a function call.
      // SearchArgumentGenerator can only work on filter expressions where root is a function call.
      filterThatCanBePushed =
          ORCFindRelevantFilters.convertBooleanInputRefToFunctionCall(rexBuilder, filterThatCanBePushed);

      final HiveTableXattr tableXattr =
        HiveTableXattr.parseFrom(scan.getTableMetadata().getReadDefinition().getExtendedProperty().asReadOnlyByteBuffer());
      final List<HiveReaderProto.ColumnInfo> columnInfos = tableXattr.getColumnInfoList();
      List<HiveReaderProto.ColumnInfo> selectedColumnInfos = new ArrayList<>();
      final List<String> columnNames = scan.getRowType().getFieldNames();
      final Set<String> columnNameSet = columnNames.stream().map(String::toUpperCase).collect(Collectors.toSet());
      final BatchSchema scanTableSchema = scan.getTableMetadata().getSchema();
      final List<SchemaPath> filterColumns = new ArrayList<>();
      try {
        filterColumns.add(ParquetFilterCondition.rexToSchemaPath(filterThatCanBePushed, scan.getRowType()));
      } catch(UnsupportedOperationException ignored) {}

      // columnInfos contains hive data type info
      // scanTableSchema is table BatchSchema
      // columnNames are selected / projected column names
      // Here we prepare column info that contains hive data type information for selected columns
      // Iterate over all fields of table schema, and if it is in projected columnNames list,
      // then add ColumnInfo to selectedColumnInfos
      if (columnInfos.size() == scanTableSchema.getFieldCount()) {
        for (int fieldPos = 0; fieldPos < scanTableSchema.getFieldCount(); ++fieldPos) {
          if (columnNameSet.contains(scanTableSchema.getColumn(fieldPos).getName().toUpperCase())) {
            selectedColumnInfos.add(columnInfos.get(fieldPos));
          }
        }
      }

      final ORCSearchArgumentGenerator sargGenerator = new ORCSearchArgumentGenerator(columnNames, selectedColumnInfos);
      filterThatCanBePushed.accept(sargGenerator);
      final SearchArgument sarg = sargGenerator.get();

      final HiveProxiedOrcScanFilter proxiedOrcScanFilter = new ORCScanFilter(sarg, pluginId, filterFinder.getColumn());
      final HiveProxyingOrcScanFilter proxyingOrcScanFilter = new HiveProxyingOrcScanFilter(pluginId.getName(), proxiedOrcScanFilter);

      final RelNode newScan = scan.applyFilter(proxyingOrcScanFilter);

      // We still need the original filter in Filter operator as the ORC filtering is based only on the stripe stats and
      // we could end up with values out of ORC reader that don't satisfy the filter.
      call.transformTo(filter.copy(filter.getTraitSet(), newScan, originalFilter));
    } catch (Exception e) {
      logger.warn("Failed to push filter into ORC reader", e);
      // ignore the exception and continue with planning
    }
  }
}
