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
package com.dremio.exec.planner.physical;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.physical.base.IcebergWriterOptions;
import com.dremio.exec.physical.base.ImmutableIcebergWriterOptions;
import com.dremio.exec.physical.base.ImmutableTableFormatWriterOptions;
import com.dremio.exec.physical.base.TableFormatWriterOptions;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.logical.WriterRel;
import com.dremio.exec.planner.sql.parser.DmlUtils;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.IcebergManifestWriterPrel;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.collect.ImmutableList;

public class WriterPrule extends Prule {

  public static final RelOptRule INSTANCE = new WriterPrule();

  public WriterPrule() {
    super(RelOptHelper.some(WriterRel.class, Rel.LOGICAL, RelOptHelper.any(RelNode.class)),
        "Prel.WriterPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final WriterRel writer = call.rel(0);
    final RelNode input = call.rel(1);

    final RelTraitSet requestedTraits = writer.getCreateTableEntry()
        .getOptions()
        .inferTraits(input.getTraitSet(), input.getRowType());
    final RelNode convertedInput = convert(input, requestedTraits);

    if (!new WriteTraitPull(call).go(writer, convertedInput)) {
      call.transformTo(convertWriter(writer, convertedInput));
    }
  }

  private static class WriteTraitPull extends SubsetTransformer<WriterRel, RuntimeException> {

    public WriteTraitPull(RelOptRuleCall call) {
      super(call);
    }

    @Override
    public RelNode convertChild(WriterRel writer, RelNode rel) throws RuntimeException {
      return convertWriter(writer, rel);
    }
  }

  public static Prel createWriter(RelNode relNode, RelDataType rowType, DatasetConfig datasetConfig, CreateTableEntry createTableEntry, Function<RelNode, Prel> finalize) {
    final RelTraitSet requestedTraits = createTableEntry
      .getOptions()
      .inferTraits(relNode.getTraitSet(), rowType);
    final RelNode convertedInput = convert(relNode, requestedTraits);

    return convertWriter(relNode, convertedInput, rowType, datasetConfig, createTableEntry, finalize);
  }

  private static Prel convertWriter(WriterRel writer, RelNode rel) {
    return convertWriter(writer, rel, writer.getExpectedInboundRowType(), null, writer.getCreateTableEntry(), null);
  }

  private static Prel convertWriter(RelNode writer, RelNode rel, RelDataType rowType, DatasetConfig datasetConfig, CreateTableEntry createTableEntry, Function<RelNode, Prel> finalize) {
    final boolean isSingleWriter = createTableEntry.getOptions().isSingleWriter();
    DistributionTrait childDist = rel.getTraitSet().getTrait(DistributionTraitDef.INSTANCE);
    final RelTraitSet traits = writer.getTraitSet()
      .plus(DistributionTrait.SINGLETON)
      .plus(Prel.PHYSICAL);

    // Create the Writer with the child's distribution because the degree of parallelism for the writer
    // should correspond to the number of child minor fragments. The Writer itself is not concerned with
    // the collation of the child.  Note that the Writer's output RowType consists of
    // {fragment_id varchar(255), number_of_records_written bigint} which are very different from the
    // child's output RowType.
    final WriterPrel child = new WriterPrel(writer.getCluster(),
      writer.getTraitSet()
        .plus(isSingleWriter ? DistributionTrait.SINGLETON : childDist)
        .plus(Prel.PHYSICAL),
      isSingleWriter ? convert(rel, traits) : rel, createTableEntry, rowType);

    if (child.getCreateTableEntry()  == null) {
      // we can only rename using file system
      return child;
    }

    final CreateTableEntry fileEntry = child.getCreateTableEntry();

    // first, resolve our children.
    final String finalPath = fileEntry.getLocation();
    final String userName = fileEntry.getUserName();
    final Path finalStructuredPath = Path.of(finalPath);
    final MutablePlugin plugin = fileEntry.getPlugin();

    final String tempPath = PrelUtil.getPlannerSettings(rel.getCluster()).options.getOption(PlannerSettings.WRITER_TEMP_FILE)
      ? finalStructuredPath.getParent().resolve("." + finalStructuredPath.getName()).toString() + "-" + System.currentTimeMillis()
      : null;

    final RelNode newChild = getManifestWriterPrelIfNeeded(
      tempPath == null
        ? child
        : new WriterPrel(child.getCluster(),
        child.getTraitSet(),
        isSingleWriter ? convert(rel, traits) : rel,
        child.getCreateTableEntry().cloneWithNewLocation(tempPath),
        rowType
      ),
      traits, writer, fileEntry, plugin, childDist);
    WriterCommitterPrel writerCommitterPrel = new WriterCommitterPrel(writer.getCluster(), traits, finalize != null ? finalize.apply(newChild) : newChild,
      plugin, tempPath, finalPath, userName, fileEntry, Optional.ofNullable(datasetConfig), false, false);
    return getInsertRowCountPlanIfNeeded(writerCommitterPrel, createTableEntry);
  }

  /***
   * This function is to get Iceberg DML Flow with ManifestWriter. This writer is responsible to consume data files output from ParquetWriter
   * create ManifestFile out of it and then send to Writer Committer Operator which will commit to iceberg table.
   */
  public static RelNode getManifestWriterPrelIfNeeded(RelNode child, RelTraitSet oldTraits, RelNode writer, CreateTableEntry fileEntry, MutablePlugin plugin,
                                                      DistributionTrait childDist) {
    //For OPTIMIZE TABLE command IcebergManifestWriterPrel is not required.
    if(fileEntry.getIcebergTableProps() == null || fileEntry.getIcebergTableProps().getIcebergOpType() == IcebergCommandType.OPTIMIZE) {
      return convert(child, oldTraits);
    } else {
      DistributionTrait.DistributionField distributionField = new DistributionTrait.DistributionField(RecordWriter.SCHEMA.getFields().indexOf(RecordWriter.ICEBERG_METADATA));
      DistributionTrait distributionTrait = new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED, ImmutableList.of(distributionField));
      final RelTraitSet newTraits = writer.getTraitSet()
        .plus(distributionTrait)
        .plus(Prel.PHYSICAL);

      final RelNode newChild = new HashToRandomExchangePrel(
        child.getCluster(),
        newTraits,
        child,
        distributionTrait.getFields(),
        HashPrelUtil.DATA_FILE_DISTRIBUTE_HASH_FUNCTION_NAME,
        null);

      CreateTableEntry icebergCreateTableEntry = getCreateTableEntryForManifestWriter(fileEntry, plugin, fileEntry.getIcebergTableProps().getFullSchema(), fileEntry.getIcebergTableProps());
      final WriterPrel manifestWriterPrel = new IcebergManifestWriterPrel(writer.getCluster(),
        writer.getTraitSet()
          .plus(childDist)
          .plus(Prel.PHYSICAL),
        newChild, icebergCreateTableEntry);
      return convert(manifestWriterPrel, oldTraits);
    }
  }

  private static Prel getInsertRowCountPlanIfNeeded(Prel relNode, CreateTableEntry createTableEntry) {
    //if not insert op return original plan
    if (!DmlUtils.isInsertOperation(createTableEntry)) {
      return relNode;
    }
    //Return as plan for insert command only with records columns. Same is not applicable in case of incremental reflections refresh
    RelDataTypeField recordsField  = relNode.getRowType()
      .getField(RecordWriter.RECORDS.getName(), false, false);
    AggregateCall aggRowCount = AggregateCall.create(SqlStdOperatorTable.SUM, false, false, ImmutableList.of(recordsField.getIndex()),
      -1, 0, relNode, null, RecordWriter.RECORDS.getName());

    try {
      RexBuilder rexBuilder = relNode.getCluster().getRexBuilder();
      StreamAggPrel rowCountAgg = StreamAggPrel.create(
        relNode.getCluster(),
        relNode.getTraitSet(),
        relNode,
        ImmutableBitSet.of(),
        ImmutableList.of(),
        ImmutableList.of(aggRowCount),
        null);
      // Project: return 0 as row count in case there is no Agg record (i.e., no DMLed results)
      recordsField  = rowCountAgg.getRowType()
        .getField(RecordWriter.RECORDS.getName(), false, false);
      List<String> projectNames = ImmutableList.of(recordsField.getName());
      RexNode zeroLiteral = rexBuilder.makeLiteral(0, rowCountAgg.getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER), true);
      // check if the count of row count records is 0 (i.e., records column is null)
      RexNode rowCountRecordExistsCheckCondition = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL,
        rexBuilder.makeInputRef(recordsField.getType(), recordsField.getIndex()));
      // case when the count of row count records is 0, return 0, else return aggregated row count
      RexNode projectExpr = rexBuilder.makeCall(SqlStdOperatorTable.CASE,
        rowCountRecordExistsCheckCondition, zeroLiteral,
        rexBuilder.makeInputRef(recordsField.getType(), recordsField.getIndex()));
      List<RexNode> projectExprs = ImmutableList.of(projectExpr);

      RelDataType projectRowType = RexUtil.createStructType(rowCountAgg.getCluster().getTypeFactory(), projectExprs,
        projectNames, null);
      return ProjectPrel.create(
        rowCountAgg.getCluster(),
        rowCountAgg.getTraitSet(),
        rowCountAgg,
        projectExprs,
        projectRowType);
    } catch (InvalidRelException e) {
      throw UserException.planError(e).buildSilently();
    }
  }

  public static CreateTableEntry getCreateTableEntryForManifestWriter(CreateTableEntry fileEntry, MutablePlugin plugin, BatchSchema writeTableSchema, IcebergTableProps icebergTableProps) {
    WriterOptions oldOptions = fileEntry.getOptions();
    IcebergWriterOptions icebergOptions = new ImmutableIcebergWriterOptions.Builder()
      .setIcebergTableProps(icebergTableProps).build();
    TableFormatWriterOptions tableFormatOptions = new ImmutableTableFormatWriterOptions.Builder()
      .setIcebergSpecificOptions(icebergOptions).build();
    WriterOptions manifestWriterOption = new WriterOptions(null, null, null, null,
      null, false, oldOptions.getRecordLimit(), tableFormatOptions, oldOptions.getExtendedProperty(), false);
    // IcebergTableProps is the only obj we need in manifestWriter
    return fileEntry.cloneWithFields(manifestWriterOption);
  }
}
