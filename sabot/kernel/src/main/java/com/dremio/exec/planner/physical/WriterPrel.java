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

import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.TableFormatWriterOptions;
import com.dremio.exec.planner.common.WriterRelBase;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.store.RecordWriter;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

@Options
public class WriterPrel extends WriterRelBase implements Prel {

  public static final LongValidator RESERVE =
      new PositiveLongValidator("planner.op.writer.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final LongValidator LIMIT =
      new PositiveLongValidator("planner.op.writer.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);

  public static final String PARTITION_COMPARATOR_FIELD = "P_A_R_T_I_T_I_O_N_C_O_M_P_A_R_A_T_O_R";
  public static final String BUCKET_NUMBER_FIELD = "P_A_R_T_I_T_I_O_N_N_U_M_B_E_R";
  public static final String PARTITION_COMPARATOR_FUNC = "newPartitionValue";

  private final RelDataType expectedInboundRowType;
  private final TableFormatWriterOptions tableFormatWriterOptions;

  public WriterPrel(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      CreateTableEntry createTableEntry,
      RelDataType expectedInboundRowType) {
    super(Prel.PHYSICAL, cluster, traits, child, createTableEntry);
    this.expectedInboundRowType = expectedInboundRowType;
    this.tableFormatWriterOptions = createTableEntry.getOptions().getTableFormatOptions();
  }

  @Override
  public WriterPrel copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new WriterPrel(
        getCluster(), traitSet, sole(inputs), getCreateTableEntry(), expectedInboundRowType);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    PhysicalOperator child = ((Prel) this.getInput()).getPhysicalOperator(creator);

    return getCreateTableEntry()
        .getWriter(
            creator.props(this, creator.getContext().getQueryUserName(), RecordWriter.SCHEMA),
            child);
  }

  @Override
  public Iterator<Prel> iterator() {
    return PrelUtil.iter(getInput());
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value)
      throws E {
    return logicalVisitor.visitWriter(this, value);
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.DEFAULT;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return true;
  }

  public RelDataType getExpectedInboundRowType() {
    return expectedInboundRowType;
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    double estimateInputRowCount = super.estimateRowCount(mq); // Count estimate from input
    BatchSchema schema = CalciteArrowHelper.fromCalciteRowType(getExpectedInboundRowType());

    long parquetFileSize =
        PrelUtil.getPlannerSettings(getCluster().getPlanner())
            .getOptions()
            .getOption(ExecConstants.PARQUET_SPLIT_SIZE_VALIDATOR);
    int listEstimate =
        (int)
            PrelUtil.getPlannerSettings(getCluster().getPlanner())
                .getOptions()
                .getOption(ExecConstants.BATCH_LIST_SIZE_ESTIMATE);
    int variableField =
        (int)
            PrelUtil.getPlannerSettings(getCluster().getPlanner())
                .getOptions()
                .getOption(ExecConstants.BATCH_VARIABLE_FIELD_SIZE_ESTIMATE);
    int estimatedRowSize = schema.estimateRecordSize(listEstimate, variableField);

    double numRecords = Math.ceil(parquetFileSize / estimatedRowSize) + 1;

    return Math.max(10, estimateInputRowCount / numRecords)
        * PrelUtil.getPlannerSettings(getCluster().getPlanner())
            .getOptions()
            .getOption(ExecConstants.PARQUET_FILES_ESTIMATE_SCALING_FACTOR_VALIDATOR);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .itemIf(
            "target_file_size_bytes",
            tableFormatWriterOptions.getTargetFileSize(),
            tableFormatWriterOptions != null
                && tableFormatWriterOptions.getTargetFileSize() != null);
  }
}
