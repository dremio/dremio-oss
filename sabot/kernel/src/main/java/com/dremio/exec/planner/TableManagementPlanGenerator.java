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
package com.dremio.exec.planner;

import java.util.function.Function;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.TableFunctionUtil;
import com.dremio.exec.planner.physical.UnionAllPrel;
import com.dremio.exec.planner.physical.UnionExchangePrel;
import com.dremio.exec.planner.physical.WriterPrule;
import com.dremio.exec.store.OperationType;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.TableMetadata;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Abstract to generate physical plan for DML and OPTIMIZE
 */
public abstract class TableManagementPlanGenerator {

  protected final RelOptTable table;
  protected final RelOptCluster cluster;
  protected final RelTraitSet traitSet;
  protected final RelNode input;
  protected final TableMetadata tableMetadata;
  protected final CreateTableEntry createTableEntry;
  protected final OptimizerRulesContext context;

  public TableManagementPlanGenerator(RelOptTable table, RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                                      TableMetadata tableMetadata, CreateTableEntry createTableEntry, OptimizerRulesContext context) {
    this.table = Preconditions.checkNotNull(table);
    this.tableMetadata = Preconditions.checkNotNull(tableMetadata, "TableMetadata cannot be null.");
    this.createTableEntry = Preconditions.checkNotNull(createTableEntry, "CreateTableEntry cannot be null.");
    this.context = Preconditions.checkNotNull(context, "Context cannot be null.");
    this.cluster = cluster;
    this.traitSet = traitSet;
    this.input = input;
  }

  public abstract Prel getPlan();

  /**
   *    WriterCommitterPrel
   *        |
   *        |
   *    UnionAllPrel ---------------------------------------------|
   *        |                                                     |
   *        |                                                     |
   *    WriterPrel                                            TableFunctionPrel (DELETED_FILES_METADATA)
   *        |                                                 this converts a path into required IcebergMetadata blob
   *        |                                                     |
   *    (input from copyOnWriteResultsPlan)                       (deleted data files list from dataFileAggrPlan)
   */
  protected Prel getDataWriterPlan(RelNode copyOnWriteResultsPlan, final RelNode dataFileAggrPlan) {
    return WriterPrule.createWriter(
      copyOnWriteResultsPlan,
      copyOnWriteResultsPlan.getRowType(),
      tableMetadata.getDatasetConfig(),
      createTableEntry,
      manifestWriterPlan -> {
        try {
          return getMetadataWriterPlan(dataFileAggrPlan, manifestWriterPlan);
        } catch (InvalidRelException e) {
          throw new RuntimeException(e);
        }
      });
  }

  /**
   *    UnionAllPrel <------ WriterPrel
   *        |
   *        |
   *    UnionAllPrel ---------------------------------------------|
   *        |                                                     |
   *        |                                                     |
   *    TableFunctionPrel (DELETED_FILES_METADATA)            TableFunctionPrel (DELETED_FILES_METADATA)
   *        |                                                 this converts deleteFile paths into required IcebergMetadata blob
   *        |                                                     |
   *    (deleted data files list from dataFileAggrPlan)       (deleted data files list from deleteFileAggrPlan)
   */
  protected Prel getDataWriterPlan(RelNode copyOnWriteResultsPlan, final Function<RelNode, Prel> metadataWriterFunction) {
    return WriterPrule.createWriter(
      copyOnWriteResultsPlan,
      copyOnWriteResultsPlan.getRowType(),
      tableMetadata.getDatasetConfig(),
      createTableEntry,
      metadataWriterFunction);
  }

  private Prel getMetadataWriterPlan(RelNode dataFileAggrPlan, RelNode manifestWriterPlan) throws InvalidRelException {
    // Insert a table function that'll pass the path through and set the OperationType
    TableFunctionPrel deletedFilesTableFunctionPrel = getDeleteFilesMetadataTableFunctionPrel(dataFileAggrPlan,
      getProjectedColumns(), TableFunctionUtil.getDeletedFilesMetadataTableFunctionContext(
        OperationType.DELETE_DATAFILE, RecordWriter.SCHEMA, getProjectedColumns(), true));

    final RelTraitSet traits = traitSet.plus(DistributionTrait.SINGLETON).plus(Prel.PHYSICAL);

    // Union the updating of the deleted data's metadata with the rest
    return getUnionPrel(traits, manifestWriterPlan, deletedFilesTableFunctionPrel);
  }

  protected ImmutableList<SchemaPath> getProjectedColumns() {
    return RecordWriter.SCHEMA.getFields().stream()
      .map(f -> SchemaPath.getSimplePath(f.getName()))
      .collect(ImmutableList.toImmutableList());
  }

  protected TableFunctionPrel getDeleteFilesMetadataTableFunctionPrel(RelNode input, ImmutableList<SchemaPath> projectedCols, TableFunctionContext tableFunctionContext) {
    return new TableFunctionPrel(
      input.getCluster(),
      input.getTraitSet(),
      table,
      input,
      tableMetadata,
      new TableFunctionConfig(
        TableFunctionConfig.FunctionType.DELETED_FILES_METADATA,
        true,
        tableFunctionContext),
      ScanRelBase.getRowTypeFromProjectedColumns(projectedCols,
        RecordWriter.SCHEMA, input.getCluster()));
  }

  protected Prel getUnionPrel(RelTraitSet traits, RelNode manifestWriterPlan, RelNode input) throws InvalidRelException {
    return new UnionAllPrel(cluster,
      traits,
      ImmutableList.of(manifestWriterPlan,
        new UnionExchangePrel(cluster, traits,
          input)),
      false);
  }
}
