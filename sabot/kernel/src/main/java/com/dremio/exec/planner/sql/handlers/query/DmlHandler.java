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
package com.dremio.exec.planner.sql.handlers.query;

import static com.dremio.exec.physical.config.TableFunctionConfig.FunctionType.DATA_FILE_SCAN;
import static com.dremio.exec.planner.physical.visitor.WriterUpdater.getCollation;
import static com.dremio.exec.planner.sql.handlers.query.DataAdditionCmdHandler.refreshDataset;
import static com.dremio.exec.planner.sql.parser.DmlUtils.resolveVersionContextForDml;
import static com.dremio.exec.util.ColumnUtils.DML_SYSTEM_COLUMNS;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.catalog.model.dataset.TableVersionType;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.ops.PlannerCatalog;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.ScreenRel;
import com.dremio.exec.planner.physical.HashJoinPrel;
import com.dremio.exec.planner.physical.IcebergDmlMergeDuplicateCheckPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.SortPrel;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.explain.PrelSequencer;
import com.dremio.exec.planner.physical.relbuilder.PrelBuilderFactory;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.DrelTransformer;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.SqlToRelTransformer;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.parser.DmlUtils;
import com.dremio.exec.planner.sql.parser.DremioHint;
import com.dremio.exec.planner.sql.parser.SqlDeleteFromTable;
import com.dremio.exec.planner.sql.parser.SqlDmlOperator;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.options.OptionValue;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.Pair;
import org.apache.iceberg.RowLevelOperationMode;

/**
 * An abstract class to be extended by classes that will handle future DML operations. The current
 * plan is to have DELETE, MERGE, and UPDATE to extend this class.
 *
 * <p>Eventually, refactor DataAdditionCmdHandler such that INSERT and CTAS can be handled through
 * this abstract class as well. There are lots of shared code once we get to implementing `getPlan`.
 * Lots of those shared code can probably go into a utility class or this base method.
 */
public abstract class DmlHandler extends TableManagementHandler {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(DmlHandler.class);

  private String textPlan;
  private Rel drel;
  private Prel prel;
  private Supplier<DremioTable> dremioTable = null;
  private RowLevelOperationMode dmlWriteMode;

  @Override
  void checkValidations(
      Catalog catalog, SqlHandlerConfig config, NamespaceKey path, SqlNode sqlNode)
      throws Exception {
    CatalogEntityKey key =
        CatalogEntityKey.namespaceKeyToCatalogEntityKey(
            path, DmlUtils.getVersionContext((SqlDmlOperator) sqlNode));
    validateDmlRequest(catalog, config, key, getSqlOperator());
    validatePrivileges(catalog, key, sqlNode);
  }

  @VisibleForTesting
  @Override
  public PhysicalPlan getPlan(
      SqlHandlerConfig config, String sql, SqlNode sqlNode, NamespaceKey path) throws Exception {
    try {
      DremioTable table =
          dremioTableSupplier(
                  config.getContext().getCatalog(),
                  CatalogEntityKey.namespaceKeyToCatalogEntityKey(
                      path, DmlUtils.getVersionContext((SqlDmlOperator) sqlNode)))
              .get();
      dmlWriteMode = getRowLevelOperationMode(table);

      if (!config
              .getContext()
              .getOptions()
              .getOption(ExecConstants.ENABLE_ICEBERG_POSITIONAL_DELETE_WRITER)
          && dmlWriteMode == RowLevelOperationMode.MERGE_ON_READ) {
        throw UserException.unsupportedError()
            .message(
                String.format(
                    "The target iceberg table's "
                        + "write.%s.mode table-property is set to 'merge-on-read', "
                        + "but dremio does not support this write property at this time. "
                        + "Please alter your write.%s.mode table property to 'copy-on-write' to proceed.",
                    getSqlOperator().kind.toString().toLowerCase(),
                    getSqlOperator().kind.toString().toLowerCase()))
            .buildSilently();
      }

      // Extends sqlNode's DML target table with system columns (e.g., file_name and row_index)
      SqlDmlOperator sqlDmlOperator = SqlNodeUtil.unwrap(sqlNode, SqlDmlOperator.class);
      sqlDmlOperator.setDmlWriteMode(dmlWriteMode);
      sqlDmlOperator.extendTableWithDataFileSystemColumns();
      if (dmlWriteMode == RowLevelOperationMode.MERGE_ON_READ
          && sqlNode.getKind() == SqlKind.DELETE) {
        ((SqlDeleteFromTable) sqlDmlOperator)
            .setPartitionColumns(
                table.getDatasetConfig().getReadDefinition().getPartitionColumnsList());
      }

      // Prohibit reflections on DML operations.
      config
          .getContext()
          .getOptions()
          .setOption(
              OptionValue.createBoolean(
                  OptionValue.OptionType.QUERY,
                  DremioHint.NO_REFLECTIONS.getOption().getOptionName(),
                  true));

      final ConvertedRelNode convertedRelNode =
          SqlToRelTransformer.validateAndConvertForDml(config, sqlNode, path.getRoot());
      final PlannerCatalog catalog = config.getConverter().getPlannerCatalog();
      final RelNode relNode = convertedRelNode.getConvertedNode();

      validateDmlVersioning(config.getContext().getCatalog(), sqlDmlOperator, path);

      drel = convertToDrel(config, sqlNode, path, catalog, relNode);
      final Pair<Prel, String> prelAndTextPlan = PrelTransformer.convertToPrel(config, drel);

      textPlan = prelAndTextPlan.getValue();
      prel = prelAndTextPlan.getKey();

      prel = enforceOrderBySystemColumnsForMorDuplicateCheck(prel, sqlDmlOperator, path);

      final PhysicalOperator pop = PrelTransformer.convertToPop(config, prel);
      final String sourceName = path.getRoot();
      final ResolvedVersionContext version =
          resolveVersionContextForDml(config, sqlDmlOperator, sourceName);
      CatalogUtil.validateResolvedVersionIsBranch(version);
      Runnable committer =
          !CatalogUtil.requestedPluginSupportsVersionedTables(
                  path, config.getContext().getCatalog())
              ? () -> refreshDataset(config.getContext().getCatalog(), path, false)
              : null;
      // cleaner will call refreshDataset to avoid the issues like DX-49928
      Runnable cleaner = committer;
      // Metadata for non-versioned plugins happens via this call back. For versioned tables
      // (currently
      // only applies to Nessie), the metadata update happens during the operation within
      // NessieClientImpl).
      return PrelTransformer.convertToPlan(config, pop, committer, cleaner);
    } catch (CalciteContextException ex) {
      SqlDmlOperator sqlDmlOperator = SqlNodeUtil.unwrap(sqlNode, SqlDmlOperator.class);
      if (sqlDmlOperator.getSqlTableVersionSpec().getTableVersionSpec().getTableVersionType()
          != TableVersionType.NOT_SPECIFIED) {
        throw UserException.validationError(ex.getCause())
            .message("Version context for source table must be specified using AT SQL syntax")
            .buildSilently();
      }
    } catch (Exception e) {
      throw SqlExceptionHelper.coerceException(logger, sql, e, true);
    }
    return null;
  }

  @VisibleForTesting
  public Prel getPrel() {
    return prel;
  }

  @VisibleForTesting
  public static void validateDmlRequest(
      Catalog catalog,
      SqlHandlerConfig config,
      CatalogEntityKey catalogEntityKey,
      SqlOperator sqlOperator) {
    IcebergUtils.checkTableExistenceAndMutability(
        catalog, config, catalogEntityKey, sqlOperator, true);
  }

  @Override
  protected Rel convertToDrel(
      SqlHandlerConfig config,
      SqlNode sqlNode,
      NamespaceKey path,
      PlannerCatalog catalog,
      RelNode relNode)
      throws Exception {
    // Allow TableModifyCrel to access CreateTableEntry that can only be created now.
    SqlDmlOperator dmlOperator = (SqlDmlOperator) sqlNode;
    String sourceName = path.getRoot();
    TableVersionContext tableVersionContext = DmlUtils.getVersionContext(dmlOperator);
    CatalogEntityKey catalogEntityKey =
        CatalogEntityKey.newBuilder()
            .keyComponents(path.getPathComponents())
            .tableVersionContext(tableVersionContext)
            .build();
    DremioTable table =
        tableVersionContext.getType() == TableVersionType.NOT_SPECIFIED
            ? catalog.getTableWithSchema(path)
            : catalog.getTableWithSchema(catalogEntityKey);
    final ResolvedVersionContext version =
        resolveVersionContextForDml(config, dmlOperator, sourceName);
    CreateTableEntry createTableEntry =
        IcebergUtils.getIcebergCreateTableEntry(
            config, config.getContext().getCatalog(), table, getSqlOperator(), null, version);

    WriterOptions options = createTableEntry.getOptions();
    if (checkIfRowSplitterNeeded(dmlWriteMode, options, sqlNode)) {
      options.enableMergeOnReadRowSplitterMode();
    }

    Rel convertedRelNode =
        DrelTransformer.convertToDrel(config, createTableEntryShuttle(relNode, createTableEntry));

    // below is for results to be returned to client - delete/update/merge operation summary output
    convertedRelNode =
        SqlHandlerUtil.storeQueryResultsIfNeeded(
            config.getConverter().getParserConfig(), config.getContext(), convertedRelNode);

    return new ScreenRel(
        convertedRelNode.getCluster(), convertedRelNode.getTraitSet(), convertedRelNode);
  }

  @Override
  public String getTextPlan() {
    return textPlan;
  }

  @Override
  public Rel getLogicalPlan() {
    return drel;
  }

  protected Supplier<DremioTable> dremioTableSupplier(
      Catalog catalog, CatalogEntityKey catalogEntityKey) {
    if (dremioTable == null) {
      dremioTable = Suppliers.memoize(() -> catalog.getTable(catalogEntityKey));
    }
    return dremioTable;
  }

  /** Gets the Iceberg DML write mode for the given table based on the table properties */
  protected abstract RowLevelOperationMode getRowLevelOperationMode(DremioTable table);

  private void validateDmlVersioning(
      Catalog catalog, SqlDmlOperator sqlDmlOperator, NamespaceKey targetTableKey) {
    if (sqlDmlOperator.getSqlTableVersionSpec().getTableVersionSpec().getTableVersionType()
        == TableVersionType.NOT_SPECIFIED) {
      // No need to validate source table when there's no version for target table.
      return;
    }
    Stream<DremioTable> requestedTables =
        StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(
                catalog.getAllRequestedTables().iterator(), Spliterator.ORDERED),
            false);
    requestedTables.forEach(
        (requestedTable) -> {
          // TODO: DX-85701
          // You don't have to specify version for non-versioned sources.
          if (catalog
                  .getSource(requestedTable.getPath().getRoot())
                  .isWrapperFor(VersionedPlugin.class)
              && !requestedTable.hasAtSpecifier()
              && !requestedTable.getDataset().getName().equals(targetTableKey)) {
            throw UserException.validationError()
                .message(
                    String.format(
                        "When specifying the version of the table to be modified, you must also specify the version of the source table %s using AT SQL syntax.",
                        requestedTable.getDataset().getName()))
                .buildSilently();
          }
        });
  }

  /**
   * Check if Merge-On-Read Merge DML plan will require the row-splitter.
   *
   * <p>see {@link com.dremio.exec.store.iceberg.IcebergMergeOnReadRowSplitterTableFunction} for
   * more details on row splitter and its qualifications.
   *
   * @param options Writer options
   * @param sqlCall Dml call
   * @return True if row splitter is needed, false otherwise
   */
  protected boolean checkIfRowSplitterNeeded(
      RowLevelOperationMode dmlWriteMode, WriterOptions options, SqlNode sqlCall) {
    return false;
  }

  protected abstract void validatePrivileges(
      Catalog catalog, CatalogEntityKey path, SqlNode sqlNode) throws Exception;

  /***
   * 'DuplicateCheck TableFunction' relies on an essential assumption: the incoming input rows are sorted (grouped) by filePath, RowIndex.
   * This function enforce the order by system columns to
   * satisfy 'DuplicateCheck TableFunction's requirement
   */
  private Prel enforceOrderBySystemColumnsForMorDuplicateCheck(
      Prel prel, SqlDmlOperator sqlDmlOperator, NamespaceKey targetTableName) {
    if (!sqlDmlOperator.getDmlWriteMode().equals(RowLevelOperationMode.MERGE_ON_READ)) {
      return prel;
    }

    // only if there is a DuplicateCheckTableFunction available
    if (!DuplicateCheckTableFunctionFinder.find(prel)) {
      return prel;
    }

    boolean planIsChanged = false;

    // check if it is a hash join and the target table is on the left(probe) side
    // swap the sides if necessary
    HashJoinOrderSwapper swapper = new HashJoinOrderSwapper(targetTableName);
    Prel newPrel = (Prel) swapper.visit(prel);
    if (swapper.isSwapped()) {
      planIsChanged = true;
    }

    // the target table is not on the left side, but we could not do swap because
    // 1 it is not a HashJoin, or
    // 2 there are nested joins on the right
    // We would add a sort by filePath/rowIndex so that IcebergDmlMergeDuplicateCheckPrel can work
    if (!swapper.isNoNeedToSwap() && !swapper.isSwapped()) {
      newPrel = (Prel) SystemColumnsSorter.addSort(newPrel);
      planIsChanged = true;
    }

    if (planIsChanged) {
      textPlan = PrelSequencer.getPlanText(newPrel, SqlExplainLevel.ALL_ATTRIBUTES);
    }
    return newPrel;
  }

  /***
   * Find DuplicateCheckTableFunction in the plan
   */
  private static class DuplicateCheckTableFunctionFinder extends StatelessRelShuttleImpl {
    private boolean foundDuplicateCheckTF = false;

    public static boolean find(RelNode root) {
      DuplicateCheckTableFunctionFinder finder = new DuplicateCheckTableFunctionFinder();
      finder.visit(root);
      return finder.foundDuplicateCheckTF;
    }

    @Override
    public RelNode visit(RelNode other) {
      if (other instanceof IcebergDmlMergeDuplicateCheckPrel) {
        foundDuplicateCheckTF = true;
        return other;
      }
      return super.visit(other);
    }
  }

  /***
   * Sort by filePath/rowIndex so that IcebergDmlMergeDuplicateCheckPrel can work
   */
  private static class SystemColumnsSorter extends StatelessRelShuttleImpl {
    public static RelNode addSort(RelNode root) {
      SystemColumnsSorter sorter = new SystemColumnsSorter();
      return sorter.visit(root);
    }

    @Override
    public RelNode visit(RelNode other) {
      if (other instanceof IcebergDmlMergeDuplicateCheckPrel) {
        RelNode input = other.getInput(0);
        RelDataTypeField filePathField =
            input.getRowType().getField(ColumnUtils.FILE_PATH_COLUMN_NAME, false, false);
        RelDataTypeField rowIndexField =
            input.getRowType().getField(ColumnUtils.ROW_INDEX_COLUMN_NAME, false, false);

        final RelCollation collation =
            getCollation(
                input.getTraitSet(),
                ImmutableList.of(filePathField.getIndex(), rowIndexField.getIndex()));
        Prel sortPrel =
            SortPrel.create(
                input.getCluster(), input.getTraitSet().plus(collation), input, collation);
        return other.copy(other.getTraitSet(), ImmutableList.of(sortPrel));
      }
      return super.visit(other);
    }
  }

  /***
   * Swap HashJoin sides so that target table is on the left(probe) side of the join
   * This is a preparation step for IcebergDmlMergeDuplicateCheckPrel
   * By putting the target table on the left(probe) side, a single target table row probe should output
   * all source table matches consecutively.
   * We leverage that to use a IcebergDmlMergeDuplicateCheckTableFunction to check if there are consecutive rows
   * having duplicate FilePath/RowIndex
   */
  private static class HashJoinOrderSwapper extends StatelessRelShuttleImpl {
    private enum State {
      SEARCHING_DUPLICATE_CHECK_TF,
      FOUND_DUPLICATE_CHECK_TF,
      CHECKING_TARGET_JOIN,
      QUIT
    }

    private NamespaceKey targetTableName;

    // if it is the right side of the hash join
    private boolean isRightSide = false;

    // target table is already on left side, no need to swap
    private boolean noNeedToSwap = false;
    private boolean shouldSwap = false;

    private State state = State.SEARCHING_DUPLICATE_CHECK_TF;

    public HashJoinOrderSwapper(NamespaceKey targetTableName) {
      super();
      this.targetTableName = targetTableName;
    }

    @Override
    public RelNode visitChildren(RelNode rel) {
      if (state == State.QUIT) {
        return rel;
      }
      return super.visitChildren(rel);
    }

    @Override
    public RelNode visit(RelNode other) {
      switch (state) {
        case SEARCHING_DUPLICATE_CHECK_TF:
          if (other instanceof IcebergDmlMergeDuplicateCheckPrel) {
            state = State.FOUND_DUPLICATE_CHECK_TF;
          }
          break;
        case FOUND_DUPLICATE_CHECK_TF:
          if (other instanceof Join) {
            // it is not a Hashjoin, quit
            if (!(other instanceof HashJoinPrel)) {
              state = State.QUIT;
              break;
            }

            // go to the left
            state = State.CHECKING_TARGET_JOIN;
            ((Join) other).getLeft().accept(this);
            // go to the right
            isRightSide = true;
            ((Join) other).getRight().accept(this);
            state = State.QUIT;

            // swap
            if (shouldSwap) {
              return JoinCommuteRule.swap(
                  ((Join) other),
                  true,
                  PrelBuilderFactory.INSTANCE.create(other.getCluster(), null));
            }
          }
          break;
        case CHECKING_TARGET_JOIN:
          // we already find the target join, seeing another join means nested join. quit
          if (other instanceof Join) {
            state = State.QUIT;
            break;
          }

          if (other instanceof TableFunctionPrel
              && ((TableFunctionPrel) other)
                  .getTableFunctionConfig()
                  .getType()
                  .equals(DATA_FILE_SCAN)) {
            // target able is already on left side
            if (!isRightSide && isTargetTable(targetTableName, other.getTable())) {
              noNeedToSwap = true;
              state = State.QUIT;
            }

            // target able is on right side of the target join (no nested joins)
            if (isRightSide && isTargetTable(targetTableName, other.getTable())) {
              shouldSwap = true;
            }
          }
          break;
        case QUIT:
          return other;
        default:
          throw new AssertionError("Invalid state: " + state);
      }

      return super.visit(other);
    }

    /***
     * target table should have dml system columns
     */
    private static boolean isTargetTable(NamespaceKey targetTableName, RelOptTable table) {
      return targetTableName.equals(new NamespaceKey(table.getQualifiedName()))
          && table.getRowType().getFieldNames().containsAll(DML_SYSTEM_COLUMNS);
    }

    public boolean isSwapped() {
      return shouldSwap;
    }

    public boolean isNoNeedToSwap() {
      return noNeedToSwap;
    }
  }
}
