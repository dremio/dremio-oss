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

import static com.dremio.exec.planner.sql.handlers.query.DataAdditionCmdHandler.refreshDataset;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.Pair;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.calcite.logical.TableModifyCrel;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.ScreenRel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.parser.DmlUtils;
import com.dremio.exec.planner.sql.parser.SqlDmlOperator;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.annotations.VisibleForTesting;

/**
 * An abstract class to be extended by classes that will handle future DML operations.
 * The current plan is to have DELETE, MERGE, and UPDATE to extend this class.
 *
 * Eventually, refactor DataAdditionCmdHandler such that INSERT and CTAS can be handled
 * through this abstract class as well. There are lots of shared code once we get to
 * implementing `getPlan`. Lots of those shared code can probably go into a utility
 * class or this base method.
 */
public abstract class DmlHandler implements SqlToPlanHandler {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DmlHandler.class);

  /**
   * Given a SqlNode, returns the target table's path. For example, SqlIdentifier::names
   * for the table the query is targeting.
   *
   * @param sqlNode node representing the query.
   * @return NamespaceKey of the target table's path.
   */
  @VisibleForTesting
  public abstract NamespaceKey getTargetTablePath(SqlNode sqlNode) throws Exception;

  /**
   * SqlOperator for the DML being handled.
   */
  protected abstract SqlOperator getSqlOperator();

  /**
   * Validate the privileges that are needed to run the query.
   */
  protected abstract void validatePrivileges(Catalog catalog, NamespaceKey path, SqlNode sqlNode) throws Exception;


  @Override
  public PhysicalPlan getPlan(SqlHandlerConfig config, String sql, SqlNode sqlNode) throws Exception {
    try {
      final Catalog catalog = config.getContext().getCatalog();
      final NamespaceKey path = DmlUtils.getTablePath(catalog, getTargetTablePath(sqlNode));
      validateDmlRequest(catalog, config, path, getSqlOperator());
      // We should put validate privileges after validating dml requests.
      validatePrivileges(catalog, path, sqlNode);

      final Prel prel = getNonPhysicalPlan(catalog, config, sqlNode, path);
      final PhysicalOperator pop = PrelTransformer.convertToPop(config, prel);
      Runnable committer = !CatalogUtil.requestedPluginSupportsVersionedTables(path, catalog)
        ? () -> refreshDataset(catalog, path, false)
        : null;
      // cleaner will call refreshDataset to avoid the issues like DX-49928
      Runnable cleaner = committer;
      // Metadata for non-versioned plugins happens via this call back. For versioned tables (currently
      // only applies to Nessie), the metadata update happens during the operation within NessieClientImpl).
      return PrelTransformer.convertToPlan(config, pop, committer, cleaner);
    } catch(Exception e){
      throw SqlExceptionHelper.coerceException(logger, sql, e, true);
    }
  }

  @VisibleForTesting
  public Prel getNonPhysicalPlan(Catalog catalog, SqlHandlerConfig config, SqlNode sqlNode, NamespaceKey path) throws Exception{
    // Extends sqlNode's DML target table with system columns (e.g., file_name and row_index)
    SqlDmlOperator sqlDmlOperator = SqlNodeUtil.unwrap(sqlNode, SqlDmlOperator.class);
    sqlDmlOperator.extendTableWithDataFileSystemColumns();

    final ConvertedRelNode convertedRelNode = PrelTransformer.validateAndConvert(config, sqlNode);

    final RelNode relNode = convertedRelNode.getConvertedNode();

    final Rel drel = convertToDrel(config, sqlNode, path, catalog, relNode);
    final Pair<Prel, String> prelAndTextPlan = PrelTransformer.convertToPrel(config, drel);

    return prelAndTextPlan.getKey();
  }

  @VisibleForTesting
  public static void validateDmlRequest(Catalog catalog, SqlHandlerConfig config, NamespaceKey path, SqlOperator sqlOperator) {
    if (!config.getContext().getOptions().getOption(ExecConstants.ENABLE_ICEBERG_ADVANCED_DML)) {
      throw UserException.unsupportedError()
        .message("%s clause is not supported in the query for this source", sqlOperator)
        .buildSilently();
    }

    IcebergUtils.checkTableExistenceAndMutability(catalog, config, path, sqlOperator, false);
    blockDMLForMapTables(catalog, path);
  }

  static void blockDMLForMapTables(Catalog catalog, NamespaceKey path) {
    DremioTable table = catalog.getTableNoResolve(path);
    if (table.getSchema().getFields().stream().anyMatch(DmlHandler::fieldIsMapOrContainsMap)) {
      throw UserException.unsupportedError()
        .message("DML operations on tables with MAP columns is not yet supported.")
        .buildSilently();
    }
  }

  static boolean fieldIsMapOrContainsMap(Field field) {
    if (field.getType().getTypeID() == ArrowType.ArrowTypeID.Map) {
      return true;
    }
    return field.getChildren().stream().anyMatch(DmlHandler::fieldIsMapOrContainsMap);
  }

  private Rel convertToDrel(SqlHandlerConfig config, SqlNode sqlNode, NamespaceKey path, Catalog catalog, RelNode relNode) throws Exception {
    // Allow TableModifyCrel to access CreateTableEntry that can only be created now.
    CreateTableEntry createTableEntry = IcebergUtils.getIcebergCreateTableEntry(config, catalog,
      catalog.getTable(path), getSqlOperator().getKind());
    Rel convertedRelNode = PrelTransformer.convertToDrel(config, rewriteTableModifyCrel(relNode, createTableEntry));

    // below is for results to be returned to client - delete/update/merge operation summary output
    convertedRelNode = SqlHandlerUtil.storeQueryResultsIfNeeded(config.getConverter().getParserConfig(),
      config.getContext(), convertedRelNode);

    return new ScreenRel(convertedRelNode.getCluster(), convertedRelNode.getTraitSet(), convertedRelNode);
  }

  @Override
  public String getTextPlan() {
    throw new UnsupportedOperationException();
  }

  private static RelNode rewriteTableModifyCrel(RelNode relNode, CreateTableEntry createTableEntry) {
    return TableModifyScanCrelSubstitutionRewriter.disableScanCrelSubstitution(
      TableModifyCrelInputRewriter.castIfRequired(
        TableModifyCrelCreateTableEntryApplier.apply(relNode, createTableEntry)));
  }

  /**
   * Visitor which adds CreateTableEntry to TableModifyCrel.
   */
  private static class TableModifyCrelCreateTableEntryApplier extends StatelessRelShuttleImpl {

    private final CreateTableEntry createTableEntry;

    public static RelNode apply(RelNode relNode, CreateTableEntry createTableEntry) {
      TableModifyCrelCreateTableEntryApplier applier = new TableModifyCrelCreateTableEntryApplier(createTableEntry);
      return applier.visit(relNode);
    }

    private TableModifyCrelCreateTableEntryApplier(CreateTableEntry createTableEntry) {
      this.createTableEntry = createTableEntry;
    }

    @Override
    public RelNode visit(RelNode other) {
      if (other instanceof TableModifyCrel) {
        other = ((TableModifyCrel) other).createWith(createTableEntry);
      }

      return super.visit(other);
    }
  }

  /**
   * Visitor which disable substitution of TableModifyCrel's target table ScanCrel. Thus, reflections wont be used
   */
  public static class TableModifyScanCrelSubstitutionRewriter extends StatelessRelShuttleImpl {
    private NamespaceKey targetTableName = null;

    public static RelNode disableScanCrelSubstitution(RelNode root) {
      TableModifyScanCrelSubstitutionRewriter rewriter = new TableModifyScanCrelSubstitutionRewriter();
      return rewriter.visit(root);
    }

    @Override
    public RelNode visit(RelNode other) {
      if (other instanceof TableModifyCrel) {
        targetTableName = ((DremioPrepareTable) other.getTable()).getTable().getPath();
      }

      RelNode ret = super.visit(other);
      if (other instanceof TableModifyCrel) {
        targetTableName = null;
      }
      return ret;
    }

    @Override
    public RelNode visit(TableScan scan) {
      if (!(scan instanceof ScanCrel)
        || targetTableName == null  // we only care the ScanCrels inside TableModifyCrel
        || !((ScanCrel) scan).getTableMetadata().getName().equals(targetTableName)) {
        return super.visit(scan);
      }

      ScanCrel oldScan =  ((ScanCrel) scan);
      // disable reflection by set ScanCrel's 'isSubstitutable' false
      ScanCrel newScan = new ScanCrel(
        oldScan.getCluster(),
        oldScan.getTraitSet(),
        oldScan.getPluginId(),
        oldScan.getTableMetadata(),
        oldScan.getProjectedColumns(),
        oldScan.getObservedRowcountAdjustment(),
        oldScan.isDirectNamespaceDescendent(),
        false);
      return super.visit(newScan);
    }
  }

  /**
   * When the input row types doesn't match what we expect (with respect to the target table + the system columns,
   * we add an explicit cast and make the DML work. I.e.,
   *  UPDATE table SET float_column = (a query that results in DECIMAL) -> We cast the query result to FLOAT
   */
  private static class TableModifyCrelInputRewriter extends StatelessRelShuttleImpl {

    public static RelNode castIfRequired(RelNode root) {
      return new TableModifyCrelInputRewriter().visit(root);
    }

    @Override
    public RelNode visit(RelNode other) {
      if (other instanceof TableModifyCrel) {
        final TableModifyCrel tableModifyCrel = (TableModifyCrel) other;
        final RelDataType expectedInputRowType = tableModifyCrel.getExpectedInputRowType();
        final RelNode input = tableModifyCrel.getInput();
        other = MoreRelOptUtil.areDataTypesEqual(expectedInputRowType, input.getRowType(), false)
          ? other
          : tableModifyCrel.createWith(MoreRelOptUtil.createCastRel(input, expectedInputRowType));
      }

      return super.visit(other);
    }
  }
}
