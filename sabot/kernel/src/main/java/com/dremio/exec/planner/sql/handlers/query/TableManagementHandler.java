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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.calcite.logical.TableModifyCrel;
import com.dremio.exec.calcite.logical.TableOptimizeCrel;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.parser.DmlUtils;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.annotations.VisibleForTesting;

/**
 * Abstraction of the plan building components, within table modification operations, such as DML and OPTIMIZE.
 */
public abstract class TableManagementHandler implements SqlToPlanHandler {

  @VisibleForTesting
  public abstract NamespaceKey getTargetTablePath(SqlNode sqlNode) throws Exception;

  /**
   * Run  {@link #checkValidations(Catalog, SqlHandlerConfig, NamespaceKey, SqlNode)}
   * and return Plan {@link #getPlan(Catalog, SqlHandlerConfig, String, SqlNode, NamespaceKey)}
   */
  @Override
  public final PhysicalPlan getPlan(SqlHandlerConfig config, String sql, SqlNode sqlNode) throws Exception {
    final Catalog catalog = config.getContext().getCatalog();
    final NamespaceKey path = DmlUtils.getTablePath(catalog, getTargetTablePath(sqlNode));
    checkValidations(catalog, config, path, sqlNode);
    return getPlan(catalog, config, sql, sqlNode, path);
  }

  /**
   * Build the physical plan if SQL is valid.
   */
  protected abstract PhysicalPlan getPlan(Catalog catalog, SqlHandlerConfig config, String sql, SqlNode sqlNode, NamespaceKey path) throws Exception;

  /**
   * A single place to do all the SQL validations.
   */
  abstract void checkValidations(Catalog catalog, SqlHandlerConfig config, NamespaceKey path, SqlNode sqlNode) throws Exception ;

  /**
   *
   */
  protected abstract SqlOperator getSqlOperator();

  /**
   * Privileges that are needed to run the query.
   */
  protected abstract void validatePrivileges(Catalog catalog, NamespaceKey path, SqlNode sqlNode) throws Exception;

  protected abstract Rel convertToDrel(SqlHandlerConfig config, SqlNode sqlNode, NamespaceKey path, Catalog catalog, RelNode relNode) throws Exception;

  @Override
  public String getTextPlan() {
    throw new UnsupportedOperationException();
  }

  protected static RelNode rewriteCrel(RelNode relNode, CreateTableEntry createTableEntry) {
    return ScanCrelSubstitutionRewriter.disableScanCrelSubstitution(
      CrelInputRewriter.castIfRequired(
        CrelCreateTableEntryApplier.apply(relNode, createTableEntry)));
  }

  protected static RelNode rewriteCrel(RelNode relNode) {
    return ScanCrelSubstitutionRewriter.disableScanCrelSubstitution(
      CrelInputRewriter.castIfRequired(
        CrelEntryApplier.apply(relNode)));
  }

  private static class CrelCreateTableEntryApplier extends StatelessRelShuttleImpl {

    private final CreateTableEntry createTableEntry;

    private CrelCreateTableEntryApplier(CreateTableEntry createTableEntry) {
      this.createTableEntry = createTableEntry;
    }

    public static RelNode apply(RelNode relNode, CreateTableEntry createTableEntry) {
      CrelCreateTableEntryApplier applier = new CrelCreateTableEntryApplier(createTableEntry);
      return applier.visit(relNode);
    }

    @Override
    public RelNode visit(RelNode other) {
      if (other instanceof TableModifyCrel) {
        other = ((TableModifyCrel) other).createWith(createTableEntry);
      }

      if (other instanceof TableOptimizeCrel) {
        other = ((TableOptimizeCrel) other).createWith(createTableEntry);
      }

      return super.visit(other);
    }
  }

  private static class CrelEntryApplier extends StatelessRelShuttleImpl {

    public static RelNode apply(RelNode relNode) {
      CrelEntryApplier applier = new CrelEntryApplier();
      return applier.visit(relNode);
    }

    @Override
    public RelNode visit(RelNode other) {
      return super.visit(other);
    }
  }

  public static class ScanCrelSubstitutionRewriter extends StatelessRelShuttleImpl {
    private NamespaceKey targetTableName = null;

    public static RelNode disableScanCrelSubstitution(RelNode root) {
      ScanCrelSubstitutionRewriter rewriter = new ScanCrelSubstitutionRewriter();
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
        oldScan.getHints(),
        oldScan.isDirectNamespaceDescendent(),
        false);
      return super.visit(newScan);
    }
  }

  private static class CrelInputRewriter extends StatelessRelShuttleImpl {

    public static RelNode castIfRequired(RelNode root) {
      return new CrelInputRewriter().visit(root);
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
