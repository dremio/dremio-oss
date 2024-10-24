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

import static com.dremio.exec.planner.sql.handlers.SqlHandlerUtil.PLANNER_SOURCE_TARGET_SOURCE_TYPE_SPAN_ATTRIBUTE_NAME;

import com.dremio.exec.calcite.logical.TableModifyCrel;
import com.dremio.exec.calcite.logical.TableOptimizeCrel;
import com.dremio.exec.calcite.logical.VacuumTableCrel;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.ops.PlannerCatalog;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.direct.DirectHandlerValidator;
import com.dremio.exec.planner.sql.parser.DmlUtils;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;

/**
 * Abstraction of the plan building components, within table modification operations, such as DML
 * and OPTIMIZE.
 */
public abstract class TableManagementHandler implements SqlToPlanHandler, DirectHandlerValidator {

  @VisibleForTesting
  public abstract NamespaceKey getTargetTablePath(SqlNode sqlNode) throws Exception;

  /**
   * Run {@link #checkValidations(Catalog, SqlHandlerConfig, NamespaceKey, SqlNode)} and return Plan
   * {@link #getPlan(SqlHandlerConfig, String, SqlNode, NamespaceKey)}
   */
  @WithSpan
  @Override
  public final PhysicalPlan getPlan(SqlHandlerConfig config, String sql, SqlNode sqlNode)
      throws Exception {
    final Catalog catalog = config.getContext().getCatalog();
    final NamespaceKey path =
        CatalogUtil.getResolvePathForTableManagement(
            config.getContext().getCatalog(),
            getTargetTablePath(sqlNode),
            DmlUtils.getVersionContext(sqlNode));
    Span.current()
        .setAttribute(
            PLANNER_SOURCE_TARGET_SOURCE_TYPE_SPAN_ATTRIBUTE_NAME,
            SqlHandlerUtil.getSourceType(catalog, path.getRoot()));
    checkValidations(catalog, config, path, sqlNode);
    return getPlan(config, sql, sqlNode, path);
  }

  /** Build the physical plan if SQL is valid. */
  protected abstract PhysicalPlan getPlan(
      SqlHandlerConfig config, String sql, SqlNode sqlNode, NamespaceKey path) throws Exception;

  /** A single place to do all the SQL validations. */
  abstract void checkValidations(
      Catalog catalog, SqlHandlerConfig config, NamespaceKey path, SqlNode sqlNode)
      throws Exception;

  /** */
  protected abstract SqlOperator getSqlOperator();

  protected abstract Rel convertToDrel(
      SqlHandlerConfig config,
      SqlNode sqlNode,
      NamespaceKey path,
      PlannerCatalog catalog,
      RelNode relNode)
      throws Exception;

  @Override
  public String getTextPlan() {
    throw new UnsupportedOperationException();
  }

  protected static RelNode createTableEntryShuttle(
      RelNode relNode, CreateTableEntry createTableEntry) {
    return CrelInputRewriter.castIfRequired(
        CrelCreateTableEntryApplier.apply(relNode, createTableEntry));
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

      if (other instanceof VacuumTableCrel) {
        other = ((VacuumTableCrel) other).createWith(createTableEntry);
      }

      return super.visit(other);
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
        other =
            MoreRelOptUtil.areDataTypesEqual(expectedInputRowType, input.getRowType(), false)
                ? other
                : tableModifyCrel.createWith(
                    MoreRelOptUtil.createCastRel(input, expectedInputRowType));
      }

      return super.visit(other);
    }
  }

  protected void validateTableExistenceAndMutability(
      Catalog catalog, SqlHandlerConfig config, NamespaceKey namespaceKey) {
    // Validate table exists and is Iceberg table
    IcebergUtils.checkTableExistenceAndMutability(
        catalog, config, namespaceKey, getSqlOperator(), true);
  }
}
