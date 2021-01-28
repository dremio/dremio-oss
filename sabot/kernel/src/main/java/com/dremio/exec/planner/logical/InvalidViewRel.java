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

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql2rel.RelStructuredTypeFlattener;
import org.apache.calcite.sql2rel.RelStructuredTypeFlattener.SelfFlatteningRel;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Joiner;

/**
 * A marker rel node that identifies a VDS as invalid and needed to be refreshed.
 */
public class InvalidViewRel extends SingleRel implements SelfFlatteningRel {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InvalidViewRel.class);
  private static final int MAX_RETRIES = 5;
  private final ViewTable viewTable;

  private InvalidViewRel(ViewTable viewTable, RelNode input) {
    super(input.getCluster(), input.getTraitSet(), input);
    this.rowType = viewTable.getRowType(getCluster().getTypeFactory());
    this.viewTable = viewTable;
  }

  public ViewTable getViewTable() {
    return viewTable;
  }

  /**
   * Adjust the original RelRoot to have a new row type matching what the ViewTable expected so we can complete expansion.
   * @param expandedRoot The original expanded root.
   * @param viewTable The inconsistent view table.
   * @return A new RelRoot that has a InvalidVDSRel node that contains the expanded tree.
   */
  public static RelRoot adjustInvalidRowType(RelRoot expandedRoot, ViewTable viewTable) {
    InvalidViewRel invalid = new InvalidViewRel(viewTable, expandedRoot.rel);
    return RelRoot.of(invalid, invalid.getRowType(), expandedRoot.kind);
  }

  /**
   * Check if the tree has any InvalidVDSRels in it. If so, fix them and throw an appropriate exception.
   * @param rel
   */
  public static void checkForInvalid(Catalog viewCatalog, SqlConverter sqlConverter, RelNode rel) {

    InvalidFinder finder = new InvalidFinder();
    rel.accept(finder);
    if(finder.invalidViews.isEmpty()) {
      return;
    }

    List<String> updated = new ArrayList<>();
    List<String> failed = new ArrayList<>();
    List<Exception> suppressed = new ArrayList<>();
    for (ViewTable view : finder.invalidViews) {
      try {
        DremioCatalogReader catalog;
        if(view.getViewOwner() != null) {
          catalog = sqlConverter.getCatalogReader().withSchemaPathAndUser(view.getViewOwner(), view.getView().getWorkspaceSchemaPath());
        } else {
          catalog = sqlConverter.getCatalogReader().withSchemaPath(view.getView().getWorkspaceSchemaPath());
        }
        SqlConverter converter = new SqlConverter(sqlConverter, catalog);
        RelDataType rowType = converter.getValidatedRowType(view.getView().getSql());
        View newView = view.getView().withRowType(rowType);

        int count = 0;
        while (true) {
          boolean concurrentUpdate = false;
          try {
            Catalog updateViewCatalog = viewCatalog;
            if (view.getViewOwner() != null) {
              updateViewCatalog = updateViewCatalog.resolveCatalog(view.getViewOwner());
            }
            updateViewCatalog.updateView(view.getPath(), newView);
          } catch (ConcurrentModificationException ex) {
            concurrentUpdate = true;
            if (count++ >= MAX_RETRIES) {
              throw ex;
            }
            logger.debug("concurrent update", ex);
            // fall-through
          }

          // check the latest view.
          ViewTable latestView = (ViewTable) viewCatalog.getTableNoResolve(view.getPath());
          RelDataType latestViewRowType = latestView.getRowType(sqlConverter.getCluster().getTypeFactory());
          if (equalsRowTypeDeep(latestViewRowType, rowType)) {
            // successfully updated.
            break;
          } else if (!concurrentUpdate) {
            throw new UnableToFixVDSException(view.getPath(), latestViewRowType, rowType);
          }
        }
        updated.add(view.getPath().toString());
      } catch (Exception e) {
        if (e instanceof UnableToFixVDSException) {
          throw UserException.validationError(e).build(logger);
        }
        suppressed.add(new VDSOutOfDate(view.getPath(), e));
        failed.add(view.getPath().toString());
      }
    }

    UserException.Builder builder;
    if(!failed.isEmpty()) {
      builder = UserException.validationError().message("Some virtual datasets are out of date and need to "
          + "be manually updated. These include: %s. Please correct any issues and rerun this query.", Joiner.on(", ")
          .join(failed));
    } else {
      builder = UserException.schemaChangeError().message("Some virtual datasets were out of date and have been corrected.");
    }
    for(String f : updated) {
      builder = builder.addContext("updated", f);
    }

    for(String f : failed) {
      builder = builder.addContext("failed", f);
    }

    UserException b = builder.buildSilently();
    if(!suppressed.isEmpty()) {
      for(Exception e : suppressed) {
        b.addSuppressed(e);
      }
    }
    throw b;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new InvalidViewRel(viewTable, inputs.get(0));
  }

  @Override
  public void flattenRel(RelStructuredTypeFlattener flattener) {
    flattener.rewriteGeneric(this);
  }

  /**
   * Visitor that finds InvalidVDSRel nodes.
   */
  private static class InvalidFinder extends StatelessRelShuttleImpl {

    private final LinkedHashSet<ViewTable> invalidViews = new LinkedHashSet<>();

    @Override
    public RelNode visit(RelNode other) {
      RelNode visit = super.visit(other);
      // do bottom up.
      if (other instanceof InvalidViewRel) {
        InvalidViewRel ivr = (InvalidViewRel) other;
        invalidViews.add(ivr.viewTable);
      }
      return visit;
    }

  }

  /**
   * Exception to capture what VDS failed.
   */
  @SuppressWarnings("serial")
  private static class VDSOutOfDate extends RuntimeException {

    public VDSOutOfDate(NamespaceKey key, Throwable cause) {
      super("Unable to correct out of date vds " + key.toString(), cause);
    }

  }

  private static class UnableToFixVDSException extends RuntimeException {

    public UnableToFixVDSException(NamespaceKey key, RelDataType viewRowType, RelDataType validatedRowType) {
      super(String.format("Attempted to update view/vds but result was consistent with initial construction." +
        "\n\tTable Path: %s\n\tView: %s\n\tValidated: %s", key, viewRowType, validatedRowType));
    }

  }

  /**
   * Compare row types using deep equality.
   * @param r1 RowType 1
   * @param r2 RowType 2
   * @return True if field types and names are equal, even if the top level type is not identity equal.
   */
  public static boolean equalsRowTypeDeep(RelDataType r1, RelDataType r2) {
    // Note that we compare names separately because there is an issue with record type canonicalization. See DX-17725
    return RelOptUtil.areRowTypesEqual(r1, r2, false) && Objects.deepEquals(r1.getFieldNames(), r2.getFieldNames());
  }
}
