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
package com.dremio.exec.planner.sql;

import static com.google.common.base.Preconditions.checkNotNull;

import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.exceptions.ErrorHelper;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.CatalogIdentity;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.ops.ViewExpansionContext;
import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.acceleration.ExpansionNode;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionProvider;
import com.dremio.exec.planner.catalog.AutoVDSFixer;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.sql.handlers.SqlToRelTransformer;
import com.dremio.sabot.exec.context.ContextInformation;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.users.SystemUser;
import com.dremio.service.users.UserNotFoundException;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ViewExpander {
  private static final Logger LOGGER = LoggerFactory.getLogger(ViewExpander.class);

  private final SqlValidatorAndToRelContext.BuilderFactory
      sqlValidatorAndToRelContextBuilderFactory;
  private final ViewExpansionContext viewExpansionContext;
  private final SubstitutionProvider substitutionProvider;
  private final AutoVDSFixer viewVersionChecker;

  public ViewExpander(
      SqlValidatorAndToRelContext.BuilderFactory sqlValidatorAndToRelContextBuilderFactory,
      ContextInformation contextInformation,
      ViewExpansionContext viewExpansionContext,
      SubstitutionProvider substitutionProvider,
      AutoVDSFixer viewVersionChecker) {
    this.sqlValidatorAndToRelContextBuilderFactory =
        checkNotNull(
            sqlValidatorAndToRelContextBuilderFactory, "sqlValidatorAndToRelContextBuilderFactory");
    this.viewExpansionContext = checkNotNull(viewExpansionContext, "viewExpansionContext");
    this.substitutionProvider = checkNotNull(substitutionProvider, "substitutionProvider");
    this.viewVersionChecker = checkNotNull(viewVersionChecker, "viewVersionChecker");
  }

  /**
   * This is a here due to the legacy code structure. Their should probably be a common
   * abstraction/class for handling the flow of parse, validation and conversion of SqlToRel. The
   * abstraction could contain code from {@link SqlToRelTransformer} and various Handlers.
   */
  public RelRoot stringToRelRootAsSystemUser(String queryString) {
    final CatalogIdentity viewOwner = new CatalogUser(SystemUser.SYSTEM_USERNAME);
    SqlValidatorAndToRelContext.Builder builder =
        sqlValidatorAndToRelContextBuilderFactory.builder().withUser(viewOwner);

    SqlValidatorAndToRelContext newConverter = builder.build();
    final SqlNode parsedNode = newConverter.parse(queryString);
    final SqlNode validatedNode = newConverter.validate(parsedNode);
    final RelRoot root = newConverter.toConvertibleRelRoot(validatedNode, true);
    return root;
  }

  public RelRoot expandView(ViewTable viewTable) {
    final RelRoot root = expandViewInternalErrorWithHandling(viewTable);

    // After expanding view, check if the view's schema (i.e. cached row type) does not match the
    // validated row type.  If so,
    // then update the view's row type in the catalog so that it is consistent again.  A common
    // example when a view
    // needs to be updated is when a view is defined as a select * on a table and the table's schema
    // changes.
    viewVersionChecker.autoFixVds(viewTable, root);

    return root;
  }

  private RelRoot expandViewInternalErrorWithHandling(final ViewTable viewTable) {

    try {
      return expandViewInternal(viewTable);
    } catch (Exception ex) {
      String message = String.format("Error while expanding view %s. ", viewTable.getPath());
      final SqlValidatorException sve =
          ErrorHelper.findWrappedCause(ex, SqlValidatorException.class);
      if (sve != null && StringUtils.isNotBlank(sve.getMessage())) {
        // Expose reason why view expansion failed such as specific table or column not found
        message += String.format("%s. Verify the view’s SQL definition.", sve.getMessage());
      } else if (StringUtils.isNotBlank(ex.getMessage())) {
        message += String.format("%s", ex.getMessage());
      } else {
        message += "Verify the view’s SQL definition.";
      }

      throw UserException.planError(ex)
          .message(message)
          .addContext("View SQL", viewTable.getView().getSql())
          .build(LOGGER);
    }
  }

  private RelRoot expandViewInternal(final ViewTable viewTable) {
    assert viewTable != null;

    final CatalogIdentity viewOwner = viewTable.getViewOwner();
    final String queryString = viewTable.getView().getSql();

    ViewExpansionContext.ViewExpansionToken token = null;
    try {
      token = viewExpansionContext.reserveViewExpansionToken(viewOwner);
      return expandRelNode(viewTable, viewOwner, queryString);
    } catch (RuntimeException e) {
      if (!(e.getCause() instanceof UserNotFoundException)) {
        throw e;
      }

      final CatalogIdentity delegatedUser = viewExpansionContext.getQueryUser();
      return expandRelNode(viewTable, delegatedUser, queryString);
    } finally {
      if (token != null) {
        token.release();
      }
    }
  }

  private RelRoot expandRelNode(
      final ViewTable viewTable, final CatalogIdentity viewOwner, final String queryString) {
    assert viewTable != null;

    final NamespaceKey viewPath = viewTable.getPath();
    final TableVersionContext versionContext = viewTable.getVersionContext();

    SqlValidatorAndToRelContext.Builder builder =
        sqlValidatorAndToRelContextBuilderFactory.builder();
    if (viewOwner != null) {
      builder = builder.withUser(viewOwner);
    }
    builder = builder.withSchemaPath(viewTable.getView().getWorkspaceSchemaPath());
    if (viewTable.getVersionContext() != null) {
      // Nested views/tables should inherit this version context (unless explicitly overridden by
      // the nested view/table)
      builder = builder.withVersionContext(viewPath.getRoot(), viewTable.getVersionContext());
    }
    SqlValidatorAndToRelContext sqlValidatorAndToRelContext = builder.build();
    final SqlNode parsedNode = sqlValidatorAndToRelContext.parse(queryString);
    final SqlNode validatedNode = sqlValidatorAndToRelContext.validate(parsedNode);
    Optional<RelRoot> defaultReflectionRoot =
        generateDefaultReflectionRelRoot(viewTable, sqlValidatorAndToRelContext, validatedNode);
    if (defaultReflectionRoot.isPresent()) {
      return defaultReflectionRoot.get();
    }

    final RelRoot root = sqlValidatorAndToRelContext.toConvertibleRelRoot(validatedNode, true);
    RelNode expansionNode =
        ExpansionNode.wrap(viewPath, root.rel, root.validatedRowType, false, versionContext);
    return RelRoot.of(expansionNode, root.validatedRowType, root.kind);
  }

  private Optional<RelRoot> generateDefaultReflectionRelRoot(
      final ViewTable viewTable,
      final SqlValidatorAndToRelContext sqlValidatorAndToRelContext,
      final SqlNode validatedNode) {
    assert viewTable != null;

    final NamespaceKey viewPath = viewTable.getPath();
    final TableVersionContext versionContext = viewTable.getVersionContext();

    try {
      Optional<DremioMaterialization> defaultRawMaterialization =
          substitutionProvider.getDefaultRawMaterialization(viewTable);
      if (defaultRawMaterialization.isPresent()) {
        final RelRoot unflattenedRoot =
            sqlValidatorAndToRelContext.toConvertibleRelRoot(validatedNode, false, false);
        final RelNode defaultExpansionNode =
            substitutionProvider.wrapDefaultExpansionNode(
                viewPath,
                unflattenedRoot.rel,
                defaultRawMaterialization.get(),
                unflattenedRoot.validatedRowType,
                versionContext,
                viewExpansionContext);
        viewExpansionContext.setSubstitutedWithDRR();
        RelRoot relRootPlus =
            RelRoot.of(
                defaultExpansionNode, unflattenedRoot.validatedRowType, unflattenedRoot.kind);
        return Optional.of(relRootPlus);
      } else {
        return Optional.empty();
      }
    } catch (RuntimeException e) {
      LOGGER.warn("Unable to get default raw materialization for {}", viewPath.getSchemaPath(), e);
      return Optional.empty();
    }
  }
}
