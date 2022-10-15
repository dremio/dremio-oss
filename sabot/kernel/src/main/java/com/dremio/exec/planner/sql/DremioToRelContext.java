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

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.commons.lang3.StringUtils;

import com.dremio.common.exceptions.ErrorHelper;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.Describer;
import com.dremio.exec.planner.logical.InvalidViewRel;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.google.common.collect.ImmutableList;

public interface DremioToRelContext {
  org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DremioToRelContext.class);

  interface DremioQueryToRelContext extends ToRelContext{
    SqlValidatorAndToRelContext.Builder getSqlValidatorAndToRelContext();
    RelRoot expandView(ViewTable view);
  }

  interface DremioSerializationToRelContext extends ToRelContext {
  }

  static DremioSerializationToRelContext createSerializationContext(RelOptCluster relOptCluster) {
    return new DremioSerializationToRelContext() {
      @Override
      public RelOptCluster getCluster() {
        return relOptCluster;
      }

      @Override
      public List<RelHint> getTableHints() {
        return ImmutableList.of();
      }

      @Override
      public RelRoot expandView(RelDataType rowType,
        String queryString,
        List<String> schemaPath, List<String> viewPath) {
        throw new UnsupportedOperationException();
      }
    };
  }

  static DremioQueryToRelContext createQueryContext(final SqlConverter sqlConverter) {
    return new DremioQueryToRelContext() {

      public SqlValidatorAndToRelContext.Builder getSqlValidatorAndToRelContext() {
        return SqlValidatorAndToRelContext.builder(sqlConverter);
      }

      @Override
      public RelOptCluster getCluster() {
        return sqlConverter.getCluster();
      }

      @Override
      public List<RelHint> getTableHints() {
        return ImmutableList.of();
      }

      public RelRoot expandView(ViewTable view) {
        final RelRoot root;

        try {
          root = DremioSqlToRelConverter.expandView(view.getPath(),
            view.getViewOwner(),
            view.getView().getSql(),
            view.getView().getWorkspaceSchemaPath(),
            sqlConverter,
            view.getSchema(),
            view.getVersionContext());
        } catch (Exception ex) {
          String message = String.format("Error while expanding view %s. ", view.getPath());
          final SqlValidatorException sve = ErrorHelper.findWrappedCause(ex, SqlValidatorException.class);
          if (sve != null && StringUtils.isNotBlank(sve.getMessage())) {
            // Expose reason why view expansion failed such as specific table or column not found
            message += String.format("%s. Verify the view’s SQL definition.", sve.getMessage());
          } else if (StringUtils.isNotBlank(ex.getMessage())){
            message += String.format("%s", ex.getMessage());
          } else {
            message += "Verify the view’s SQL definition.";
          }

          throw UserException.planError(ex)
            .message(message)
            .addContext("View SQL", view.getView().getSql())
            .build(logger);
        }

        if (sqlConverter.getSettings().getOptions().getOption(PlannerSettings.VDS_AUTO_FIX)  && !view.getView().hasDeclaredFieldNames()) {
          // this functionality only works for views that without externally defined field names. This is consistent with how VDSs are defined. (only legacy views support this)

          if(!InvalidViewRel.equalsRowTypeDeep(root.validatedRowType, view.getView().getRowType(sqlConverter.getCluster().getTypeFactory()))) {
            return InvalidViewRel.adjustInvalidRowType(root, view);
          }
        } else {
          checkRowTypeConsistency(root.validatedRowType, view.getView().getRowType(sqlConverter.getCluster().getTypeFactory()),
            view.getPath().getSchemaPath());
        }

        return root;
      }

      /**
       * Confirm the row type is consistent with that of the validated node.
       * Performs the least restrictive check to confirm the validated field list is not smaller than the view's field list.
       *
       * @param validatedRowType    The validated row type from the RelRoot
       * @param rowType             The row type of the view.
       * @param datasetPath         The path of the dataset being expanded.
       *
       * @throws UserException indicating dataset definition is out of date.
       */
      private void checkRowTypeConsistency(final RelDataType validatedRowType, final RelDataType rowType, String datasetPath) {
        List<RelDataTypeField> rowTypeFieldList = rowType.getFieldList();
        List<RelDataTypeField> validatedFieldList = validatedRowType.getFieldList();

        // Confirm that the validate field list is not smaller than view's field list.
        if (validatedFieldList.size() < rowTypeFieldList.size()) {
          throw UserException.validationError()
            .message(String.format("Definition of this dataset is out of date. There were schema changes in %s.\n",
              datasetPath))
            .addContext("Original", Describer.describe(CalciteArrowHelper.fromCalciteRowType(rowType)))
            .addContext("New", Describer.describe(CalciteArrowHelper.fromCalciteRowType(validatedRowType)))
            .build(logger);
        }
      }

      @Override
      public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
        throw new IllegalStateException("This expander should not be used.");
      }
    };
  }
}
