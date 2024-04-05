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
package com.dremio.exec.planner.sql.parser;

import com.dremio.exec.calcite.logical.VacuumCatalogCrel;
import com.dremio.exec.planner.VacuumOutputSchema;
import com.dremio.exec.planner.sql.handlers.query.SqlToPlanHandler;
import com.dremio.exec.planner.sql.handlers.query.SupportsSqlToRelConversion;
import com.dremio.exec.planner.sql.handlers.query.VacuumCatalogHandler;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

public class SqlVacuumCatalog extends SqlVacuum
    implements SqlToPlanHandler.Creator, SupportsSqlToRelConversion {

  public static final SqlSpecialOperator VACUUM_CATALOG_OPERATOR =
      new SqlSpecialOperator("VACUUM_CATALOG_OPERATOR", SqlKind.OTHER) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          Preconditions.checkArgument(
              operands.length == 1, "SqlVacuumCatalog.createCall() " + "has 1 operand!");
          return new SqlVacuumCatalog(pos, (SqlIdentifier) operands[0]);
        }

        @Override
        public RelDataType deriveType(
            SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
          final RelDataTypeFactory typeFactory = validator.getTypeFactory();
          return VacuumOutputSchema.getCatalogOutputRelDataType(typeFactory);
        }
      };

  private final SqlIdentifier catalogSource;

  /** Creates a SqlVacuum. */
  public SqlVacuumCatalog(SqlParserPos pos, SqlIdentifier catalogSource) {
    super(
        pos,
        SqlLiteral.createBoolean(true, pos),
        SqlLiteral.createBoolean(true, pos),
        SqlNodeList.EMPTY,
        SqlNodeList.EMPTY);
    this.catalogSource = catalogSource;
  }

  @Override
  public SqlOperator getOperator() {
    return VACUUM_CATALOG_OPERATOR;
  }

  public SqlIdentifier getCatalogSource() {
    return catalogSource;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableList.of(catalogSource);
  }

  @Override
  public NamespaceKey getPath() {
    return new NamespaceKey(catalogSource.getSimple());
  }

  @Override
  protected void populateOptions(SqlNodeList optionsList, SqlNodeList optionsValueList) {
    // Nothing to do
  }

  @Override
  public Optional<Consumer<SqlNode>> getOptionsConsumer(String optionName) {
    return Optional.empty();
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("VACUUM");
    writer.keyword("CATALOG");
    catalogSource.unparse(writer, leftPrec, rightPrec);
  }

  @Override
  public SqlToPlanHandler toPlanHandler() {
    return new VacuumCatalogHandler();
  }

  @Override
  public RelNode convertToRel(
      RelOptCluster cluster,
      Prepare.CatalogReader catalogReader,
      RelNode inputRel,
      RelOptTable.ToRelContext relContext) {
    return new VacuumCatalogCrel(
        cluster,
        cluster.traitSetOf(Convention.NONE),
        null,
        null,
        getCatalogSource().getSimple(),
        null,
        null);
  }
}
