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

import static com.dremio.exec.planner.sql.handlers.query.CopyIntoTableContext.IngestionOption.BATCH_ID;
import static com.dremio.exec.planner.sql.handlers.query.CopyIntoTableContext.IngestionOption.PIPE_ID;
import static com.dremio.exec.planner.sql.handlers.query.CopyIntoTableContext.IngestionOption.PIPE_NAME;

import com.dremio.exec.planner.sql.handlers.query.CopyIntoTableContext;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;
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

public class SqlTriggerPipe extends SqlCopyIntoTable {

  private final SqlIdentifier pipeName;
  private final SqlIdentifier batchId;

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("TRIGGER PIPE", SqlKind.OTHER) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          Preconditions.checkArgument(
              operands.length == 2,
              String.format("Invalid number of operands: %d", operands.length));
          return new SqlTriggerPipe(pos, (SqlIdentifier) operands[0], (SqlIdentifier) operands[1]);
        }

        @Override
        public RelDataType deriveType(
            SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
          return new RelDataTypeFactory.Builder(validator.getTypeFactory()).build();
        }
      };

  public SqlTriggerPipe(SqlParserPos pos, SqlIdentifier pipeId, SqlIdentifier batchId) {
    this(
        pos,
        pipeId,
        null,
        batchId,
        null,
        null,
        null,
        null,
        null,
        new SqlNodeList(pos),
        new SqlNodeList(pos));
  }

  private SqlTriggerPipe(
      SqlParserPos pos,
      SqlIdentifier pipeName,
      SqlNode pipeId,
      SqlIdentifier batchId,
      SqlNode targetTable,
      SqlNodeList mapping,
      SqlNode select,
      SqlNode storageLocation,
      SqlNode fileFormat,
      SqlNodeList optionsList,
      SqlNodeList optionsValueList) {
    super(
        pos,
        targetTable,
        mapping,
        select,
        storageLocation,
        SqlNodeList.EMPTY,
        null,
        fileFormat,
        optionsList,
        optionsValueList);

    this.pipeName = pipeName;
    this.batchId = batchId != null ? batchId : new SqlIdentifier(UUID.randomUUID().toString(), pos);

    optionsList.add(SqlLiteral.createCharString(PIPE_NAME.name(), pos));
    optionsValueList.add(SqlLiteral.createCharString(pipeName.getSimple(), pos));
    optionsList.add(SqlLiteral.createCharString(BATCH_ID.name(), pos));
    optionsValueList.add(SqlLiteral.createCharString(this.batchId.getSimple(), pos));
    optionsList.add(SqlLiteral.createCharString(PIPE_ID.name(), pos));
    optionsValueList.add(pipeId);
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  public SqlIdentifier getPipeName() {
    return pipeName;
  }

  public SqlIdentifier getBatchId() {
    return batchId;
  }

  public SqlTriggerPipe withCopyIntoOptions(
      String pipeId,
      Function<SqlParserPos, SqlNode> targetTableTransformer,
      String storageLocation,
      String fileFormat,
      int dedupLookbackPeriod,
      Map<CopyIntoTableContext.CopyOption, Object> copyIntoOptions,
      Map<CopyIntoTableContext.FormatOption, Object> fileFormatOptions) {
    SqlNode targetTable = targetTableTransformer.apply(pos);
    SqlNodeList optionsList = new SqlNodeList(pos);
    SqlNodeList optionsValueList = new SqlNodeList(pos);
    Stream.concat(fileFormatOptions.entrySet().stream(), copyIntoOptions.entrySet().stream())
        .forEach(
            option -> {
              optionsList.add(SqlLiteral.createCharString(option.getKey().name(), pos));
              optionsValueList.add(SqlLiteral.createCharString(option.getValue().toString(), pos));
            });
    optionsList.add(
        SqlLiteral.createCharString(
            CopyIntoTableContext.IngestionOption.DEDUP_LOOKBACK_PERIOD.name(), pos));
    optionsValueList.add(SqlLiteral.createCharString(String.valueOf(dedupLookbackPeriod), pos));

    return new SqlTriggerPipe(
        pos,
        pipeName,
        SqlLiteral.createCharString(pipeId, pos),
        batchId,
        targetTable,
        // TODO: fix this later
        null,
        null,
        SqlLiteral.createCharString(storageLocation, pos),
        SqlLiteral.createCharString(fileFormat, pos),
        optionsList,
        optionsValueList);
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> operands = new ArrayList<>();
    operands.add(pipeName);
    operands.add(batchId);
    return operands;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("TRIGGER");
    writer.keyword("PIPE");
    pipeName.unparse(writer, leftPrec, rightPrec);
    if (batchId != null) {
      writer.keyword("FOR");
      writer.keyword("BATCH");
      batchId.unparse(writer, leftPrec, rightPrec);
    }
  }

  @Override
  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    // Skipping the validation here to avoid the validator operator from
    // trying to resolve the table version for the current node which is `TRIGGER PIPE <>`
    // anyway this is not needed since `TRIGGER PIPE` is just wrapper for the
    // actual copy into plan which happens at a later stage
  }
}
