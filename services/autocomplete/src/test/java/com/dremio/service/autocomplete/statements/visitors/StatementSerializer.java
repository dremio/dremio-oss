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
package com.dremio.service.autocomplete.statements.visitors;

import com.dremio.service.autocomplete.statements.grammar.AggregateReflectionCreateStatement;
import com.dremio.service.autocomplete.statements.grammar.AlterStatement;
import com.dremio.service.autocomplete.statements.grammar.Column;
import com.dremio.service.autocomplete.statements.grammar.DropStatement;
import com.dremio.service.autocomplete.statements.grammar.Expression;
import com.dremio.service.autocomplete.statements.grammar.ExternalReflectionCreateStatement;
import com.dremio.service.autocomplete.statements.grammar.FieldList;
import com.dremio.service.autocomplete.statements.grammar.NessieVersion;
import com.dremio.service.autocomplete.statements.grammar.RawReflectionCreateStatement;
import com.dremio.service.autocomplete.statements.grammar.SelectItem;
import com.dremio.service.autocomplete.statements.grammar.SetQueryStatement;
import com.dremio.service.autocomplete.statements.grammar.Statement;
import com.dremio.service.autocomplete.statements.grammar.StatementList;
import com.dremio.service.autocomplete.statements.grammar.TableReference;
import com.dremio.service.autocomplete.tokens.Cursor;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public final class StatementSerializer {
  private final StringBuilder stringBuilder;
  private int level;

  public StatementSerializer(StringBuilder stringBuilder) {
    Preconditions.checkNotNull(stringBuilder);

    this.stringBuilder = stringBuilder;
    this.level = 0;
  }

  public void visit(Statement statement) {
    Preconditions.checkNotNull(statement);

    if (statement instanceof StatementList) {
      visit((StatementList) statement);
    } else if (statement instanceof SetQueryStatement) {
      visit((SetQueryStatement) statement);
    } else if (statement instanceof DropStatement) {
      visit((DropStatement) statement);
    } else if (statement instanceof AlterStatement) {
      visit((AlterStatement) statement);
    } else if (statement instanceof RawReflectionCreateStatement) {
      visit((RawReflectionCreateStatement) statement);
    } else if (statement instanceof AggregateReflectionCreateStatement) {
      visit((AggregateReflectionCreateStatement) statement);
    } else if (statement instanceof ExternalReflectionCreateStatement) {
      visit((ExternalReflectionCreateStatement) statement);
    } else if (statement instanceof FieldList) {
      visit((FieldList) statement);
    } else if (statement instanceof Expression) {
      visit((Expression) statement);
    } else if (statement instanceof NessieVersion) {
      visit((NessieVersion) statement);
    } else if (statement instanceof Column) {
      visit((Column) statement);
    } else if (statement instanceof TableReference) {
      visit((TableReference) statement);
    } else if (statement instanceof SelectItem) {
      visit((SelectItem) statement);
    } else {
      visitImplementation(statement.getClass().getSimpleName(), statement);
    }
  }

  private void visit(StatementList statementList) {
    if (statementList.getChildren().size() == 1) {
      visit(statementList.getChildren().get(0));
    } else {
      visitImplementation("LIST", statementList);
    }
  }

  private void visit(AlterStatement alterStatement) {
    visitImplementation("ALTER", alterStatement);
    writeSubline(() -> {
      stringBuilder.append("TYPE: ").append(alterStatement.getType());
    });
  }

  private void visit(SetQueryStatement setQueryStatement) {
    if (setQueryStatement.getChildren().size() == 1) {
      visit(setQueryStatement.getChildren().get(0));
    } else {
      visitImplementation("SET QUERY", setQueryStatement);
    }
  }

  private void visit(DropStatement dropStatement) {
    visitImplementation("DROP", dropStatement);
    if (dropStatement.getDropType() != null) {
      writeSubline(() -> stringBuilder
        .append("TYPE: ")
        .append(dropStatement.getDropType()));
    }
  }

  private void visit(RawReflectionCreateStatement rawReflectionCreateStatement) {
    visitImplementation("RAW REFLECTION CREATE", rawReflectionCreateStatement);
    writeSubline(() -> stringBuilder.append("NAME: ").append(rawReflectionCreateStatement.getReflectionName()));
  }

  private void visit(AggregateReflectionCreateStatement aggregateReflectionCreateStatement) {
    visitImplementation("AGGREGATE REFLECTION CREATE", aggregateReflectionCreateStatement);
    writeSubline(() -> stringBuilder.append("NAME: ").append(aggregateReflectionCreateStatement.getName()));
  }

  private void visit(ExternalReflectionCreateStatement externalReflectionCreateStatement) {
    visitImplementation("EXTERNAL REFLECTION CREATE", externalReflectionCreateStatement);
    writeSubline(() -> stringBuilder.append("NAME: ").append(externalReflectionCreateStatement.getName()));
  }

  private void visit(Column column) {
    visitImplementation("COLUMN", column);
    level++;
    visit(column.getTableReference());
    level--;
  }

  private void visit(Expression expression) {
    visitImplementation("EXPRESSION", expression);
    level++;
    for (TableReference tableReference : expression.getTableReferences()) {
      visit(tableReference);
    }
    level--;
  }

  private void visit(NessieVersion nessieVersion) {
    visitImplementation("NESSIE VERSION", nessieVersion);
    if (nessieVersion.getType().isPresent()) {
      writeSubline(() -> stringBuilder.append("TYPE: ").append(nessieVersion.getType().get()));
    }
  }

  private void visit(FieldList fieldList) {
    visitImplementation("FIELD LIST", fieldList);
    level++;
    for (String field : fieldList.getFields()) {
      writeLine(() -> {
        stringBuilder.append("FIELD: ").append(field);
      });
    }

    visit(fieldList.getTableReference());
    level--;
  }

  private void visit(SelectItem selectItem) {
    visitImplementation("SELECT ITEM", selectItem);
    if (selectItem.getAlias() != null) {
      writeSubline(() -> stringBuilder.append("ALIAS: ").append(selectItem.getAlias()));
    }
  }

  private void visit(TableReference tableReference) {
    visitImplementation("TABLE REFERENCE", tableReference);
  }

  private void visitImplementation(String type, Statement statement) {
    writeLine(() -> {
      stringBuilder.append(type).append(": ");
      writeTokens(statement.getTokens());
    });

    level++;
    for (Statement child : statement.getChildren()) {
      visit(child);
    }
    level--;
  }

  private void writeLine(Runnable runnable) {
    for (int i = 0; i < level; i++) {
      stringBuilder.append("  ");
    }

    runnable.run();
    stringBuilder.append("\n");
  }

  private void writeSubline(Runnable runnable) {
    level++;
    for (int i = 0; i < level; i++) {
      stringBuilder.append("  ");
    }

    runnable.run();
    stringBuilder.append("\n");
    level--;
  }

  private void writeTokens(ImmutableList<DremioToken> tokens) {
    for (DremioToken token : tokens) {
      writeToken(token);
      stringBuilder.append(" ");
    }
  }

  private void writeToken(DremioToken token) {
    writeToken(token.getImage());
  }

  private void writeToken(String token) {
    this.stringBuilder.append(token.replace(Cursor.CURSOR_CHARACTER, "^"));
  }
}
