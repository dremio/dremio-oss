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

import org.apache.arrow.util.Preconditions;

import com.dremio.service.autocomplete.statements.grammar.AggregateReflectionCreateStatement;
import com.dremio.service.autocomplete.statements.grammar.AlterStatement;
import com.dremio.service.autocomplete.statements.grammar.CatalogPath;
import com.dremio.service.autocomplete.statements.grammar.DeleteStatement;
import com.dremio.service.autocomplete.statements.grammar.DropStatement;
import com.dremio.service.autocomplete.statements.grammar.ExternalReflectionCreateStatement;
import com.dremio.service.autocomplete.statements.grammar.FieldList;
import com.dremio.service.autocomplete.statements.grammar.FromClause;
import com.dremio.service.autocomplete.statements.grammar.JoinCondition;
import com.dremio.service.autocomplete.statements.grammar.RawReflectionCreateStatement;
import com.dremio.service.autocomplete.statements.grammar.SelectQueryStatement;
import com.dremio.service.autocomplete.statements.grammar.SetQueryStatement;
import com.dremio.service.autocomplete.statements.grammar.Statement;
import com.dremio.service.autocomplete.statements.grammar.StatementList;
import com.dremio.service.autocomplete.statements.grammar.UnknownStatement;
import com.dremio.service.autocomplete.statements.grammar.UpdateStatement;
import com.dremio.service.autocomplete.tokens.Cursor;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.google.common.collect.ImmutableList;

public final class StatementSerializer implements StatementVisitor {
  private final StringBuilder stringBuilder;
  private int level;

  public StatementSerializer(StringBuilder stringBuilder) {
    Preconditions.checkNotNull(stringBuilder);

    this.stringBuilder = stringBuilder;
    this.level = 0;
  }

  @Override
  public void visit(StatementList statementList) {
    if (statementList.getChildren().size() == 1) {
      statementList.getChildren().get(0).accept(this);
    } else {
      visitImplementation("LIST", statementList);
    }
  }

  @Override
  public void visit(UnknownStatement unknownStatement) {
    visitImplementation("UNKNOWN", unknownStatement);
  }

  @Override
  public void visit(SelectQueryStatement selectQueryStatement) {
    visitImplementation("SELECT QUERY", selectQueryStatement);
    FromClause fromClause = selectQueryStatement.getFromClause();
    if (fromClause != null) {
      writeSubline(() -> {
        stringBuilder.append("FROM: ");
        writeTokens(fromClause.getTokens());
        stringBuilder.append("\n");

        writeSubline(() -> {
          stringBuilder.append("CATALOG PATHS: [");
          for (CatalogPath catalogPath : fromClause.getCatalogPaths()) {
            writeTokens(catalogPath.getTokens());
            stringBuilder.append(",");
            stringBuilder.append(",");
          }
          stringBuilder.append("]");
        });

        writeSubline(() -> {
          stringBuilder.append("JOIN CONDITIONS: [");
          for (JoinCondition joinCondition : fromClause.getJoinConditions()) {
            writeTokens(joinCondition.getTokens());
            stringBuilder.append(",");
          }
          stringBuilder.append("]");
        });
      });
    }
  }

  @Override
  public void visit(SetQueryStatement setQueryStatement) {
    if (setQueryStatement.getChildren().size() == 1) {
      setQueryStatement.getChildren().get(0).accept(this);
    } else {
      visitImplementation("SET QUERY", setQueryStatement);
    }
  }

  @Override
  public void visit(DropStatement dropStatement) {
    visitImplementation("DROP", dropStatement);
    if (dropStatement.getDropType() != null) {
      writeSubline(() -> stringBuilder
        .append("TYPE: ")
        .append(dropStatement.getDropType()));
    }

    if (dropStatement.getCatalogPath() != null) {
      writeCatalogPath(dropStatement.getCatalogPath());
    }
  }

  @Override
  public void visit(DeleteStatement deleteStatement) {
    visitImplementation("DELETE", deleteStatement);
    if (deleteStatement.getCatalogPath() != null) {
      writeCatalogPath(deleteStatement.getCatalogPath());
    }

    if (deleteStatement.getCondition() != null) {
      writeSubline(() -> {
        stringBuilder.append("BOOLEAN EXPRESSION: ");
        writeTokens(deleteStatement.getCondition());
      });
    }
  }

  @Override
  public void visit(UpdateStatement updateStatement) {
    visitImplementation("UPDATE", updateStatement);
    if (updateStatement.getTablePrimary() != null) {
      writeSubline(() -> {
        stringBuilder.append("TABLE PRIMARY: ");
        writeTokens(updateStatement.getTablePrimary().getTokens());
      });
    }

    if (updateStatement.getAssignTokens() != null) {
      writeSubline(() -> {
        stringBuilder.append("ASSIGN TOKENS: ");
        writeTokens(updateStatement.getAssignTokens());
      });
    }

    if (updateStatement.getCondition() != null) {
      writeSubline(() -> {
        stringBuilder.append("BOOLEAN EXPRESSION: ");
        writeTokens(updateStatement.getCondition());
      });
    }
  }

  @Override
  public void visit(RawReflectionCreateStatement rawReflectionCreateStatement) {
    visitImplementation("RAW REFLECTION CREATE", rawReflectionCreateStatement);
    writeCatalogPath(rawReflectionCreateStatement.getCatalogPath());
    writeSubline(() -> stringBuilder.append("NAME: ").append(rawReflectionCreateStatement.getReflectionName()));

    if (rawReflectionCreateStatement.getDisplayFields() != null) {
      writeFieldList(rawReflectionCreateStatement.getDisplayFields(), "DISPLAY");
    }

    if (rawReflectionCreateStatement.getFieldLists() != null) {
      if (rawReflectionCreateStatement.getFieldLists().getDistributeFields() != null) {
        writeFieldList(rawReflectionCreateStatement.getFieldLists().getDistributeFields(), "DISTRIBUTE");
      }

      if (rawReflectionCreateStatement.getFieldLists().getPartitionFields() != null) {
        writeFieldList(rawReflectionCreateStatement.getFieldLists().getPartitionFields(), "PARTITION");
      }

      if (rawReflectionCreateStatement.getFieldLists().getLocalSortFields() != null) {
        writeFieldList(rawReflectionCreateStatement.getFieldLists().getLocalSortFields(), "LOCALSORT");
      }
    }
  }

  @Override
  public void visit(AlterStatement alterStatement) {
    visitImplementation("ALTER", alterStatement);

    writeSubline(() -> {
      stringBuilder.append("TYPE: ").append(alterStatement.getType());
    });

    if (alterStatement.getCatalogPath() != null) {
      writeCatalogPath(alterStatement.getCatalogPath());
    }
  }

  @Override
  public void visit(AggregateReflectionCreateStatement aggregateReflectionCreateStatement) {
    visitImplementation("AGGREGATE REFLECTION CREATE", aggregateReflectionCreateStatement);
    writeCatalogPath(aggregateReflectionCreateStatement.getCatalogPath());
    writeSubline(() -> stringBuilder.append("NAME: ").append(aggregateReflectionCreateStatement.getName()));

    if (aggregateReflectionCreateStatement.getDimensions() != null) {
      writeFieldList(aggregateReflectionCreateStatement.getDimensions(), "DIMENSIONS");
    }

    if (aggregateReflectionCreateStatement.getMeasures() != null) {
      writeFieldList(aggregateReflectionCreateStatement.getMeasures(), "MEASURES");
    }

    if (aggregateReflectionCreateStatement.getFieldLists() != null && aggregateReflectionCreateStatement.getFieldLists().getDistributeFields() != null) {
      writeFieldList(aggregateReflectionCreateStatement.getFieldLists().getDistributeFields(), "DISTRIBUTE");
    }

    if (aggregateReflectionCreateStatement.getFieldLists() != null && aggregateReflectionCreateStatement.getFieldLists().getPartitionFields() != null) {
      writeFieldList(aggregateReflectionCreateStatement.getFieldLists().getPartitionFields(), "PARTITION");
    }

    if (aggregateReflectionCreateStatement.getFieldLists() != null && aggregateReflectionCreateStatement.getFieldLists().getLocalSortFields() != null) {
      writeFieldList(aggregateReflectionCreateStatement.getFieldLists().getLocalSortFields(), "LOCALSORT");
    }
  }

  @Override
  public void visit(ExternalReflectionCreateStatement externalReflectionCreateStatement) {
    visitImplementation("EXTERNAL REFLECTION CREATE", externalReflectionCreateStatement);
    writeCatalogPath(externalReflectionCreateStatement.getSourcePath());
    writeSubline(() -> stringBuilder.append("NAME: ").append(externalReflectionCreateStatement.getName()));

    writeCatalogPath(externalReflectionCreateStatement.getTargetPath());
  }

  private void writeCatalogPath(CatalogPath catalogPath) {
    writeSubline(() -> {
      stringBuilder.append("CATALOG PATH: ");
      for (String pathToken : catalogPath.getPathTokens()) {
        writeToken(pathToken);
        stringBuilder.append(".");
      }
    });
  }

  private void writeFieldList(FieldList fieldList, String name) {
    writeSubline(() -> {
      stringBuilder.append(name).append(": ");
      writeTokens(fieldList.getTokens());
    });
  }

  private void visitImplementation(String type, Statement statement) {
    writeLine(() -> {
      stringBuilder.append(type).append(": ");
      writeTokens(statement.getTokens());
    });

    level++;
    for (Statement child : statement.getChildren()) {
      child.accept(this);
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
