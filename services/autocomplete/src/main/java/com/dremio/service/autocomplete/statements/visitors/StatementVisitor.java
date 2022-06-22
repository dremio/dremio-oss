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
import com.dremio.service.autocomplete.statements.grammar.DeleteStatement;
import com.dremio.service.autocomplete.statements.grammar.DropStatement;
import com.dremio.service.autocomplete.statements.grammar.ExternalReflectionCreateStatement;
import com.dremio.service.autocomplete.statements.grammar.RawReflectionCreateStatement;
import com.dremio.service.autocomplete.statements.grammar.SelectQueryStatement;
import com.dremio.service.autocomplete.statements.grammar.SetQueryStatement;
import com.dremio.service.autocomplete.statements.grammar.StatementList;
import com.dremio.service.autocomplete.statements.grammar.UnknownStatement;
import com.dremio.service.autocomplete.statements.grammar.UpdateStatement;

public interface StatementVisitor {
  void visit(StatementList statementList);

  void visit(UnknownStatement unknownStatement);

  void visit(SelectQueryStatement selectQueryStatement);

  void visit(SetQueryStatement setQueryStatement);

  void visit(DropStatement dropStatement);

  void visit(DeleteStatement deleteStatement);

  void visit(UpdateStatement updateStatement);

  void visit(RawReflectionCreateStatement rawReflectionCreateStatement);

  void visit(AlterStatement alterStatement);

  void visit(AggregateReflectionCreateStatement aggregateReflectionCreateStatement);

  void visit(ExternalReflectionCreateStatement externalReflectionCreateStatement);
}
