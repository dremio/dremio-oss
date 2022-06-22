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

public interface StatementInputOutputVisitor<I, O> {

  O visit(StatementList statementList, I input);

  O visit(UnknownStatement unknownStatement, I input);

  O visit(SelectQueryStatement selectQueryStatement, I input);

  O visit(SetQueryStatement setQueryStatement, I input);

  O visit(DropStatement dropStatement, I input);

  O visit(DeleteStatement deleteStatement, I input);

  O visit(UpdateStatement updateStatement, I input);

  O visit(RawReflectionCreateStatement rawReflectionCreateStatement, I input);

  O visit(AlterStatement alterStatement, I input);

  O visit(AggregateReflectionCreateStatement aggregateReflectionCreateStatement, I input);

  O visit(ExternalReflectionCreateStatement externalReflectionCreateStatement, I input);
}
