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
package com.dremio.dac.obfuscate.visitor;

import static com.dremio.dac.obfuscate.visitor.ObfuscationRegistry.processSqlCall;

import com.dremio.dac.obfuscate.ObfuscationUtils;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;

/**
 * This visitor class is solely used for obfuscating table names, alias names and dataset names and
 * contains {@code visit(SqlIdentifier)} which obfuscates them as some unique hash value. This
 * {@code visit(SqlIdentifier)} function deserves a separate class and cannot be placed in {@link
 * BaseVisitor} because we cannot have two overloaded methods with the same signature in the same
 * class.<br>
 * <br>
 * {@code visit(SqlCall call)} is also overriden so that recursion can occur.
 */
public class TableNameVisitor extends SqlShuttle {

  TableNameVisitor() {}

  @Override
  public SqlNode visit(SqlCall call) {
    return processSqlCall(call, () -> super.visit(call));
  }

  @Override
  public SqlNode visit(SqlIdentifier id) {
    if (!ObfuscationUtils.shouldObfuscateFull()) {
      return id;
    }

    List<String> redactedNames =
        id.names.stream()
            .map(s -> Integer.toHexString(s.toLowerCase().hashCode()))
            .collect(Collectors.toList());
    return new SqlIdentifier(redactedNames, id.getCollation(), id.getParserPosition(), null);
  }
}
