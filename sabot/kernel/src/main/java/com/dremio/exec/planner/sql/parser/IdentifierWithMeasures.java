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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.parser.SqlCreateReflection.MeasureType;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.google.common.base.Throwables;

public class IdentifierWithMeasures extends SqlIdentifier {

  private final SqlNodeList measures;

  public IdentifierWithMeasures(SqlIdentifier identifier, SqlNodeList measures, SqlParserPos pos) {
    super(identifier.names, pos);
    this.measures = measures;
  }

  public final List<MeasureType> getMeasureTypes() {
    try {
      List<MeasureType> measures = new ArrayList<>();
      for(SqlNode n : this.measures.getList()) {
        SqlLiteral l = SqlNodeUtil.unwrap(n, SqlLiteral.class);
        measures.add((MeasureType) l.getValue());
      }

      return measures;
    } catch(ForemanSetupException e) {
      throw Throwables.propagate(e);
    }
  }

}
