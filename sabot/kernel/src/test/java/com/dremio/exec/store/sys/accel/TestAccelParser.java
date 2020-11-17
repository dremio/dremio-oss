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
package com.dremio.exec.store.sys.accel;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.Test;

import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.ParserConfig;

public class TestAccelParser {

  private SqlNode parse(String toParse) throws SqlParseException{
    ParserConfig config = new ParserConfig(Quoting.DOUBLE_QUOTE, 255, PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());
    SqlParser parser = SqlParser.create(toParse, config);
    return parser.parseStmt();
  }

  @Test
  public void addAggReflection() throws SqlParseException {
    parse("ALTER TABLE a.b.c CREATE AGGREGATE REFLECTION reflection USING DIMENSIONS (x by day,y) MEASURES (b,c) DISTRIBUTE BY (r,z) PARTITION BY (s,l) LOCALSORT BY (n,x)");
  }

  @Test
  public void addAggReflectionMeasures() throws SqlParseException {
    parse("ALTER TABLE a.b.c CREATE AGGREGATE REFLECTION reflection USING DIMENSIONS (x by day,y) MEASURES (b (COUNT, SUM),c (COUNT, MIN, MAX)) DISTRIBUTE BY (r,z) PARTITION BY (s,l) LOCALSORT BY (n,x)");
  }

  @Test
  public void addRawReflection() throws SqlParseException {
    parse("ALTER TABLE a.b.c CREATE RAW REFLECTION reflection USING DISPLAY(x,y) DISTRIBUTE BY (r,z) PARTITION BY (s,l) LOCALSORT BY (n,x)");
  }

  @Test
  public void dropLayout() throws SqlParseException {
    parse("ALTER TABLE a.b.c DROP REFLECTION \"123\"");
  }

  @Test
  public void toggleRawOn() throws SqlParseException {
    parse("ALTER TABLE a.b.c ENABLE RAW ACCELERATION");
  }

  @Test
  public void toggleRawOff() throws SqlParseException {
    parse("ALTER TABLE a.b.c DISABLE RAW ACCELERATION");
  }

  @Test
  public void toggleAggOn() throws SqlParseException {
    parse("ALTER TABLE a.b.c ENABLE AGGREGATE ACCELERATION");
  }
  @Test
  public void toggleAggOff() throws SqlParseException {
    parse("ALTER TABLE a.b.c DISABLE AGGREGATE ACCELERATION");
  }
}
