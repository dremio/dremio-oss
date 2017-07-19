/*
 * Copyright (C) 2017 Dremio Corporation
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

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.Test;

import com.dremio.exec.planner.sql.ParserConfig;

public class TestDDLAliases {

  private SqlNode parse(String toParse) throws SqlParseException{
    ParserConfig config = new ParserConfig(Quoting.DOUBLE_QUOTE, 255);
    SqlParser parser = SqlParser.create(toParse, config);
    return parser.parseStmt();
  }

  @Test
  public void alterDataset() throws SqlParseException{
    parse("ALTER DATASET a.b.c CREATE ACCELERATION");
  }

  @Test
  public void alterVDS() throws SqlParseException{
    parse("ALTER VDS a.b.c DROP ACCELERATION");
  }

  @Test
  public void alterPDS() throws SqlParseException {
    parse("ALTER PDS a.b.c ADD AGGREGATE LAYOUT DIMENSIONS (x by day,y) MEASURES (b,c) DISTRIBUTE BY (r,z) PARTITION BY (s,l) LOCALSORT BY (n,x)");
  }

  @Test
  public void createVDS() throws SqlParseException {
    parse("CREATE VDS MY_VDS AS SELECT * FROM SYS.OPTIONS");
  }

  @Test
  public void dropVDS() throws SqlParseException {
    parse("DROP VDS MY_VDS");
  }

}
