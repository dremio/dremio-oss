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
package com.dremio.dac.obfuscate;

import com.dremio.dac.obfuscate.visitor.ObfuscationRegistry;
import com.dremio.exec.calcite.SqlNodes;
import com.dremio.exec.planner.sql.ParserConfig;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParser;

/**
 * Tool to redact the identifiers and string literals in a sql statement This is package-protected
 * and all usages should be via ObfuscationUtils because this does not check whether to obfuscate or
 * not.
 */
class SqlCleanser {
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(SqlCleanser.class);
  private static final ParserConfig CONFIG = new ParserConfig(Quoting.DOUBLE_QUOTE, 1000);

  public static String cleanseSql(String sql) {
    String cleansedSql;
    SqlParser parser = SqlParser.create(sql, CONFIG);
    try {
      SqlNodeList sqlNodeList = parser.parseStmtList();
      SqlNode cleansedTree = ObfuscationRegistry.obfuscate(sqlNodeList);
      cleansedSql = SqlNodes.toSQLString(cleansedTree);
    } catch (Exception e) {
      String errorMsg =
          "Exception while parsing sql, so obfuscating whole sql with a random hex string. ";
      LOGGER.error(errorMsg, e);
      cleansedSql = errorMsg + ObfuscationUtils.obfuscatePartial(sql);
    }
    return cleansedSql;
  }
}
