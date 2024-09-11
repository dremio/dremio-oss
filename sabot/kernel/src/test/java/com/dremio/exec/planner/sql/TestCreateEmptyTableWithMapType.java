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
package com.dremio.exec.planner.sql;

import static org.junit.Assert.assertEquals;

import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.parser.DremioSqlColumnDeclaration;
import com.dremio.exec.planner.sql.parser.SqlCreateEmptyTable;
import com.dremio.exec.record.BatchSchema;
import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.Test;

public class TestCreateEmptyTableWithMapType {

  ParserConfig config = new ParserConfig(Quoting.DOUBLE_QUOTE, 255);

  @Test
  public void testMapWithBasicTypes() throws SqlParseException {
    BatchSchema schema = createBatchSchema("CREATE TABLE a.b.c (col MAP<INT, INT>)");
    assertEquals(schema.getColumn(0).getType().getTypeID(), ArrowType.Map.TYPE_TYPE);
    assertEquals(getChildOfMapOrListTypeID(schema.getColumn(0), 0), ArrowType.Int.TYPE_TYPE);
    assertEquals(getChildOfMapOrListTypeID(schema.getColumn(0), 1), ArrowType.Int.TYPE_TYPE);
  }

  @Test
  public void testListInsideMap() throws SqlParseException {
    BatchSchema schema = createBatchSchema("CREATE TABLE a.b.c (col MAP<INT, MAP<INT,INT>>)");
    assertEquals(schema.getColumn(0).getType().getTypeID(), ArrowType.Map.TYPE_TYPE);
    assertEquals(getChildOfMapOrListTypeID(schema.getColumn(0), 0), ArrowType.Int.TYPE_TYPE);
    assertEquals(getChildOfMapOrListTypeID(schema.getColumn(0), 1), ArrowType.Map.TYPE_TYPE);
  }

  @Test
  public void testStructInsideMap() throws SqlParseException {
    BatchSchema schema = createBatchSchema("CREATE TABLE a.b.c (col MAP<INT, LIST<INT>>)");
    assertEquals(schema.getColumn(0).getType().getTypeID(), ArrowType.Map.TYPE_TYPE);
    assertEquals(getChildOfMapOrListTypeID(schema.getColumn(0), 0), ArrowType.Int.TYPE_TYPE);
    assertEquals(getChildOfMapOrListTypeID(schema.getColumn(0), 1), ArrowType.List.TYPE_TYPE);
  }

  @Test
  public void testMapInsideStruct() throws SqlParseException {
    BatchSchema schema =
        createBatchSchema("CREATE TABLE a.b.c (col STRUCT<x: MAP<INT,INT>, y: INT>)");
    assertEquals(schema.getColumn(0).getType().getTypeID(), ArrowType.Struct.TYPE_TYPE);
    assertEquals(
        schema.getColumn(0).getChildren().get(0).getType().getTypeID(), ArrowType.Map.TYPE_TYPE);
  }

  @Test
  public void testMapInsideList() throws SqlParseException {
    BatchSchema schema =
        createBatchSchema("CREATE TABLE a.b.c (col LIST<STRUCT<x: MAP<INT,INT>>>)");
    assertEquals(schema.getColumn(0).getType().getTypeID(), ArrowType.List.TYPE_TYPE);
    assertEquals(getChildOfMapOrListTypeID(schema.getColumn(0), 0), ArrowType.Map.TYPE_TYPE);
  }

  /** Creates a BatchSchema based on the SQL string */
  private BatchSchema createBatchSchema(String sql) throws SqlParseException {
    SqlParser parser = SqlParser.create(sql, config);
    SqlCreateEmptyTable node = (SqlCreateEmptyTable) parser.parseStmt();
    List<DremioSqlColumnDeclaration> columnDeclarations =
        SqlHandlerUtil.columnDeclarationsFromSqlNodes(node.getFieldList(), sql);
    return SqlHandlerUtil.batchSchemaFromSqlSchemaSpec(columnDeclarations, sql);
  }

  /**
   * MAP/LIST is a list of STRUCT, this method helps to unnest these types. col: Map(false)<entries:
   * Struct<key: Int(32, true) not null, value: Int(32, true)> not null>
   */
  private ArrowType.ArrowTypeID getChildOfMapOrListTypeID(Field map, int childId) {
    return map.getChildren().get(0).getChildren().get(childId).getType().getTypeID();
  }
}
