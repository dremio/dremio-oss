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
package com.dremio.plugins.elastic;

import static com.dremio.TestBuilder.listOf;
import static com.dremio.TestBuilder.mapOf;
import static com.dremio.plugins.elastic.ElasticsearchType.BINARY;
import static com.dremio.plugins.elastic.ElasticsearchType.BOOLEAN;
import static com.dremio.plugins.elastic.ElasticsearchType.BYTE;
import static com.dremio.plugins.elastic.ElasticsearchType.DATE;
import static com.dremio.plugins.elastic.ElasticsearchType.DOUBLE;
import static com.dremio.plugins.elastic.ElasticsearchType.FLOAT;
import static com.dremio.plugins.elastic.ElasticsearchType.GEO_POINT;
import static com.dremio.plugins.elastic.ElasticsearchType.GEO_SHAPE;
import static com.dremio.plugins.elastic.ElasticsearchType.INTEGER;
import static com.dremio.plugins.elastic.ElasticsearchType.IP;
import static com.dremio.plugins.elastic.ElasticsearchType.LONG;
import static com.dremio.plugins.elastic.ElasticsearchType.NESTED;
import static com.dremio.plugins.elastic.ElasticsearchType.OBJECT;
import static com.dremio.plugins.elastic.ElasticsearchType.SHORT;
import static com.dremio.plugins.elastic.ElasticsearchType.TEXT;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;

import javax.xml.bind.DatatypeConverter;

import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.expr.fn.impl.DateFunctionsUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class ITTestAllDataTypes extends ElasticBaseTestQuery {
  private static final Logger logger = LoggerFactory.getLogger(ITTestAllDataTypes.class);
  DateTimeFormatter formatter = DateFunctionsUtils.getISOFormatterForFormatString("YYYY-MM-DD").withZone(DateTimeZone.UTC);

  private static final String SPECIAL_COLUMN_NAME_1 = "@column_name_with_symbols";
  private static final String SPECIAL_COLUMN_NAME_2 = "column name with spaces";
  private static final String SPECIAL_COLUMN_NAME_3 = "column_with_special_characters''\"#`";
  private static final String SPECIAL_COLUMN_NAME_4 = "_name";
  protected static final String[] SPECIAL_COLUMNS = new String[] {SPECIAL_COLUMN_NAME_1, SPECIAL_COLUMN_NAME_2, SPECIAL_COLUMN_NAME_3, SPECIAL_COLUMN_NAME_4};
  private static String[] SPECIAL_COLUMNS_IN_BACKTICKS = new String[4];
  private static String[] SPECIAL_COLUMNS_FOR_SQL = new String[4];
  static {
    for (int i = 0; i < SPECIAL_COLUMNS.length; i++) {
      SPECIAL_COLUMNS_IN_BACKTICKS[i] = "`" + SPECIAL_COLUMNS[i].replace("`", "\\`") + "`";
      SPECIAL_COLUMNS_FOR_SQL[i] = "\"" + SPECIAL_COLUMNS[i].replace("\"", "\"\"") + "\"";
    }
  }

  @Before
  public void loadTable() throws IOException, ParseException {
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData(SPECIAL_COLUMN_NAME_1, LONG, new Object[][]{  {1}, {2}  }),
            new ElasticsearchCluster.ColumnData(SPECIAL_COLUMN_NAME_2, LONG, new Object[][]{  {3}, {4}  }),
            new ElasticsearchCluster.ColumnData(SPECIAL_COLUMN_NAME_3, LONG, new Object[][]{  {5}, {6}  }),
            new ElasticsearchCluster.ColumnData(SPECIAL_COLUMN_NAME_4, LONG, new Object[][]{  {7}, {8}  }),
            new ElasticsearchCluster.ColumnData("ID", TEXT, new Object[][]{  {"one"}, {"two"}  }),
            new ElasticsearchCluster.ColumnData("colString", TEXT, new Object[][]{  {"abcde"}, {null}  }),
            new ElasticsearchCluster.ColumnData("colCompletion", ElasticsearchType.COMPLETION, new Object[][]{  {"abcde"}, {"hello"}  }), // unknown type should be treated as hidden field.
            // requires es5 // new ElasticsearchCluster.ColumnData("halfFloat", ElasticsearchType.HALF_FLOAT, new Object[][]{  {1.0}, {-1.0}  }),
            new ElasticsearchCluster.ColumnData("colLong", LONG, new Object[][]{  {853848593.0}, {null}  }),
            new ElasticsearchCluster.ColumnData("colInt", INTEGER, new Object[][]{ {35000}, {null} }),
            new ElasticsearchCluster.ColumnData("colShort", SHORT, new Object[][]{  {100}, {null}  }),
            new ElasticsearchCluster.ColumnData("colByte", BYTE, new Object[][]{  {3}, {null}  }),
            new ElasticsearchCluster.ColumnData("colDouble", DOUBLE, new Object[][]{  {434.564}, {null}  }),
            new ElasticsearchCluster.ColumnData("colFloat", FLOAT, new Object[][]{  { 34.54321 }, { null } }),
            new ElasticsearchCluster.ColumnData("colDate", DATE, new Object[][]{  { formatter.parseLocalDateTime("2016-01-01") }, { null } }),
            new ElasticsearchCluster.ColumnData("colBoolean", BOOLEAN, new Object[][]{  { "true" }, { null } }),
            new ElasticsearchCluster.ColumnData("colBinary", BINARY, new Object[][]{  { DatatypeConverter.parseBase64Binary("U29tZSBiaW5hcnkgYmxvYg==") }, { null } }),
            new ElasticsearchCluster.ColumnData("colArrayString", TEXT, new Object[][]{
                    {ImmutableList.of("a", "bcd", "efg") }, { null } }),
            new ElasticsearchCluster.ColumnData("colArrayObject", OBJECT, new Object[][]{
                    { ImmutableList.of(ImmutableMap.of("a", "b"), ImmutableMap.of("a", "c")) }, { null } }),
            new ElasticsearchCluster.ColumnData("colObject", OBJECT, new Object[][]{
                    { ImmutableMap.of("name", ImmutableMap.of("first", "dremio", "last", "corporation")) }, { null } }),
            new ElasticsearchCluster.ColumnData("colNested", NESTED, new Object[][]{
                    { ImmutableMap.of("size", 2, "employees", ImmutableList.of(ImmutableMap.of("first", "emp1", "last", "last1"), ImmutableMap.of("first", "emp2", "last", "last2")))  },
                    {null} }),
            new ElasticsearchCluster.ColumnData("colIP", IP, new Object[][]{  { "172.168.1.1" }, { null } }),
            new ElasticsearchCluster.ColumnData("colGeoPoint", GEO_POINT, new Object[][]{  {ImmutableMap.of("lat", 41.12, "lon", -71.34)},
                    {null}  }),
            new ElasticsearchCluster.ColumnData("colGeoShapePoint", GEO_SHAPE, new Object[][]{
                    { ImmutableMap.of("type", "point", "coordinates", ImmutableList.of(-77.03653, 38.897676))},
                    {null}  }),
            new ElasticsearchCluster.ColumnData("colGeoShapeLineString", GEO_SHAPE, new Object[][]{
                    {ImmutableMap.of("type", "linestring", "coordinates", ImmutableList.of(ImmutableList.of(-77.03653, 38.897676), ImmutableList.of(-77.009051, 38.889939)))},
                    {null}  }),
            new ElasticsearchCluster.ColumnData("colGeoShapePolygon", GEO_SHAPE, new Object[][]{
                    {ImmutableMap.of("type", "polygon", "coordinates",
                            ImmutableList.of(
                                    ImmutableList.of(
                                            ImmutableList.of(100.0, 0.0), ImmutableList.of(101.0, 0.0), ImmutableList.of(101.0, 1.0), ImmutableList.of(100.0, 1.0), ImmutableList.of(100.0, 0.0)),
                                    ImmutableList.of(
                                            ImmutableList.of(100.2, 0.2), ImmutableList.of(100.8, 0.2), ImmutableList.of(100.8, 0.8), ImmutableList.of(100.2, 0.8), ImmutableList.of(100.2, 0.2))
                                    ))},
                    {null}  }),
            new ElasticsearchCluster.ColumnData("colGeoShapeMultiPoint", GEO_SHAPE, new Object[][]{
                    {ImmutableMap.of("type", "multipoint", "coordinates",
                            ImmutableList.of(ImmutableList.of(102.0, 2.0), ImmutableList.of(103.0, 2.0)))},
                    {null}  }),
            new ElasticsearchCluster.ColumnData("colGeoShapeMultiLineString", GEO_SHAPE, new Object[][]{
                    {ImmutableMap.of("type", "multilinestring", "coordinates",
                            ImmutableList.of(
                                    ImmutableList.of(
                                            ImmutableList.of(102.0, 2.0), ImmutableList.of(103.0, 2.0), ImmutableList.of(103.0, 3.0), ImmutableList.of(102.0, 3.0)
                                    ),
                                    ImmutableList.of(
                                            ImmutableList.of(102.0, 2.0), ImmutableList.of(103.0, 2.0), ImmutableList.of(103.0, 3.0), ImmutableList.of(102.0, 3.0)
                                    ),
                                    ImmutableList.of(
                                            ImmutableList.of(100.2, 0.2), ImmutableList.of(100.8, 0.2), ImmutableList.of(100.8, 0.8), ImmutableList.of(100.2, 0.8)
                                    )))},
                    {null}  }),
            new ElasticsearchCluster.ColumnData("colGeoShapeMultiPolygon", GEO_SHAPE, new Object[][]{
                    {ImmutableMap.of("type", "multipolygon", "coordinates",
                            ImmutableList.of(
                                    ImmutableList.of(
                                            ImmutableList.of(
                                                    ImmutableList.of(102.0, 2.0), ImmutableList.of(103.0, 2.0), ImmutableList.of(103.0, 3.0), ImmutableList.of(102.0, 3.0), ImmutableList.of(102.0, 2.0)
                                            )
                                    ),
                                    ImmutableList.of(
                                            ImmutableList.of(
                                                    ImmutableList.of(100.0, 0.0), ImmutableList.of(101.0, 0.0), ImmutableList.of(101.0, 1.0), ImmutableList.of(100.0, 1.0), ImmutableList.of(100.0, 0.0)
                                            ),
                                            ImmutableList.of(
                                                    ImmutableList.of(100.2, 0.2), ImmutableList.of(100.8, 0.2), ImmutableList.of(100.8, 0.8), ImmutableList.of(100.2, 0.8), ImmutableList.of(100.2, 0.2)
                                            )
                                    )
                            )
                    )},
                    {null}  }),
            new ElasticsearchCluster.ColumnData("colGeoShapeGeometryCollection", GEO_SHAPE, new Object[][]{
                    {ImmutableMap.of("type", "geometrycollection", "geometries",
                            ImmutableList.of(
                                    ImmutableMap.of("type", "point", "coordinates", ImmutableList.of(100.0, 0.0)),
                                    ImmutableMap.of("type", "linestring", "coordinates", ImmutableList.of(ImmutableList.of(101.0, 0.0), ImmutableList.of(102.0, 1.0)))
                            ))},
                    {null}  }),
            new ElasticsearchCluster.ColumnData("colGeoShapeEnvelope", GEO_SHAPE, new Object[][]{
                    {ImmutableMap.of("type", "envelope", "coordinates", ImmutableList.of(ImmutableList.of(-45.0, 45.0), ImmutableList.of(45.0, -45.)))}, {null}  }),
            new ElasticsearchCluster.ColumnData("colGeoShapeCircle", GEO_SHAPE, new Object[][]{
                    {ImmutableMap.of("type", "circle", "radius", "100m", "coordinates", ImmutableList.of(45.0, -45.0))}, {null}  }),
    };

    elastic.load(schema, table, data);
  }

  @Test
  public void join() throws Exception {
    String query = "select t1.ID, t2.colIP, t2.colInt, t2.ID as t2ID from elasticsearch." + schema + "." + table
        + " t1 join elasticsearch." + schema + "." + table + " t2 on t1.colInt = t2.colInt";
    try {
      test("set planner.leaf_limit_enable = true");
      testPlanMatchingPatterns(query,
          new String[]{
            "=[{\n" +
              "  \"from\" : 0,\n" +
              "  \"size\" : 1000,\n" +
              "  \"query\" : {\n" +
              "    \"match_all\" : { }\n" +
              "  }\n" +
              "}]",
            "=[{\n" +
              "  \"from\" : 0,\n" +
              "  \"size\" : 1000,\n" +
              "  \"query\" : {\n" +
              "    \"match_all\" : { }\n" +
              "  }\n" +
              "}]"},
          null);
      testBuilder().sqlQuery(query).unOrdered().baselineColumns("ID", "colIP", "colInt", "t2ID")
          .baselineValues("one", "172.168.1.1", 35000, "one")
          .go();
    } finally {
      test("set planner.leaf_limit_enable = false");
    }
  }

  @Ignore("requires es5")
  @Test
  public void testHalfFloat() throws Exception{
    final String sqlQuery = "select halfFloat from elasticsearch." + schema + "." + table + " where abs(halfFloat) = 1";
    test(sqlQuery);
  }

  @Test
  public void testOddColumnNames() throws Exception {
    String columns = Joiner.on(",").join(SPECIAL_COLUMNS_FOR_SQL);
    final String sqlQuery = "select " + columns + " from elasticsearch." + schema + "." + table;
    testBuilder()
      .sqlQuery(sqlQuery)
      .unOrdered()
      .baselineColumns(SPECIAL_COLUMNS_IN_BACKTICKS)
      .baselineValues(1l, 3l, 5l, 7l)
      .baselineValues(2l, 4l, 6l, 8l)
      .go();
  }

}
