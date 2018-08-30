/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import static com.dremio.plugins.elastic.ElasticBaseTestQuery.TestNameGenerator.aliasName;
import static com.dremio.plugins.elastic.ElasticBaseTestQuery.TestNameGenerator.schemaName;
import static com.dremio.plugins.elastic.ElasticBaseTestQuery.TestNameGenerator.tableName;
import static com.dremio.plugins.elastic.ElasticsearchType.DATE;
import static com.dremio.plugins.elastic.ElasticsearchType.INTEGER;
import static com.dremio.plugins.elastic.ElasticsearchType.TEXT;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.junit.Assume.assumeFalse;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

public class ITTestAliases extends ElasticBaseTestQuery {

  private static final Logger logger = LoggerFactory.getLogger(ITTestAliases.class);

  @Test
  public void testAlias() throws Exception {
    // DX-12162: upstream typo when processing contains
    if (elastic.getMinVersionInCluster().getMajor() == 5) {
      assumeFalse(elastic.getMinVersionInCluster().getMinor() <= 2);
    }

    String schema1 = schema;
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
                    {"San Francisco"},
                    {"Oakland"},
                    {"San Jose"}
            })
    };

    elastic.load(schema, table, data);
    schema = schemaName();
    data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
                    {"New York"},
                    {"Los Angeles"},
                    {"Chicago"}
            })
    };

    elastic.load(schema, table, data);

    elastic.alias("alias1", schema1, schema);

    String sql = String.format("select location from elasticsearch.alias1.%s where contains(location:(Chicago OR Oakland))", table);
    testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("location")
            .baselineValues("Chicago")
            .baselineValues("Oakland")
            .go();
  }

  @Test
  public void testAliasAggregation() throws Exception {
    // DX-12162: upstream typo when processing contains
    if (elastic.getMinVersionInCluster().getMajor() == 5) {
      assumeFalse(elastic.getMinVersionInCluster().getMinor() <= 2);
    }

    String schema1 = schema;
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
                    {"San Francisco"},
                    {"Oakland"},
                    {"San Jose"}
            })
    };

    elastic.load(schema, table, data);
    schema = schemaName();
    data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
                    {"New York"},
                    {"Los Angeles"},
                    {"Chicago"}
            })
    };

    elastic.load(schema, table, data);

    elastic.alias("alias1", schema1, schema);

    String sql = String.format("select count(location) from elasticsearch.alias1.%s where contains(location:(Chicago OR Oakland))", table);

    testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("EXPR$0")
            .baselineValues(2L)
            .go();
  }

  @Test
  public void testAliasWithDateFormat() throws Exception {
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("EventDate", DATE, ImmutableMap.of("format", "strict_date_optional_time"),new Object[][]{
        {"1970-01-23T20:23:51.927"}
      })
    };

    elastic.load(schema, table, data);
    elastic.alias(alias, schema);

    String sql = String.format("select EventDate from elasticsearch.%s.%s", alias, table);
    test(sql);
  }

  @Test
  public void q() throws Exception {
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
        {"San Francisco"},
        {"San Jose"},
        {"Oakland"}
      }),
      new ElasticsearchCluster.ColumnData("id", ElasticsearchType.DOUBLE, new Object[][]{
        {1.0},
        {2.0},
        {3.0},
      })
    };

    elastic.load(schema, table, data);

//    elastic.aliasWithFilter(alias, "{ \"range\": { \"id\" : { \"lt\" : 2.0 } } }", schema);
//    elastic.aliasWithFilter(alias, "{ \"match\" : { \"id\": 1.0 } }", schema);
    elastic.alias(alias, schema);

//    test(String.format("describe elasticsearch.%s.%s", alias, table));
    String sql = String.format("select cast(floor(id) as integer) as id from elasticsearch.%s.%s", alias, table);
    test("explain plan for " + sql);
    test(sql);
  }

  @Test
  public void testAliasWithFilterSort() throws Exception {
    String schema1 = schema;
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
        {"San Francisco"},
        {"San Jose"},
        {"Oakland"}
      }),
      new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
        {1},
        {2},
        {3},
      })
    };

    elastic.load(schema, table, data);
    schema = schemaName();

    data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
        {"New York"},
        {"Chicago"},
        {"Los Angeles"}
      }),
      new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
        {1},
        {2},
        {3},
      })
    };
    elastic.load(schema, table, data);

    elastic.aliasWithFilter(alias, "{ \"range\": { \"id\" : { \"lte\" : 1 } } }", schema1, schema);

    String sql = String.format("select location, id from elasticsearch.%s.%s order by id, location", alias, table);
    testBuilder()
      .sqlQuery(sql)
      .ordered()
      .baselineColumns("location", "id")
      .baselineValues("New York", 1)
      .baselineValues("San Francisco",1)
      .go();
  }

  @Test
  public void testAliasWithFilterAggregation() throws Exception {
    String schema1 = schema;
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
        {"San Francisco"},
        {"San Jose"},
        {"Oakland"}
      }),
      new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
        {1},
        {2},
        {3},
      })
    };

    elastic.load(schema, table, data);
    schema = schemaName();

    data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
        {"New York"},
        {"Chicago"},
        {"Los Angeles"}
      }),
      new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
        {1},
        {2},
        {3},
      })
    };
    elastic.load(schema, table, data);

    elastic.aliasWithFilter(alias, "{ \"range\": { \"id\" : { \"lte\" : 1 } } }", schema1, schema);

    String sql = String.format("select location, count(*) cnt from elasticsearch.%s.%s group by location", alias, table);
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("location", "cnt")
      .baselineValues("San Francisco",1L)
      .baselineValues("New York", 1L)
      .go();
  }

  @Test
  public void testAliasWithFilterNoWhereClause() throws Exception {

    String schema1 = schema;
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
        {"San Francisco"},
        {"San Jose"},
        {"Oakland"}
      }),
      new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
        {1},
        {2},
        {3},
      })
    };

    elastic.load(schema, table, data);
    schema = schemaName();

    data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
        {"New York"},
        {"Chicago"},
        {"Los Angeles"}
      }),
      new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
        {1},
        {2},
        {3},
      })
    };
    elastic.load(schema, table, data);

    elastic.aliasWithFilter(alias, "{ \"range\": { \"id\" : { \"lte\" : 1 } } }", schema1, schema);

    String sql = String.format("select location from elasticsearch.%s.%s", alias, table);
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("location")
      .baselineValues("San Francisco")
      .baselineValues("New York")
      .go();
  }
  @Test
  public void testAliasWithFilterAndWhereClause() throws Exception {
    // DX-12162: upstream typo when processing contains
    if (elastic.getMinVersionInCluster().getMajor() == 5) {
      assumeFalse(elastic.getMinVersionInCluster().getMinor() <= 2);
    }

    String schema1 = schema;
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
        {"San Francisco"},
        {"San Jose"},
        {"Oakland"}
      }),
      new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
        {1},
        {2},
        {3},
      })
    };

    elastic.load(schema, table, data);
    schema = schemaName();

    data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
        {"New York"},
        {"Chicago"},
        {"Los Angeles"}
      }),
      new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
        {1},
        {2},
        {3},
      })
    };
    elastic.load(schema, table, data);

    elastic.aliasWithFilter(alias, "{ \"range\": { \"id\" : { \"lte\" : 1 } } }", schema1, schema);

    String sql = String.format("select location from elasticsearch.%s.%s where contains(location:(Chicago OR \"San Francisco\"~))", alias, table);
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("location")
      .baselineValues("San Francisco")
      .go();
  }

  @Test
  public void testAliasWildCardFail() throws Exception {

    String schema1 = schema;
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
                    {"San Francisco"},
                    {"San Jose"},
                    {"Oakland"}
            }),
            new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
                    {1},
                    {2},
                    {3},
            })
    };

    elastic.load(schema, table, data);
    schema = schemaName();

    data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
                    {"New York"},
                    {"Chicago"},
                    {"Los Angeles"}
            }),
            new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
                    {1},
                    {2},
                    {3},
            })
    };
    elastic.load(schema, table, data);

    elastic.aliasWithFilter(alias, "{ \"range\": { \"id\" : { \"lte\" : 1 } } }", schema1, schema);
    String alias1 = alias;
    alias = aliasName();
    elastic.aliasWithFilter(alias, "{ \"range\": { \"id\" : { \"gt\" : 1 } } }", schema1, schema);

    String sql = String.format("select location from elasticsearch.\"%s,%s\".%s where contains(location:(Chicago OR \"San Francisco\"~))", alias, alias1, table);
    try {
      test(sql);
      fail("Query should have failed");
    } catch (Exception e) {
      assertTrue(e.getMessage(), e.getMessage().contains("Unable to access a collection of aliases with differing filters."));
    }
  }

  @Test
  public void testMultipleIndex() throws Exception {
    // DX-12162: upstream typo when processing contains
    if (elastic.getMinVersionInCluster().getMajor() == 5) {
      assumeFalse(elastic.getMinVersionInCluster().getMinor() <= 2);
    }

    String schema1 = schema;
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
                    {"San Francisco"},
                    {"San Jose"},
                    {"Oakland"}
            }),
            new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
                    {1},
                    {2},
                    {3},
            })
    };

    elastic.load(schema, table, data);
    schema = schemaName();

    data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
                    {"New York"},
                    {"Chicago"},
                    {"Los Angeles"}
            }),
            new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
                    {1},
                    {2},
                    {3},
            })
    };
    elastic.load(schema, table, data);

    String sql = String.format("select location from elasticsearch.\"%s,%s\".%s where contains(location:(Chicago OR \"San Francisco\"~))", schema1, schema, table);

    testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("location")
            .baselineValues("San Francisco")
            .baselineValues("Chicago")
            .go();
  }

  @Test
  public void testMultipleIndexAggregation() throws Exception {
    // DX-12162: upstream typo when processing contains
    if (elastic.getMinVersionInCluster().getMajor() == 5) {
      assumeFalse(elastic.getMinVersionInCluster().getMinor() <= 2);
    }

    String schema1 = schema;
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
                    {"San Francisco"},
                    {"San Jose"},
                    {"Oakland"}
            }),
            new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
                    {1},
                    {2},
                    {3},
            })
    };

    elastic.load(schema, table, data);
    schema = schemaName();

    data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
                    {"New York"},
                    {"Chicago"},
                    {"Los Angeles"}
            }),
            new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
                    {1},
                    {2},
                    {3},
            })
    };
    elastic.load(schema, table, data);

    String sql = String.format("select count(location), sum(id) from elasticsearch.\"%s,%s\".%s where contains(location:(Chicago OR \"San Francisco\"~))", schema1, schema, table);

    testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("EXPR$0", "EXPR$1")
            .baselineValues(2L, 3L)
            .go();
  }

  @Test
  public void testWildcardIndex() throws Exception {

    String schema1 = "wildcard_schema_1";
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
                    {"San Francisco"},
                    {"San Jose"},
                    {"Oakland"}
            }),
            new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
                    {1},
                    {2},
                    {3},
            })
    };

    elastic.load(schema1, table, data);

    String schema2 = "wildcard_schema_2";
    data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
                    {"New York"},
                    {"Chicago"},
                    {"Los Angeles"}
            }),
            new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
                    {1},
                    {2},
                    {3},
            })
    };
    elastic.load(schema2, table, data);
    String sql = String.format("select location from elasticsearch.\"wildcard*\".%s where id < 2", table);
    testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("location")
            .baselineValues("San Francisco")
            .baselineValues("New York")
            .go();
  }

  @Test
  public void testWildcardIndexAggregation() throws Exception {

    String schema1 = "wildcard_schema_1";
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
                    {"San Francisco"},
                    {"San Jose"},
                    {"Oakland"}
            }),
            new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
                    {1},
                    {2},
                    {3},
            })
    };

    elastic.load(schema1, table, data);

    String schema2 = "wildcard_schema_2";
    data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
                    {"New York"},
                    {"Chicago"},
                    {"Los Angeles"}
            }),
            new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
                    {1},
                    {2},
                    {3},
            })
    };
    elastic.load(schema2, table, data);
    String sql = String.format("select count(location), sum(id) from elasticsearch.\"wildcard*\".%s where id < 2", table);
    testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("EXPR$0", "EXPR$1")
            .baselineValues(2L, 2L)
            .go();
  }

  @Ignore("DX-660")
  @Test
  public void testIndexWithAliasFilterConflict() throws Exception {

    String schema1 = schema;
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
                    {"San Francisco"},
                    {"San Jose"},
                    {"Oakland"}
            }),
            new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
                    {1},
                    {3},
                    {5},
            })
    };

    elastic.load(schema1, table, data);

    String schema2 = schema = schemaName();
    data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
                    {"New York"},
                    {"Chicago"},
                    {"Los Angeles"}
            }),
            new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
                    {2},
                    {4},
                    {6},
            })
    };
    elastic.load(schema2, table, data);

    elastic.aliasWithFilter(alias, "{ \"range\": { \"id\" : { \"lte\" : 4 } } }", schema1, schema2);

    String sql = String.format("select location from elasticsearch.\"%s,%s\".%s where id > 2", alias, schema1, table);
    testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("location")
            .baselineValues("San Jose")
            .baselineValues("Oakland") // This is included because the explicit reference to schema1
            .baselineValues("Chicago")
            .go();
  }

  @Test
  public void testMultipleTypes() throws Exception {
    // DX-11939: No two _type allowed in same mapping since ES 6
    assumeTrue(this.elastic.getMinVersionInCluster().compareTo(ELASTIC_V6) < 0);

    String schema1 = schema;
    String table1 = table;
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
                    {"San Francisco"},
                    {"San Jose"},
                    {"Oakland"}
            }),
            new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
                    {1},
                    {3},
                    {5},
            })
    };

    elastic.load(schema1, table, data);

    data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
                    {"New York"},
                    {"Chicago"},
                    {"Los Angeles"}
            }),
            new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
                    {2},
                    {4},
                    {6},
            })
    };
    table = tableName();
    elastic.load(schema1, table, data);

    String sql = String.format("select location from elasticsearch.%s.\"%s,%s\" where id > 4", schema1, table1, table);
    testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("location")
            .baselineValues("Oakland")
            .baselineValues("Los Angeles")
            .go();
  }

  @Test
  public void testMultipleTypesAggregation() throws Exception {
    // DX-11939: No two _type allowed in same mapping since ES 6
    assumeTrue(this.elastic.getMinVersionInCluster().compareTo(ELASTIC_V6) < 0);

    String schema1 = schema;
    String table1 = table;
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
                    {"San Francisco"},
                    {"San Jose"},
                    {"Oakland"}
            }),
            new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
                    {1},
                    {3},
                    {5},
            })
    };

    elastic.load(schema1, table, data);

    data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
                    {"New York"},
                    {"Chicago"},
                    {"Los Angeles"}
            }),
            new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
                    {2},
                    {4},
                    {6},
            })
    };
    table = tableName();
    elastic.load(schema1, table, data);

    String sql = String.format("select count(location), sum(id) from elasticsearch.%s.\"%s,%s\" where id > 4", schema1, table1, table);
    testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("EXPR$0", "EXPR$1")
            .baselineValues(2L, 11L)
            .go();
  }

  // In elasticsearch, wildcard in type names do not work.  We had it working as a
  // bonus before, but we cleaned up the code and doesn't work anymore.
  @Ignore("DX-661")
  @Test
  public void testMultipleTypesWildcard() throws Exception {

    String schema1 = schema;
    String table1 = "wildcard_table_1";
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
                    {"San Francisco"},
                    {"San Jose"},
                    {"Oakland"}
            }),
            new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
                    {1},
                    {3},
                    {5},
            })
    };

    elastic.load(schema1, table1, data);

    String table2 = "wildcard_table_2";
    data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
                    {"New York"},
                    {"Chicago"},
                    {"Los Angeles"}
            }),
            new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
                    {2},
                    {4},
                    {6},
            })
    };
    table = tableName();
    elastic.load(schema1, table2, data);

    String sql = String.format("select location from elasticsearch.%s.\"wildcard*\" where id > 4", schema1);
    testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("location")
            .baselineValues("Oakland")
            .baselineValues("Los Angeles")
            .go();
  }

  @Test
  public void testDifferentTypes() throws Exception {
    // DX-11939: No two _type allowed in same mapping since ES 6
    assumeTrue(this.elastic.getMinVersionInCluster().compareTo(ELASTIC_V6) < 0);

    String schema1 = schema;
    String table1 = table;
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location_1", TEXT, new Object[][]{
                    {"San Francisco"},
                    {"San Jose"},
                    {"Oakland"}
            }),
            new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
                    {1},
                    {3},
                    {5},
            })
    };

    elastic.load(schema1, table, data);

    data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location_2", TEXT, new Object[][]{
                    {"New York"},
                    {"Chicago"},
                    {"Los Angeles"}
            }),
            new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
                    {2},
                    {4},
                    {6},
            })
    };
    table = tableName();
    elastic.load(schema1, table, data);

    String sql = String.format("select location_1, location_2 from elasticsearch.%s.\"%s,%s\" where id > 4", schema1, table1, table);

    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("location_1", "location_2")
      .baselineValues("Oakland", null)
      .baselineValues(null, "Los Angeles")
      .go();
  }

  @Test
  public void testDifferentTypesAggregation() throws Exception {
    // DX-11939: No two _type allowed in same mapping since ES 6
    assumeTrue(this.elastic.getMinVersionInCluster().compareTo(ELASTIC_V6) < 0);

    String schema1 = schema;
    String table1 = table;
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location_1", TEXT, new Object[][]{
                    {"San Francisco"},
                    {"San Jose"},
                    {"Oakland"}
            }),
            new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
                    {1},
                    {3},
                    {5},
            })
    };

    elastic.load(schema1, table, data);

    data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location_2", TEXT, new Object[][]{
                    {"New York"},
                    {"Chicago"},
                    {"Los Angeles"}
            }),
            new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
                    {2},
                    {4},
                    {6},
            })
    };
    table = tableName();
    elastic.load(schema1, table, data);

    String sql = String.format("select count(location_1) cnt1, count(location_2) as cnt2, sum(id) as total from elasticsearch.%s.\"%s,%s\" where id > 4", schema1, table1, table);
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("cnt1", "cnt2", "total")
      .baselineValues(1l, 1l, 11l)
      .go();
  }

  @Ignore("elasticsearch 2.x checks for same field type across type names within an index")
  @Test
  public void testConflictingTypes() throws Exception {

    String schema1 = schema;
    String table1 = table;
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
                    {"San Francisco"},
                    {"San Jose"},
                    {"Oakland"}
            }),
            new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
                    {1},
                    {3},
                    {5},
            })
    };

    elastic.load(schema1, table, data);

    data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location", INTEGER, new Object[][]{
                    {10},
                    {11},
                    {12}
            }),
            new ElasticsearchCluster.ColumnData("id", INTEGER, new Object[][]{
                    {2},
                    {4},
                    {6},
            })
    };
    table = tableName();
    elastic.load(schema1, table, data);

    String sql = String.format("select location from elasticsearch.%s.\"%s,%s\" where id > 4", schema1, table1, table);
    testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("location")
            .baselineValues(12)
            .baselineValues("Oakland")
            .go();
  }

  private static final String NESTED_TYPE_MAPPING = "/json/nested-type/nested-type-mapping.json";
  private static final String NESTED_TYPE_MAPPING2 = "/json/nested-type/nested-type-mapping2.json";
  private static final String NESTED_TYPE_DATA = "/json/nested-type/data-1/";

  @Test
  public void testNestedConflictingMapping() throws Exception {
    String schema1 = schema;
    elastic.load(schema, table, NESTED_TYPE_MAPPING, NESTED_TYPE_DATA);
    String schema2 = schemaName();

    elastic.load(schema2, table, NESTED_TYPE_MAPPING2, NESTED_TYPE_DATA);

    String alias = "alias1";

    elastic.alias(alias, schema1, schema2);
    String sql = String.format("select * from elasticsearch.%s.%s t", alias, table);
    test(sql);
  }
}
