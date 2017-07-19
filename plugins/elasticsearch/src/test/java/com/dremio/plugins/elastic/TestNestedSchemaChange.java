/*
 * Copyright 2016 Dremio Corporation
 */
package com.dremio.plugins.elastic;

import static com.dremio.TestBuilder.mapOf;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.TestBuilder;
import com.dremio.exec.ExecConstants;

/**
 * Tests for the nested data type.
 */
public class TestNestedSchemaChange extends ElasticBaseTestQuery {

  private static final Logger logger = LoggerFactory.getLogger(TestNestedSchemaChange.class);

  private static final String NEW_NESTED_COLUMN_1 = "/json/nested/new_nested_column/file1.json";
  private static final String NEW_NESTED_COLUMN_2 = "/json/nested/new_nested_column/file2.json";

  @BeforeClass
  public static void enableReattempts() throws Exception {
    test("ALTER SYSTEM SET `" + ExecConstants.ENABLE_REATTEMPTS.getOptionName() + "` = true");
  }

  @Test
  public void testNewNestedColumn() throws Exception {
    for (int i = 0; i < 1000; i++) {
      elastic.dataFromFile(schema, table, NEW_NESTED_COLUMN_1);
    }
    elastic.dataFromFile(schema, table, NEW_NESTED_COLUMN_2);
    logger.info("--> mapping:\n{}", elastic.mapping(schema, table));
    String query = String.format("select a from elasticsearch.%s.%s order by id", schema, table);
    TestBuilder testBuilder = testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("a");
    for (int i = 0; i < 1000; i++) {
      testBuilder.baselineValues(mapOf("b", mapOf("c1", 1L)));
    }
    testBuilder.baselineValues(mapOf("b", mapOf("c2", 2L)));
    testBuilder.go();
  }
}
