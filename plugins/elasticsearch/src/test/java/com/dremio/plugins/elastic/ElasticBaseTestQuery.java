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
package com.dremio.plugins.elastic;

import static com.dremio.plugins.elastic.ElasticBaseTestQuery.TestNameGenerator.aliasName;
import static com.dremio.plugins.elastic.ElasticBaseTestQuery.TestNameGenerator.schemaName;
import static com.dremio.plugins.elastic.ElasticBaseTestQuery.TestNameGenerator.tableName;
import static com.dremio.plugins.elastic.ElasticsearchConstants.ES_CONFIG_DEFAULT_BATCH_SIZE;
import static com.dremio.plugins.elastic.ElasticsearchType.BOOLEAN;
import static com.dremio.plugins.elastic.ElasticsearchType.DATE;
import static com.dremio.plugins.elastic.ElasticsearchType.FLOAT;
import static com.dremio.plugins.elastic.ElasticsearchType.GEO_POINT;
import static com.dremio.plugins.elastic.ElasticsearchType.INTEGER;
import static com.dremio.plugins.elastic.ElasticsearchType.NESTED;
import static com.dremio.plugins.elastic.ElasticsearchType.STRING;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.junit.AfterClass;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.PlanTestBase;
import com.dremio.QueryTestUtil;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.fn.impl.DateFunctionsUtils;
import com.dremio.plugins.elastic.ElasticsearchCluster.ColumnData;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.ImmutableMap;

public class ElasticBaseTestQuery extends PlanTestBase {

  private static final Logger logger = LoggerFactory.getLogger(ElasticBaseTestQuery.class);

  protected static ElasticsearchCluster elastic = null;

  protected String schema;
  protected String table;
  protected String alias;

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE})
  public @interface ElasticSSL {

    /**
     * enable ssl proxy and publishes that port instead
     * <p/>
     */
    boolean enabled() default true;
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE})
  public @interface ElasticSize {

    /**
     * Sets the number of elasticsearch nodes to create
     * <p/>
     */
    int nodes() default 1;
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE})
  public @interface ElasticScrollSize {
    int scrollSize() default ES_CONFIG_DEFAULT_BATCH_SIZE;
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE})
  public @interface ScriptsEnabled {

    /**
     * enable scripts
     * <p/>
     */
    boolean enabled() default true;
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE})
  public @interface ShowIDColumn {

    /**
     * Sets the number of elasticsearch nodes to create
     * <p/>
     */
    boolean enabled() default false;
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE})
  public @interface PublishHost {
    boolean enabled() default false;
  }

  @Before
  public void before() throws Exception {
    if (elastic == null) {
      ElasticSize clusterSizeAnnotation = this.getClass().getAnnotation(ElasticSize.class);
      int elasticSize = 1;
      if (clusterSizeAnnotation != null) {
        elasticSize = clusterSizeAnnotation.nodes();
      }
      ScriptsEnabled scriptEnabledAnnotation = this.getClass().getAnnotation(ScriptsEnabled.class);
      boolean scriptsEnabled = true;
      if (scriptEnabledAnnotation != null) {
        scriptsEnabled = scriptEnabledAnnotation.enabled();
      }
      PublishHost publishHostAnnotation = this.getClass().getAnnotation(PublishHost.class);
      boolean publishHost = false;
      if (publishHostAnnotation != null) {
        publishHost = publishHostAnnotation.enabled();
      }
      ElasticSSL sslAnnotation = this.getClass().getAnnotation(ElasticSSL.class);
      boolean sslEnabled = false; // one can actually turn SSL on globally by changing this to (elasticSize == 1) by default
      if (sslAnnotation != null) {
        sslEnabled = sslAnnotation.enabled();
      }
      ShowIDColumn showIDColumnAnnotation = this.getClass().getAnnotation(ShowIDColumn.class);
      boolean showIDColumn = false;
      if (showIDColumnAnnotation != null) {
        showIDColumn = showIDColumnAnnotation.enabled();
      }
      ElasticScrollSize elasticScrollSize = this.getClass().getAnnotation(ElasticScrollSize.class);
      int scrollSize = ES_CONFIG_DEFAULT_BATCH_SIZE;
      if (elasticScrollSize != null) {
        scrollSize = elasticScrollSize.scrollSize();
      }

      elastic = new ElasticsearchCluster(elasticSize, scrollSize, new Random(), scriptsEnabled, showIDColumn, publishHost, sslEnabled);
      getSabotContext().getStorage()
        .createOrUpdate(ElasticsearchStoragePluginConfig.NAME, elastic.config(), true);

    }

    schema = schemaName();
    table = tableName();
    alias = aliasName();

//    test("alter system set `exec.enable_union_type` = true");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (elastic != null) {
      try {
        elastic.wipe();
        elastic.close();
        elastic = null;
      } catch (Throwable t) {
        logger.error("Error shutting down elasticsearch cluster", t);
        elastic = null;
      }
    }
  }

  public void load(String schema, String table, ColumnData[] data) throws IOException {
    elastic.load(schema, table, data);
  }

  public static ColumnData[] getNullBusinessData() {
    final DateTimeFormatter formatter = DateFunctionsUtils.getFormatterForFormatString("YYYY-MM-DD HH:MI:SS").withZone(DateTimeZone.UTC);
    final ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("business_id", STRING, new Object[][]{
        {null},
        {"abcde"},
        {"7890"},
        {"12345"},
        {"xyz"}
      }),
      new ElasticsearchCluster.ColumnData("full_address", STRING, new Object[][]{
        {"12345 A Street, Cambridge, MA"},
        {null},
        {"987 B Street, San Diego, CA"},
        {"12345 A Street, Cambridge, MA"},
        {"12345 C Avenue, San Francisco, CA"}
      }),
      new ElasticsearchCluster.ColumnData("city", STRING, ImmutableMap.of("index", "not_analyzed"),
        new Object[][]{
          {"Cambridge"},
          {"San Francisco"},
          {null},
          {"Cambridge"},
          {"San Francisco"}
        }),
      new ElasticsearchCluster.ColumnData("city_analyzed", STRING,
        new Object[][]{
          {"Cambridge"},
          {"San Francisco"},
          {null},
          {"Cambridge"},
          {"San Francisco"}
        }),
      new ElasticsearchCluster.ColumnData("state", STRING, ImmutableMap.of("index", "not_analyzed"),
        new Object[][]{
          {"MA"},
          {"CA"},
          {"CA"},
          {null},
          {"CA"}
        }),
      new ElasticsearchCluster.ColumnData("state_analyzed", STRING, new Object[][]{
        {"MA"},
        {"CA"},
        {"CA"},
        {null},
        {"CA"}
      }),
      new ElasticsearchCluster.ColumnData("review_count", INTEGER, new Object[][]{
        {11},
        {22},
        {33},
        {11},
        {null}
      }),
      new ElasticsearchCluster.ColumnData("stars", FLOAT, new Object[][]{
        {null},
        {3.5f},
        {5.0f},
        {4.5f},
        {1}
      }),
      new ElasticsearchCluster.ColumnData("location_field", GEO_POINT, new Object[][]{
        {ImmutableMap.of("lat", 11, "lon", 11), ImmutableMap.of("lat", -11, "lon", -11)},
        {ImmutableMap.of("lat", 22, "lon", 22), ImmutableMap.of("lat", -22, "lon", -22)},
        {null},
        {ImmutableMap.of("lat", 44, "lon", 44), ImmutableMap.of("lat", -44, "lon", -44)},
        {ImmutableMap.of("lat", 55, "lon", 55), ImmutableMap.of("lat", -55, "lon", -55)}
      }),
      new ElasticsearchCluster.ColumnData("name", STRING, new Object[][]{
        {"Store in Cambridge"},
        {"Store in San Francisco"},
        {"Store in San Diego"},
        {null},
        {"New store in San Francisco"},

      }),
      new ElasticsearchCluster.ColumnData("open", BOOLEAN, new Object[][] {
        {true},
        {true},
        {false},
        {true},
        {null}
      }),
      //withZoneRetainFields(DateTimeZone.getDefault())
      new ElasticsearchCluster.ColumnData("datefield", DATE, new Object[][] {
        {formatter.parseLocalDateTime("2014-02-10 10:50:42")},
        {null},
        {formatter.parseLocalDateTime("2014-02-12 10:50:42")},
        {formatter.parseLocalDateTime("2014-02-11 10:50:42")},
        {formatter.parseLocalDateTime("2014-02-10 10:50:42")}
      })
    };
    return data;
  }

  public static ColumnData[] getBusinessData() {
    DateTimeFormatter formatter = DateFunctionsUtils.getFormatterForFormatString("YYYY-MM-DD HH:MI:SS").withZone(DateTimeZone.UTC);
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
        new ElasticsearchCluster.ColumnData("business_id", STRING, new Object[][]{
            {"12345"},
            {"abcde"},
            {"7890"},
            {"12345"},
            {"xyz"}
        }),
        new ElasticsearchCluster.ColumnData("full_address", STRING, new Object[][]{
            {"12345 A Street, Cambridge, MA"},
            {"987 B Street, San Francisco, CA"},
            {"987 B Street, San Diego, CA"},
            {"12345 A Street, Cambridge, MA"},
            {"12345 C Avenue, San Francisco, CA"}
        }),
        new ElasticsearchCluster.ColumnData("city", STRING, ImmutableMap.of("index", "not_analyzed"),
            new Object[][]{
                {"Cambridge"},
                {"San Francisco"},
                {"San Diego"},
                {"Cambridge"},
                {"San Francisco"}
            }),
        new ElasticsearchCluster.ColumnData("city_analyzed", STRING,
            new Object[][]{
                {"Cambridge"},
                {"San Francisco"},
                {"San Diego"},
                {"Cambridge"},
                {"San Francisco"}
            }),
        new ElasticsearchCluster.ColumnData("state", STRING, ImmutableMap.of("index", "not_analyzed"),
            new Object[][]{
                {"MA"},
                {"CA"},
                {"CA"},
                {"MA"},
                {"CA"}
            }),
        new ElasticsearchCluster.ColumnData("state_analyzed", STRING, new Object[][]{
            {"MA"},
            {"CA"},
            {"CA"},
            {"MA"},
            {"CA"}
        }),
        new ElasticsearchCluster.ColumnData("review_count", INTEGER, new Object[][]{
            {11},
            {22},
            {33},
            {11},
            {1}
        }),
        new ElasticsearchCluster.ColumnData("stars", FLOAT, new Object[][]{
            {4.5f},
            {3.5f},
            {5.0f},
            {4.5f},
            {1}
        }),
        new ElasticsearchCluster.ColumnData("location_field", GEO_POINT, new Object[][]{
            {ImmutableMap.of("lat", 11, "lon", 11), ImmutableMap.of("lat", -11, "lon", -11)},
            {ImmutableMap.of("lat", 22, "lon", 22), ImmutableMap.of("lat", -22, "lon", -22)},
            {ImmutableMap.of("lat", 33, "lon", 33), ImmutableMap.of("lat", -33, "lon", -33)},
            {ImmutableMap.of("lat", 44, "lon", 44), ImmutableMap.of("lat", -44, "lon", -44)},
            {ImmutableMap.of("lat", 55, "lon", 55), ImmutableMap.of("lat", -55, "lon", -55)}
        }),
        new ElasticsearchCluster.ColumnData("test_map", NESTED, new Object[][]{
            {ImmutableMap.of("lat", 11, "lon", 11)},
            {ImmutableMap.of("lat", 22, "lon", 22)},
            {ImmutableMap.of("lat", 33, "lon", 33)},
            {ImmutableMap.of("lat", 44, "lon", 44)},
            {ImmutableMap.of("lat", 55, "lon", 55)}
        }),
        new ElasticsearchCluster.ColumnData("name", STRING, new Object[][]{
            {"Store in Cambridge"},
            {"Store in San Francisco"},
            {"Store in San Diego"},
            {"Same store in Cambridge"},
            {"New store in San Francisco"},

        }),
        new ElasticsearchCluster.ColumnData("open", BOOLEAN, new Object[][] {
            {true},
            {true},
            {false},
            {true},
            {false}
        }),
        new ElasticsearchCluster.ColumnData("datefield", DATE, new Object[][] {
            {formatter.parseLocalDateTime("2014-02-10 10:50:42")},
            {formatter.parseLocalDateTime("2014-02-11 10:50:42")},
            {formatter.parseLocalDateTime("2014-02-12 10:50:42")},
            {formatter.parseLocalDateTime("2014-02-11 10:50:42")},
            {formatter.parseLocalDateTime("2014-02-10 10:50:42")}
        })
    };
    return data;
  }

  /**
   * Search for Json blobs (used for representing elastic pushdown queries) in the query plan.
   *
   * If multiple json values are given they must be provided in the order the appear in the plan.
   *
   * This method can only be used for checking elastic plans as it looks for the string "pushdown: ="
   * to find the JSON in the plan.
   *
   * The unused third parameter gives this method the same signature as the similar
   * testPlanMatchingPatterns() method that was used for these kinds of tests before this elastic
   * specific variant was introduced. Probably not the best reason, but it was included to
   * reduce the clutter from the patch that switched the tests to use this method.
   */
  public static void verifyJsonInPlan(String query, String[] jsonExpectedInPlan) throws Exception {
    query = QueryTestUtil.normalizeQuery(query);
    verifyJsonInPlanHelper(query, jsonExpectedInPlan, true);
    verifyJsonInPlanHelper(query, jsonExpectedInPlan, false);
  }

  /**
   * Shim to run some plan validation against older test definitions before we disabled edge project pushdown.
   *
   * Much of our coverage on groovy script pushdowns was in project lists. Rather than re-write all the tests
   * this shim allows easily running a check for full pushdown plans, or conditionally checking that the
   * plan does not a project list if one should not be present.
   *
   * @param query - SQL query to check the plan for
   * @param jsonExpectedInPlan JSON pushdowns that should be found in plan, if edgeProjectEnabled flag is set
   *                           then these complete fragments are not checked for existence in the plan in all cases.
   *                           If the expected plan contains the word "includes" which provides a list of columns to
   *                           project in an elastic query, this method will only ensure that the pushdown given
   *                           in the explain plan lacks an "include" list of columns
   * @param edgeProjectEnabled set to true if the plan fragments should be matched exactly, generally for validating
   *                           groovy scripts in legacy tests
   * @throws Exception
   */
  public static void verifyJsonInPlanHelper(String query, String[] jsonExpectedInPlan, boolean edgeProjectEnabled) throws Exception {
    try {
      if (edgeProjectEnabled) {
        test("ALTER SYSTEM SET " + ExecConstants.ELASTIC_RULES_EDGE_PROJECT.getOptionName() + " = true");
      }
      String plan = getPlanInString("EXPLAIN PLAN for " + query, OPTIQ_FORMAT);
      // find the beginning of the elastic query being pushed down
      final String PUSHDOWN_PREFIX = "pushdown\n =";

      int indexInPlan = 0;
      int expectedJsonBlobIndex = 0;
      for (; expectedJsonBlobIndex < jsonExpectedInPlan.length; expectedJsonBlobIndex++) {

        // find the pushdown prefix string starting from the position of the last pushdown found
        int positionOfPushdownInPlan = plan.indexOf(PUSHDOWN_PREFIX, indexInPlan);
        if (positionOfPushdownInPlan == -1) {
          break;
        }

        indexInPlan = positionOfPushdownInPlan + PUSHDOWN_PREFIX.length();
        int pushdownEndIndexInPlan = findJsonEndBoundary(plan, indexInPlan);
        String jsonPushdown = plan.substring(indexInPlan, pushdownEndIndexInPlan);
        indexInPlan = pushdownEndIndexInPlan;
        String expectedJson = jsonExpectedInPlan[expectedJsonBlobIndex];
        if (!edgeProjectEnabled) {
          // check the expected plan for a projection list, this should be a signal that it is an older test
          // because we are now avoiding having elastic do final projections
          // if there is one, try to do the explain plan and check for an empty project list, or no project list
          // at all
          if (expectedJson.contains("includes")) {
            String toFind = "\"includes\"\\s*:\\s*\\[\\s*\\]";
            if (!jsonPushdown.matches(toFind) && jsonPushdown.contains("\"includes\"")) {
              throw new RuntimeException("Found a projection list in pushdown, " +
                "when edge projects should be disabled, plan was:\n" + jsonPushdown);
            }
          } else if (expectedJson.contains("\"script_fields\"")) {
            if (jsonPushdown.contains("\"script_fields\"")) {
              throw new RuntimeException("Found a projection list with scripts in pushdown, " +
                "when edge projects should be disabled, plan was:\n" + jsonPushdown);
            }
          }
        } else {
          // the line that starts the json pushdown has an '=', many of the tests include it, just strip it off for the comparison
          if (expectedJson.charAt(0) == '=') {
            expectedJson = expectedJson.substring(1);
          }
          compareJson(expectedJson, jsonPushdown);
        }
      }
      if (expectedJsonBlobIndex != jsonExpectedInPlan.length) {
        throw new RuntimeException("Error finding elastic pushdown query JSON in plan text\n" +
          "Could not find this pushdown in the plan:\n" + jsonExpectedInPlan[expectedJsonBlobIndex] +
          "\n\nActual plan:\n" + plan);
      }
    } finally {
      test("ALTER SYSTEM SET " + ExecConstants.ELASTIC_RULES_EDGE_PROJECT.getOptionName() + " = false");
    }
  }

  private static int findJsonEndBoundary(String plan, int indexInPlan) throws IOException {
    // read the json pushdown query with jackson to find it's total length, wasn't sure how to do this with just regex
    // as it will span across a variable number of lines
    ObjectMapper map = new ObjectMapper(); //for later inner object data binding
    JsonParser p = map.getFactory().createParser(plan.substring(indexInPlan));
    JsonToken token = p.nextToken();
    if (token != JsonToken.START_ARRAY) {
      throw new RuntimeException("Error finding elastic pushdown query JSON in plan text, " +
        "did not find start array as expected, instead found " + token);
    }
    int startEndCounter = 1;
    while (startEndCounter != 0) {
      token = p.nextToken();
      if (token == JsonToken.START_ARRAY) {
        startEndCounter++;
      } else if (token == JsonToken.END_ARRAY) {
        startEndCounter--;
      }
    }
    long pushdownEndIndexInPlan = p.getTokenLocation().getCharOffset() + 1;

    return indexInPlan + (int) pushdownEndIndexInPlan;
  }

  public static void compareJson(String expected, String actual) throws IOException {
    if(ElasticsearchCluster.USE_EXTERNAL_ES5){
      // ignore json comparison for now
      return;
    }

    ObjectMapper m = new ObjectMapper();
    JsonNode expectedRootNode = m.readTree(expected);
    JsonNode actualRootNode = m.readTree(actual);
    if (!expectedRootNode.equals(actualRootNode)) {
      ObjectWriter writer = m.writerWithDefaultPrettyPrinter();
      String message = String.format("Comparison between JSON values failed.\nExpected:\n%s\nActual:\n%s", expected, actual);
      // assertEquals gives a better diff
      assertEquals(message, writer.writeValueAsString(expectedRootNode), writer.writeValueAsString(actualRootNode));
      throw new RuntimeException(message);
    }
  }

  public static class TestNameGenerator {

    private static final AtomicInteger schemaNameGeneration = new AtomicInteger(0);
    private static final AtomicInteger tableNameGeneration = new AtomicInteger(0);
    private static final AtomicInteger aliasNameGeneration = new AtomicInteger(0);

    public static String schemaName() {
      return "test_schema_" + schemaNameGeneration.incrementAndGet();
    }

    public static String tableName() {
      return "test_table_" + tableNameGeneration.incrementAndGet();
    }

    public static String aliasName() {
      return "alias_" + aliasNameGeneration.incrementAndGet();
    }
  }
}
