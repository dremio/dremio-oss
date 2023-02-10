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

import static com.dremio.exec.ExecConstants.ELASTIC_ACTION_RETRIES_VALIDATOR;
import static com.dremio.plugins.elastic.ElasticBaseTestQuery.TestNameGenerator.aliasName;
import static com.dremio.plugins.elastic.ElasticBaseTestQuery.TestNameGenerator.schemaName;
import static com.dremio.plugins.elastic.ElasticBaseTestQuery.TestNameGenerator.tableName;
import static com.dremio.plugins.elastic.ElasticsearchConstants.ES_CONFIG_DEFAULT_BATCH_SIZE;
import static com.dremio.plugins.elastic.ElasticsearchType.BOOLEAN;
import static com.dremio.plugins.elastic.ElasticsearchType.DATE;
import static com.dremio.plugins.elastic.ElasticsearchType.FLOAT;
import static com.dremio.plugins.elastic.ElasticsearchType.GEO_POINT;
import static com.dremio.plugins.elastic.ElasticsearchType.INTEGER;
import static com.dremio.plugins.elastic.ElasticsearchType.KEYWORD;
import static com.dremio.plugins.elastic.ElasticsearchType.NESTED;
import static com.dremio.plugins.elastic.ElasticsearchType.TEXT;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.PlanTestBase;
import com.dremio.QueryTestUtil;
import com.dremio.TestBuilder;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.store.CatalogService;
import com.dremio.options.OptionValue;
import com.dremio.plugins.Version;
import com.dremio.plugins.elastic.ElasticConnectionPool.ElasticConnection;
import com.dremio.plugins.elastic.ElasticsearchCluster.ColumnData;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.ImmutableMap;

public class ElasticBaseTestQuery extends PlanTestBase {
  private static final Logger logger = LoggerFactory.getLogger(ElasticBaseTestQuery.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final String PUSHDOWN_PREFIX = "pushdown\n =";
  protected static final Version ELASTIC_V6 = new Version(6, 0, 0);

  protected ElasticsearchCluster elastic;
  protected String schema;
  protected String table;
  protected String alias;
  protected boolean enable7vFeatures;
  protected boolean enable68vFeatures;

  protected final String [] uidJsonES7 = new String[] {
    "[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "          \"match_all\" : {\n" +
      "            \"boost\" : 1.0\n" +
      "          }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [\n" +
      "      \"_id\",\n" +
      "      \"_type\"\n" +
      "    ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"
  };

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
  public @interface ElasticScrollSize {
    int scrollSize() default ES_CONFIG_DEFAULT_BATCH_SIZE;
  }

  @Override
  public TestBuilder testBuilder() {
    return new TestBuilder(allocator);
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

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE})
  public @interface AllowPushdownNormalizedOrAnalyzedFields {
    boolean enabled() default false;
  }

  @Before
  public void before() throws Exception {
    schema = schemaName();
    table = tableName();
    alias = aliasName();

//    test("alter system set \"exec.enable_union_type\" = true");
  }

  public ElasticConnection getConnection() {
    ElasticsearchStoragePlugin plugin = getSabotContext().getCatalogService().getSource("elasticsearch");
    return plugin.getRandomConnection();
  }

  @Before
  public void setupElastic() throws IOException, InterruptedException {
    setupElasticHelper(false);
  }

  public void setupElasticHelper(boolean forceDoublePrecision) throws IOException, InterruptedException {
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
    boolean sslEnabled = false;
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

    AllowPushdownNormalizedOrAnalyzedFields pushdownAnalyzed =
      this.getClass().getAnnotation(AllowPushdownNormalizedOrAnalyzedFields.class);
    boolean allowPushdownNormalizedOrAnalyzedFields = false;
    if (pushdownAnalyzed != null) {
      allowPushdownNormalizedOrAnalyzedFields = pushdownAnalyzed.enabled();
    }

    getSabotContext().getOptionManager().setOption(OptionValue.createLong(OptionValue.OptionType.SYSTEM, ExecConstants.ELASTIC_ACTION_RETRIES, 3));
    elastic = new ElasticsearchCluster(scrollSize, new Random(), scriptsEnabled, showIDColumn, publishHost, sslEnabled, getSabotContext().getOptionManager().getOption(ELASTIC_ACTION_RETRIES_VALIDATOR), forceDoublePrecision);
    SourceConfig sc = new SourceConfig();
    sc.setName("elasticsearch");
    sc.setConnectionConf(elastic.config(allowPushdownNormalizedOrAnalyzedFields));
    sc.setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY);
    createSourceWithRetry(sc);
    ElasticVersionBehaviorProvider elasticVersionBehaviorProvider = new ElasticVersionBehaviorProvider(elastic.getMinVersionInCluster());
    enable7vFeatures = elasticVersionBehaviorProvider.isEnable7vFeatures();
    enable68vFeatures = elasticVersionBehaviorProvider.isEs68Version();
  }

  @After
  public void removeSource() {
    CatalogServiceImpl service = (CatalogServiceImpl) getSabotContext().getCatalogService();
    service.deleteSource("elasticsearch");
    if (elastic != null) {
      elastic.wipe();
      try {
        elastic.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void load(String schema, String table, ColumnData[] data) throws IOException {
    elastic.load(schema, table, data);
  }

  // Retrying loaddata 3 times in case of scenarios like NotFoundException.
  public void loadWithRetry(String schema, String table, ColumnData[] data) throws InterruptedException {
    int retries = 3;
    while (retries >= 0) {
      try {
        elastic.load(schema, table, data);
        break;
      } catch (Exception e) {
        retries--;
        if (retries >= 0) {
          TimeUnit.SECONDS.sleep(5);
        }
      }
    }
  }

  public ElasticsearchCluster.SearchResults searchResultsWithExpectedCount(int expectedCount) throws Exception {
    ElasticsearchCluster.SearchResults contents = elastic.search(schema, table);
      // If record count is not as expected due to data from earlier test is not cleared then remove and load schema data again.
      if (contents.count != expectedCount && contents.count % expectedCount == 0) {
        removeSource();
        setupElastic();
        elastic.dataFromFile(schema, table, ElasticsearchConstants.NESTED_TYPE_DATA);
        contents = elastic.search(schema, table);
      }
      return contents;
  }

  // Retrying setupElastic 3 times (with delay of 5 seconds) in case of issue with cluster health.
  public void createSourceWithRetry(SourceConfig sc) throws InterruptedException {
    int retries = 3;
    while (retries >= 0) {
      try {
        getSabotContext().getCatalogService().createSourceIfMissingWithThrow(sc);
        break;
      } catch (Exception e) {
        retries--;
        if (retries >= 0) {
          TimeUnit.SECONDS.sleep(5);
        }
      }
    }
  }

  public static ColumnData[] getNullBusinessData() {
    final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("UTC")).withChronology(IsoChronology.INSTANCE);
    final ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("business_id", TEXT, new Object[][]{
        {null},
        {"abcde"},
        {"7890"},
        {"12345"},
        {"xyz"}
      }),
      new ElasticsearchCluster.ColumnData("full_address", TEXT, new Object[][]{
        {"12345 A Street, Cambridge, MA"},
        {null},
        {"987 B Street, San Diego, CA"},
        {"12345 A Street, Cambridge, MA"},
        {"12345 C Avenue, San Francisco, CA"}
      }),
      new ElasticsearchCluster.ColumnData("city", KEYWORD,
        new Object[][]{
          {"Cambridge"},
          {"San Francisco"},
          {null},
          {"Cambridge"},
          {"San Francisco"}
        }),
      new ElasticsearchCluster.ColumnData("city_analyzed", TEXT,
        new Object[][]{
          {"Cambridge"},
          {"San Francisco"},
          {null},
          {"Cambridge"},
          {"San Francisco"}
        }),
      new ElasticsearchCluster.ColumnData("state", KEYWORD,
        new Object[][]{
          {"MA"},
          {"CA"},
          {"CA"},
          {null},
          {"CA"}
        }),
      new ElasticsearchCluster.ColumnData("state_analyzed", TEXT, new Object[][]{
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
      new ElasticsearchCluster.ColumnData("name", TEXT, new Object[][]{
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
        {LocalDateTime.parse("2014-02-10 10:50:42", formatter)},
        {null},
        {LocalDateTime.parse("2014-02-12 10:50:42", formatter)},
        {LocalDateTime.parse("2014-02-11 10:50:42", formatter)},
        {LocalDateTime.parse("2014-02-10 10:50:42", formatter)}
      })
    };
    return data;
  }

  public static ColumnData[] getBusinessData() {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("UTC")).withChronology(IsoChronology.INSTANCE);
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
        new ElasticsearchCluster.ColumnData("business_id", TEXT, new Object[][]{
            {"12345"},
            {"abcde"},
            {"7890"},
            {"12345"},
            {"xyz"}
        }),
        new ElasticsearchCluster.ColumnData("full_address", TEXT, new Object[][]{
            {"12345 A Street, Cambridge, MA"},
            {"987 B Street, San Francisco, CA"},
            {"987 B Street, San Diego, CA"},
            {"12345 A Street, Cambridge, MA"},
            {"12345 C Avenue, San Francisco, CA"}
        }),
        new ElasticsearchCluster.ColumnData("city", KEYWORD,
            new Object[][]{
                {"Cambridge"},
                {"San Francisco"},
                {"San Diego"},
                {"Cambridge"},
                {"San Francisco"}
            }),
        new ElasticsearchCluster.ColumnData("city_analyzed", TEXT,
            new Object[][]{
                {"Cambridge"},
                {"San Francisco"},
                {"San Diego"},
                {"Cambridge"},
                {"San Francisco"}
            }),
        new ElasticsearchCluster.ColumnData("state", KEYWORD,
            new Object[][]{
                {"MA"},
                {"CA"},
                {"CA"},
                {"MA"},
                {"CA"}
            }),
        new ElasticsearchCluster.ColumnData("state_analyzed", TEXT, new Object[][]{
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
        new ElasticsearchCluster.ColumnData("name", TEXT, new Object[][]{
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
        new ElasticsearchCluster.ColumnData("datefield2", DATE, new Object[][] {
            {LocalDateTime.parse("2015-02-10 10:50:42", formatter)},
            {LocalDateTime.parse("2015-02-11 10:50:42", formatter)},
            {LocalDateTime.parse("2015-02-12 10:50:42", formatter)},
            {LocalDateTime.parse("2015-02-11 10:50:42", formatter)},
            {LocalDateTime.parse("2015-02-10 10:50:42", formatter)}
            }),
        new ElasticsearchCluster.ColumnData("datefield", DATE, new Object[][] {
            {LocalDateTime.parse("2014-02-10 10:50:42", formatter)},
            {LocalDateTime.parse("2014-02-11 10:50:42", formatter)},
            {LocalDateTime.parse("2014-02-12 10:50:42", formatter)},
            {LocalDateTime.parse("2014-02-11 10:50:42", formatter)},
            {LocalDateTime.parse("2014-02-10 10:50:42", formatter)}
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
  public void verifyJsonInPlan(String query, String[] jsonExpectedInPlan) throws Exception {
    if(!ElasticsearchCluster.USE_EXTERNAL_ES5) {
      query = QueryTestUtil.normalizeQuery(query);
      verifyJsonInPlanHelper(query, jsonExpectedInPlan, true);
      verifyJsonInPlanHelper(query, jsonExpectedInPlan, false);
    }
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
  public void verifyJsonInPlanHelper(String query, String[] jsonExpectedInPlan, boolean edgeProjectEnabled) throws Exception {
    try {
      if (edgeProjectEnabled) {
        test("ALTER SYSTEM SET " + ExecConstants.ELASTIC_RULES_EDGE_PROJECT.getOptionName() + " = true");
      }
      String plan = getPlanInString("EXPLAIN PLAN for " + query, OPTIQ_FORMAT);
      // find the beginning of the elastic query being pushed down

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
        if (enable7vFeatures) {
          jsonPushdown = jsonPushdown.replaceAll(ElasticsearchConstants.DISABLE_COORD_FIELD, "");
        }
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

  // To get field to select in query based upon ES version. If version is 7 , " _type || '#' || _id " will be used in place of _uid.
  public String getField() {
    if (elastic.getMinVersionInCluster().getMajor() == 7) {
      return "_type  || '#'  || _id";
    }
    return "_uid";
  }

  // To get field with alias (_uid) to select in query based upon ES version. If version is 7 , " _type || '#' || _id " will be used in place of _uid.
  public String getFieldWithAlias() {
    if (elastic.getMinVersionInCluster().getMajor() == 7) {
      return " _type || '#' || _id as _uid ";
    }
    return " _uid ";
  }

  protected String getActualFormat(String format) {
    if(format.startsWith("8")) {
      return format.substring(1);
    }
    return format;
  }

  private static int findJsonEndBoundary(String plan, int indexInPlan) throws IOException {
    // read the json pushdown query with jackson to find it's total length, wasn't sure how to do this with just regex
    // as it will span across a variable number of lines
    try(JsonParser jsonParser = objectMapper.getFactory().createParser(plan.substring(indexInPlan))) {
      JsonToken token = jsonParser.nextToken();
      if (token != JsonToken.START_ARRAY) {
        throw new RuntimeException("Error finding elastic pushdown query JSON in plan text, " +
          "did not find start array as expected, instead found " + token);
      }
      int startEndCounter = 1;
      while (startEndCounter != 0) {
        token = jsonParser.nextToken();
        if (token == JsonToken.START_ARRAY) {
          startEndCounter++;
        } else if (token == JsonToken.END_ARRAY) {
          startEndCounter--;
        }
      }
      long pushdownEndIndexInPlan = jsonParser.getTokenLocation().getCharOffset() + 1;
      return indexInPlan + (int) pushdownEndIndexInPlan;
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
