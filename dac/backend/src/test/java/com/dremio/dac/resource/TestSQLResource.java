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
package com.dremio.dac.resource;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;

import org.apache.calcite.sql.parser.SqlParserUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.dac.explore.model.AnalyzeRequest;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.SuggestionResponse;
import com.dremio.dac.explore.model.ValidationResponse;
import com.dremio.dac.model.job.QueryError;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.model.sources.UIMetadataPolicy;
import com.dremio.dac.model.spaces.Space;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.service.source.SourceService;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Tests {@link com.dremio.dac.resource.SQLResource} API
 */
public class TestSQLResource extends BaseTestServer {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestSQLResource.class);
  private static final String SOURCE_NAME = "mysrc";
  private static final long DEFAULT_REFRESH_PERIOD = TimeUnit.HOURS.toMillis(4);
  private static final long DEFAULT_GRACE_PERIOD = TimeUnit.HOURS.toMillis(12);
  private static final DatasetPath DATASET_PATH_ONE = new DatasetPath(ImmutableList.of(SOURCE_NAME, "ds1"));
  private static final DatasetPath DATASET_PATH_TWO = new DatasetPath(ImmutableList.of(SOURCE_NAME, "ds2"));
  private static final DatasetPath DATASET_PATH_THREE = new DatasetPath(ImmutableList.of(SOURCE_NAME, "ds3"));

  @Rule
  public final TemporaryFolder folder = new TemporaryFolder();

  protected NamespaceService getNamespaceService() {
    final NamespaceService service = newNamespaceService();
    return Preconditions.checkNotNull(service, "ns service is required");
  }

  protected SourceService getSourceService() {
    final SourceService service = newSourceService();
    return Preconditions.checkNotNull(service, "source service is required");
  }

  public void addPhysicalDataset(final DatasetPath path, final DatasetType type) throws Exception {
    NamespaceKey datasetPath = path.toNamespaceKey();
    final DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setName(datasetPath.getName());
    datasetConfig.setType(type);
    datasetConfig.setPhysicalDataset(new PhysicalDataset());
    getNamespaceService().tryCreatePhysicalDataset(datasetPath, datasetConfig);
  }

  @Before
  public void setup() throws Exception {
    final NASConf nas = new NASConf();
    nas.path = folder.getRoot().getPath();
    SourceUI source = new SourceUI();
    source.setName(SOURCE_NAME);
    source.setCtime(System.currentTimeMillis());
    source.setAccelerationRefreshPeriod(DEFAULT_REFRESH_PERIOD);
    source.setAccelerationGracePeriod(DEFAULT_GRACE_PERIOD);
    // Please note: if this source is ever refreshed, the physical dataset added below will disappear
    source.setMetadataPolicy(UIMetadataPolicy.of(CatalogService.NEVER_REFRESH_POLICY));
    source.setConfig(nas);
    getSourceService().registerSourceWithRuntime(source);
    addPhysicalDataset(DATASET_PATH_ONE, DatasetType.PHYSICAL_DATASET);
    addPhysicalDataset(DATASET_PATH_TWO, DatasetType.PHYSICAL_DATASET);
    addPhysicalDataset(DATASET_PATH_THREE, DatasetType.PHYSICAL_DATASET);

    expectSuccess(getBuilder(getAPIv2().path("space/testSpace")).buildPut(Entity.json(new Space(null, "testSpace", null, null, null, 0, null))), Space.class);
    DatasetPath d1Path = new DatasetPath("testSpace.supplier");
    createDatasetFromSQLAndSave(d1Path, "select s_name, s_phone from cp.\"tpch/supplier.parquet\"", asList("cp"));
  }

  @After
  public void clear() throws Exception {
    clearAllDataExceptUser();
  }

  /**
   * Logs the returned suggestions or validation errors
   *
   * @param advisorResponse       The SuggestionResponse or ValidationResponse containing suggestions or validation errors.
   */
  private <T> void logAdvisorResponse(T advisorResponse) {
    if (advisorResponse == null || !logger.isTraceEnabled()) {
      return;
    }

    StringBuilder sb = new StringBuilder();
    sb.append("SQLAnalyzer response:\n");
    sb.append(advisorResponse.toString());
    logger.trace(sb.toString());
  }

  @Test
  public void testSpaceFullSchemaCompletion() throws Exception {
    final String partialQuery = "SELECT * from ^";
    final SqlParserUtil.StringAndPos stringAndPos = SqlParserUtil.findPos(partialQuery);
    final SuggestionResponse returnedSuggestions = testSuggestSQL(stringAndPos.sql, stringAndPos.cursor, asList("mysrc"));

    logAdvisorResponse(returnedSuggestions);
    assertNotNull(returnedSuggestions);
    assertNotNull(returnedSuggestions.getSuggestions());
    assertEquals(39, returnedSuggestions.getSuggestions().size());
  }

  @Test
  public void testSpaceVDSPartialSchemaCompletion() throws Exception {
    final String partialQuery = "SELECT * from t^";
    final SqlParserUtil.StringAndPos stringAndPos = SqlParserUtil.findPos(partialQuery);
    final SuggestionResponse returnedSuggestions = testSuggestSQL(stringAndPos.sql, stringAndPos.cursor, asList("mysrc"));

    ArrayList<String> expectedTables = Lists.newArrayList();
    expectedTables.addAll(
      asList(
        "INFORMATION_SCHEMA.\"TABLES\"",
        "cp.\"tpch/supplier.parquet\"",
        "sys.threads",
        "testSpace.supplier"));

    logAdvisorResponse(returnedSuggestions);
    assertNotNull(returnedSuggestions);
    assertNotNull(returnedSuggestions.getSuggestions());
    assertEquals(6, returnedSuggestions.getSuggestions().size());
    for (int i = 0; i < 6; i++) {
      SuggestionResponse.Suggestion suggestion = returnedSuggestions.getSuggestions().get(i);
      if (suggestion.getType().equals("TABLE")) {
        assertTrue(expectedTables.contains(suggestion.getName()));
      } else if (suggestion.getType().equals("SCHEMA")) {
        assertEquals("testSpace", suggestion.getName());
      } else if (suggestion.getType().equals("KEYWORD")) {
        assertEquals("TABLE", suggestion.getName());
      }
    }
  }

  @Test
  public void spacePDSPartialSchemaCompletion() throws Exception {
    final String partialQuery = "SELECT * from c^";
    final SqlParserUtil.StringAndPos stringAndPos = SqlParserUtil.findPos(partialQuery);
    final SuggestionResponse returnedSuggestions = testSuggestSQL(stringAndPos.sql, stringAndPos.cursor, asList("mysrc"));

    ArrayList<String> expectedTables = Lists.newArrayList();
    expectedTables.addAll(
      asList(
        "INFORMATION_SCHEMA.CATALOGS",
        "INFORMATION_SCHEMA.COLUMNS",
        "cp.\"tpch/supplier.parquet\""
        ));

    logAdvisorResponse(returnedSuggestions);
    assertNotNull(returnedSuggestions);
    assertNotNull(returnedSuggestions.getSuggestions());
    assertEquals((expectedTables.size() + 1), returnedSuggestions.getSuggestions().size());
    for (int i = 0; i < (expectedTables.size() + 1); i++) {
      SuggestionResponse.Suggestion suggestion = returnedSuggestions.getSuggestions().get(i);
      if (suggestion.getType().equals("TABLE")) {
        assertTrue(expectedTables.contains(suggestion.getName()));
      } else if (suggestion.getType().equals("SCHEMA")) {
        assertEquals("cp", suggestion.getName());
      }
    }
  }

  @Test
  public void testSpaceVDSFullDatasetCompletionNoPeriod() throws Exception {
    final String partialQuery = "SELECT * from testSpace^";
    final SqlParserUtil.StringAndPos stringAndPos = SqlParserUtil.findPos(partialQuery);
    final SuggestionResponse returnedSuggestions = testSuggestSQL(stringAndPos.sql, stringAndPos.cursor, asList("mysrc"));

    logAdvisorResponse(returnedSuggestions);
    assertNotNull(returnedSuggestions);
    assertNotNull(returnedSuggestions.getSuggestions());
    assertEquals(2, returnedSuggestions.getSuggestions().size());
    for (int i = 0; i < 2; i++) {
      SuggestionResponse.Suggestion suggestion = returnedSuggestions.getSuggestions().get(i);
      if (suggestion.getType().equals("TABLE")) {
        assertEquals("testSpace.supplier", suggestion.getName());
      } else if (suggestion.getType().equals("SCHEMA")) {
        assertEquals("testSpace", suggestion.getName());
      }
    }
  }

  @Test
  public void testPDSFullDatasetCompletionNoPeriod() throws Exception {
    final String partialQuery = "SELECT * from cp^";
    final SqlParserUtil.StringAndPos stringAndPos = SqlParserUtil.findPos(partialQuery);
    final SuggestionResponse returnedSuggestions = testSuggestSQL(stringAndPos.sql, stringAndPos.cursor, asList("mysrc"));

    logAdvisorResponse(returnedSuggestions);
    assertNotNull(returnedSuggestions);
    assertNotNull(returnedSuggestions.getSuggestions());
    assertEquals(2, returnedSuggestions.getSuggestions().size());
    for (int i = 0; i < 2; i++) {
      SuggestionResponse.Suggestion suggestion = returnedSuggestions.getSuggestions().get(i);
      if (suggestion.getType().equals("TABLE")) {
        assertEquals("cp.\"tpch/supplier.parquet\"", suggestion.getName());
      } else if (suggestion.getType().equals("SCHEMA")) {
        assertEquals("cp", suggestion.getName());
      }
    }
  }

  @Test
  public void testSpaceVDSFullDatasetCompletionWPeriod() throws Exception {
    final String partialQuery = "SELECT * from testSpace.^";
    final SqlParserUtil.StringAndPos stringAndPos = SqlParserUtil.findPos(partialQuery);
    final SuggestionResponse returnedSuggestions = testSuggestSQL(stringAndPos.sql, stringAndPos.cursor, asList("mysrc"));

    logAdvisorResponse(returnedSuggestions);
    assertNotNull(returnedSuggestions);
    assertNotNull(returnedSuggestions.getSuggestions());
    assertEquals(1, returnedSuggestions.getSuggestions().size());
    SuggestionResponse.Suggestion suggestion = returnedSuggestions.getSuggestions().get(0);
    assertEquals("TABLE", suggestion.getType());
    assertEquals("testSpace.supplier", suggestion.getName());
  }

  @Test
  public void testPDSFullDatasetCompletionWPeriod() throws Exception {
    final String partialQuery = "SELECT * from cp.^";
    final SqlParserUtil.StringAndPos stringAndPos = SqlParserUtil.findPos(partialQuery);
    final SuggestionResponse returnedSuggestions = testSuggestSQL(stringAndPos.sql, stringAndPos.cursor, asList("mysrc"));

    logAdvisorResponse(returnedSuggestions);
    assertNotNull(returnedSuggestions);
    assertNotNull(returnedSuggestions.getSuggestions());
    assertEquals(1, returnedSuggestions.getSuggestions().size());
    SuggestionResponse.Suggestion suggestion = returnedSuggestions.getSuggestions().get(0);
    assertEquals("TABLE", suggestion.getType());
    assertEquals("cp.\"tpch/supplier.parquet\"", suggestion.getName());
  }

  @Test
  public void testSpaceVDSPartialDatasetCompletion() throws Exception {
    final String partialQuery = "SELECT * from testSpace.sup^";
    final SqlParserUtil.StringAndPos stringAndPos = SqlParserUtil.findPos(partialQuery);
    final SuggestionResponse returnedSuggestions = testSuggestSQL(stringAndPos.sql, stringAndPos.cursor, asList("mysrc"));

    logAdvisorResponse(returnedSuggestions);
    assertNotNull(returnedSuggestions);
    assertNotNull(returnedSuggestions.getSuggestions());
    assertEquals(1, returnedSuggestions.getSuggestions().size());
    SuggestionResponse.Suggestion suggestion = returnedSuggestions.getSuggestions().get(0);
    assertEquals("TABLE", suggestion.getType());
    assertEquals("testSpace.supplier", suggestion.getName());
  }

  @Test
  public void testSpacePDSPartialDatasetCompletion() throws Exception {
    final String partialQuery = "SELECT * from cp.t^";
    final SqlParserUtil.StringAndPos stringAndPos = SqlParserUtil.findPos(partialQuery);
    final SuggestionResponse returnedSuggestions = testSuggestSQL(stringAndPos.sql, stringAndPos.cursor, asList("mysrc"));

    logAdvisorResponse(returnedSuggestions);
    assertNotNull(returnedSuggestions);
    assertNotNull(returnedSuggestions.getSuggestions());
    assertEquals(1, returnedSuggestions.getSuggestions().size());
    SuggestionResponse.Suggestion suggestion = returnedSuggestions.getSuggestions().get(0);
    assertEquals("TABLE", suggestion.getType());
    assertEquals("cp.\"tpch/supplier.parquet\"", suggestion.getName());
  }


  @Test
  public void testPartialColumnCompletionWithAlias() throws Exception {
    final String partialQuery = "SELECT t1.s^ from testSpace.supplier t1";
    final SqlParserUtil.StringAndPos stringAndPos = SqlParserUtil.findPos(partialQuery);
    final SuggestionResponse returnedSuggestions = testSuggestSQL(stringAndPos.sql, stringAndPos.cursor, asList("mysrc"));

    ArrayList<String> expectedColumns = Lists.newArrayList();
    expectedColumns.addAll(
      asList(
        "s_name",
        "s_phone"));

    logAdvisorResponse(returnedSuggestions);
    assertNotNull(returnedSuggestions);
    assertNotNull(returnedSuggestions.getSuggestions());
    assertEquals(2, returnedSuggestions.getSuggestions().size());
    for (int i = 0; i < 2; i++) {
      SuggestionResponse.Suggestion suggestion = returnedSuggestions.getSuggestions().get(i);
      if (suggestion.getType().equals("COLUMN")) {
        assertTrue(expectedColumns.contains(suggestion.getName()));
      }
    }
  }

  @Test
  public void testSQLAnalyzeSuggestInfoSchema() throws Exception {
    final String partialQuery = "select * from i^";
    final SqlParserUtil.StringAndPos stringAndPos = SqlParserUtil.findPos(partialQuery);
    final SuggestionResponse returnedSuggestions = testSuggestSQL(stringAndPos.sql, stringAndPos.cursor, asList("@dremio"));

    ArrayList<String> expectedTables = Lists.newArrayList();
    expectedTables.addAll(
      asList(
      "INFORMATION_SCHEMA.CATALOGS",
      "INFORMATION_SCHEMA.COLUMNS",
      "INFORMATION_SCHEMA.SCHEMATA",
      "INFORMATION_SCHEMA.\"TABLES\"",
      "INFORMATION_SCHEMA.VIEWS"));

    logAdvisorResponse(returnedSuggestions);
    assertNotNull(returnedSuggestions);
    assertNotNull(returnedSuggestions.getSuggestions());
    assertEquals(6, returnedSuggestions.getSuggestions().size());

    for (SuggestionResponse.Suggestion suggestion : returnedSuggestions.getSuggestions()) {
      if (suggestion.getType().equals("TABLE")) {
        assertTrue(expectedTables.contains(suggestion.getName()));
      } else if (suggestion.getType().equals("SCHEMA")) {
        assertEquals("INFORMATION_SCHEMA", suggestion.getName());
      }
    }
  }

  @Test
  public void testSuggestFromPartialSchema() throws Exception {
    final String partialQuery = "Select * from m^";
    final SqlParserUtil.StringAndPos stringAndPos = SqlParserUtil.findPos(partialQuery);
    final SuggestionResponse returnedSuggestions = testSuggestSQL(stringAndPos.sql, stringAndPos.cursor, asList("mysrc"));

    ArrayList<String> expectedTables = Lists.newArrayList();
    expectedTables.addAll(
      asList(
        "mysrc.ds1",
        "mysrc.ds2",
        "mysrc.ds3",
        "sys.materializations",
        "sys.memory",
        "\"sys.cache\".mount_points"));

    logAdvisorResponse(returnedSuggestions);
    assertNotNull(returnedSuggestions);
    assertNotNull(returnedSuggestions.getSuggestions());
    assertEquals((expectedTables.size() + 1), returnedSuggestions.getSuggestions().size());
    for (int i = 0; i < (expectedTables.size() + 1); i++) {
      SuggestionResponse.Suggestion suggestion = returnedSuggestions.getSuggestions().get(i);
      if (suggestion.getType().equals("TABLE")) {
        assertTrue(expectedTables.contains(suggestion.getName()));
      } else if (suggestion.getType().equals("SCHEMA")) {
        assertEquals("mysrc", suggestion.getName());
      }
    }
  }

  @Test
  public void testSuggestFromFullSchema() throws Exception {
    final String partialQuery = "Select * from mysrc^";
    final SqlParserUtil.StringAndPos stringAndPos = SqlParserUtil.findPos(partialQuery);
    final SuggestionResponse returnedSuggestions = testSuggestSQL(stringAndPos.sql, stringAndPos.cursor, asList("mysrc"));

    logAdvisorResponse(returnedSuggestions);
    assertNotNull(returnedSuggestions);
    assertNotNull(returnedSuggestions.getSuggestions());
    assertEquals(4, returnedSuggestions.getSuggestions().size());
    for (int i = 0; i < 4; i++) {
      SuggestionResponse.Suggestion suggestion = returnedSuggestions.getSuggestions().get(i);
      if (suggestion.getType().equals("TABLE")) {
        assertEquals(String.format("mysrc.ds%d", i + 1), suggestion.getName());
      } else if (suggestion.getType().equals("SCHEMA")) {
        assertEquals("mysrc", suggestion.getName());
      }
    }
  }

  @Test
  public void testSuggestFromSchemaSeparator() throws Exception {
    final String partialQuery = "Select * from mysrc.^";
    final SqlParserUtil.StringAndPos stringAndPos = SqlParserUtil.findPos(partialQuery);
    final SuggestionResponse returnedSuggestions = testSuggestSQL(stringAndPos.sql, stringAndPos.cursor, new ArrayList<String>());

    logAdvisorResponse(returnedSuggestions);
    assertNotNull(returnedSuggestions);
    assertNotNull(returnedSuggestions.getSuggestions());
    assertEquals(3, returnedSuggestions.getSuggestions().size());
    for (int i = 0; i < 3; i++) {
      SuggestionResponse.Suggestion suggestion = returnedSuggestions.getSuggestions().get(i);
      assertEquals("TABLE", suggestion.getType());
      assertEquals(String.format("mysrc.ds%d", i + 1), suggestion.getName());
    }
  }

  @Test
  public void testSuggestFromPartialDataset() throws Exception {
    final String partialQuery = "Select * from mysrc.d^";
    final SqlParserUtil.StringAndPos stringAndPos = SqlParserUtil.findPos(partialQuery);
    final SuggestionResponse returnedSuggestions = testSuggestSQL(stringAndPos.sql, stringAndPos.cursor, asList("mysrc"));

    logAdvisorResponse(returnedSuggestions);
    assertNotNull(returnedSuggestions);
    assertNotNull(returnedSuggestions.getSuggestions());
    assertEquals(3, returnedSuggestions.getSuggestions().size());
    for (int i = 0; i < 3; i++) {
      SuggestionResponse.Suggestion suggestion = returnedSuggestions.getSuggestions().get(i);
      assertEquals("TABLE", suggestion.getType());
      assertEquals(String.format("mysrc.ds%d", i + 1), suggestion.getName());
    }
  }

  @Test // Could improve to suggest Dremio specific keywords
  public void testSuggestSelectList() throws Exception {
    final String partialQuery = "Select ^ from mysrc.ds1";
    final SqlParserUtil.StringAndPos stringAndPos = SqlParserUtil.findPos(partialQuery);
    final SuggestionResponse returnedSuggestions = testSuggestSQL(stringAndPos.sql, stringAndPos.cursor, asList("mysrc"));

    logAdvisorResponse(returnedSuggestions);
    assertNotNull(returnedSuggestions);
    assertNotNull(returnedSuggestions.getSuggestions());
    for (int i = 0; i < returnedSuggestions.getSuggestions().size(); i++) {
      assertEquals("KEYWORD", returnedSuggestions.getSuggestions().get(i).getType());
    }
  }

  @Test
  public void testSuggestColumn() throws Exception {
    final String partialQuery = "SELECT t.^ FROM testSpace.supplier t";
    final SqlParserUtil.StringAndPos stringAndPos = SqlParserUtil.findPos(partialQuery);
    final SuggestionResponse returnedSuggestions = testSuggestSQL(stringAndPos.sql, stringAndPos.cursor, asList("cp"));

    ArrayList<String> expectedColumns = Lists.newArrayList();
    expectedColumns.addAll(
      asList(
        "s_name",
        "s_phone"));

    logAdvisorResponse(returnedSuggestions);
    assertNotNull(returnedSuggestions);
    assertNotNull(returnedSuggestions.getSuggestions());
    assertEquals(3, returnedSuggestions.getSuggestions().size());
    for (SuggestionResponse.Suggestion suggestion : returnedSuggestions.getSuggestions()) {
      if (suggestion.getType().equals("COLUMN")) {
        assertTrue(expectedColumns.contains(suggestion.getName()));
      } else if (suggestion.getType().equals("KEYWORD")) {
        assertEquals("*", suggestion.getName());
      }
    }
  }

  @Test // Suggestions for partial require update to Calcite
  public void testSuggestColumnPartial() throws Exception {
    final String partialQuery = "SELECT t.s^ FROM testSpace.supplier t";
    final SqlParserUtil.StringAndPos stringAndPos = SqlParserUtil.findPos(partialQuery);
    final SuggestionResponse returnedSuggestions = testSuggestSQL(stringAndPos.sql, stringAndPos.cursor, asList("cp"));

    ArrayList<String> expectedColumns = Lists.newArrayList();
    expectedColumns.addAll(
      asList(
        "s_name",
        "s_phone"));

    logAdvisorResponse(returnedSuggestions);
    assertNotNull(returnedSuggestions);
    assertNotNull(returnedSuggestions.getSuggestions());
    assertEquals(2, returnedSuggestions.getSuggestions().size());
    for (SuggestionResponse.Suggestion suggestion : returnedSuggestions.getSuggestions()) {
        assertTrue(expectedColumns.contains(suggestion.getName()));
    }
  }

  @Test // Range can be improved
  public void testErrorUnrecognizedTable() throws Exception {
    final String partialQuery = "Select * from m^";
    final SqlParserUtil.StringAndPos stringAndPos = SqlParserUtil.findPos(partialQuery);
    final ValidationResponse returnedSuggestions = testValidateSQL(stringAndPos.sql, stringAndPos.cursor, asList("@dremio"));

    logAdvisorResponse(returnedSuggestions);
    assertNotNull(returnedSuggestions);
    assertNotNull(returnedSuggestions.getErrors());
    QueryError firstError =  returnedSuggestions.getErrors().get(0);
    assertEquals("Table 'M' not found", firstError.getMessage());
    assertEquals(new QueryError.Range(1,15,2,16), firstError.getRange());
  }

  @Test // Error message identical to current. (unrecognized * intead of missing keyword FROM) Can be improved.
  public void testErrorIncompleteFrom() throws Exception {
    final String partialQuery = "Select * fro^";
    final SqlParserUtil.StringAndPos stringAndPos = SqlParserUtil.findPos(partialQuery);
    final ValidationResponse returnedSuggestions = testValidateSQL(stringAndPos.sql, stringAndPos.cursor, asList("@dremio"));

    logAdvisorResponse(returnedSuggestions);
    assertNotNull(returnedSuggestions);
    assertNotNull(returnedSuggestions.getErrors());
    QueryError firstError =  returnedSuggestions.getErrors().get(0);
    assertEquals("Unknown identifier '*'", firstError.getMessage());
    assertEquals(new QueryError.Range(1,8,2,9), firstError.getRange());
  }

  @Test // Current error-handling wraps this error in a generic parse error.
  public void testErrorIncompleteSelect() throws Exception {
    final String partialQuery = "Sel^";
    final SqlParserUtil.StringAndPos stringAndPos = SqlParserUtil.findPos(partialQuery);
    final ValidationResponse returnedSuggestions = testValidateSQL(stringAndPos.sql, stringAndPos.cursor, asList("@dremio"));

    logAdvisorResponse(returnedSuggestions);
    assertNotNull(returnedSuggestions);
    assertNotNull(returnedSuggestions.getErrors());
    QueryError firstError =  returnedSuggestions.getErrors().get(0);
    assertEquals("Non-query expression encountered in illegal context", firstError.getMessage());
    assertEquals(new QueryError.Range(1,1,2,4), firstError.getRange());
  }

  @Test
  public void testErrorUnrecognizedColumn() throws Exception {
    final String partialQuery = "SELECT testCol^ FROM testSpace.supplier";
    final SqlParserUtil.StringAndPos stringAndPos = SqlParserUtil.findPos(partialQuery);
    final ValidationResponse returnedSuggestions = testValidateSQL(stringAndPos.sql, stringAndPos.cursor, asList("cp"));

    logAdvisorResponse(returnedSuggestions);
    assertNotNull(returnedSuggestions);
    assertNotNull(returnedSuggestions.getErrors());
    QueryError firstError =  returnedSuggestions.getErrors().get(0);
    assertEquals("Column 'TESTCOL' not found in any table", firstError.getMessage());
    assertEquals(new QueryError.Range(1,8,2,15), firstError.getRange());
  }

  public SuggestionResponse testSuggestSQL(String queryString, int cursorPos, List<String> context) throws Exception {
    final String endpoint = "/sql/analyze/suggest";

    return expectSuccess(
      getBuilder(getAPIv2().path(endpoint)).buildPost(Entity.entity(new AnalyzeRequest(queryString, context, cursorPos), MediaType.APPLICATION_JSON_TYPE)),
      SuggestionResponse.class
    );
  }

  public ValidationResponse testValidateSQL(String queryString, int cursorPos, List<String> context) throws Exception {
    final String endpoint = "/sql/analyze/validate";

    return expectSuccess(
      getBuilder(getAPIv2().path(endpoint)).buildPost(Entity.entity(new AnalyzeRequest(queryString, context, cursorPos), MediaType.APPLICATION_JSON_TYPE)),
      ValidationResponse.class
    );
  }
}
