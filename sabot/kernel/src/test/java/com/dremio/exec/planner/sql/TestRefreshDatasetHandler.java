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

import static com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants.METADATA_STORAGE_PLUGIN_NAME;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.calcite.sql.SqlNode;
import org.junit.Test;

import com.dremio.PlanTestBase;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.TestTools;
import com.dremio.exec.ExecTest;
import com.dremio.exec.PassthroughQueryObserver;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.physical.HashPrelUtil;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.query.SqlToPlanHandler;
import com.dremio.exec.planner.sql.parser.SqlRefreshDataset;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.rpc.user.security.testing.UserServiceTestImpl;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;
import com.dremio.sabot.rpc.user.UserSession;

/**
 * Unit test for RefreshDatasetHandler
 */
public class TestRefreshDatasetHandler extends PlanTestBase {
  private final String workingPath = TestTools.getWorkingPath();
  private final String path = workingPath + "/src/test/resources/refreshDataset";

  @Test
  public void testQuery() throws Exception {
    test("use dfs_test");
    String sql = "REFRESH DATASET " + getPath(path, "tbl");
    final SabotContext context = getSabotContext();
    final OptionManager optionManager = getSabotContext().getOptionManager();
    optionManager.setOption(OptionValue.createBoolean(OptionValue.OptionType.SYSTEM, "dremio.execution.support_unlimited_splits", true));
    optionManager.setOption(OptionValue.createBoolean(OptionValue.OptionType.SYSTEM, "dremio.iceberg.enabled", true));
    optionManager.setOption(OptionValue.createLong(OptionValue.OptionType.SYSTEM, "planner.slice_target", 1));

    final UserSession session = UserSession.Builder.newBuilder()
            .withSessionOptionManager(
                    new SessionOptionManagerImpl(getSabotContext().getOptionValidatorListing()),
                    optionManager)
            .withUserProperties(UserProtos.UserProperties.getDefaultInstance())
            .withCredentials(UserBitShared.UserCredentials.newBuilder().setUserName(UserServiceTestImpl.TEST_USER_1).build())
            .setSupportComplexTypes(true)
            .build();
    final QueryContext queryContext = spy(new QueryContext(session, context, UserBitShared.QueryId.getDefaultInstance()));
    final FileSystemPlugin<?> metadataPlugin = metadataPlugin(queryContext.getCatalog());

    final CatalogService catalogService = mock(CatalogService.class);
    when(catalogService.getSource(eq(METADATA_STORAGE_PLUGIN_NAME))).thenReturn(metadataPlugin);
    when(catalogService.getManagedSource(eq("dfs"))).thenReturn(context.getCatalogService().getManagedSource("dfs"));
    when(queryContext.getCatalogService()).thenReturn(catalogService);

    final AttemptObserver observer = new PassthroughQueryObserver(ExecTest.mockUserClientConnection(null));
    final SqlConverter converter = new SqlConverter(
            queryContext.getPlannerSettings(),
            queryContext.getOperatorTable(),
            queryContext,
            queryContext.getMaterializationProvider(),
            queryContext.getFunctionRegistry(),
            queryContext.getSession(),
            observer,
            queryContext.getCatalog(),
            queryContext.getSubstitutionProviderFactory(),
            queryContext.getConfig(),
            queryContext.getScanResult(),
            queryContext.getRelMetadataQuerySupplier());
    final SqlNode node = converter.parse(sql);
    final SqlHandlerConfig config = new SqlHandlerConfig(queryContext, converter, observer, null);
    assertTrue("SqlNode is not an instance of SqlRefreshDataset", node instanceof SqlRefreshDataset);

    SqlToPlanHandler handler = ((SqlRefreshDataset) node).toPlanHandler();
    handler.getPlan(config, sql, node);

    String plan = handler.getTextPlan();
    testMatchingPatterns(plan, new String[]{

      // We should have all these operators
      "WriterCommitter",
      "Writer",
      "HashToRandomExchange",
      "TableFunction",
      "RoundRobinExchange",
      "DirListingScan",

      // The operators should be in this order
      "(?s)" +
        "WriterCommitter.*" +
        "Writer.*" +
        "HashToRandomExchange.*" +
        "Project.*" + HashPrelUtil.HASH_EXPR_NAME + ".*" + // HashProject
        "TableFunction.*" +
        "RoundRobinExchange.*" +
        "DirListingScan.*"
    });
  }

  private FileSystemPlugin<?> metadataPlugin(Catalog catalog) {
    StoragePlugin plugin = catalog.getSource("dfs");
    if (plugin instanceof FileSystemPlugin) {
      return (FileSystemPlugin<?>) plugin;
    } else {
      throw UserException.validationError()
              .message("Source identified was invalid type. REFRESH DATASET is only supported on sources that can contain files or folders")
              .build();
    }
  }

  private String getPath(String testPath, String dataset) {
    return String.format("dfs.\"%s/%s\"", testPath, dataset);
  }
}
