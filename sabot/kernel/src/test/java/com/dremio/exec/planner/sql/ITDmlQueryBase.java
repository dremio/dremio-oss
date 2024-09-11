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

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import com.dremio.BaseTestQuery;
import com.dremio.common.utils.SqlUtils;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.ExecTest;
import com.dremio.exec.PassthroughQueryObserver;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.sabot.rpc.user.UserSession;
import java.io.IOException;
import java.util.UUID;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.OutputFile;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

public class ITDmlQueryBase extends BaseTestQuery {
  protected ParserConfig parserConfig = new ParserConfig(ParserConfig.QUOTING, 100);
  protected static SqlConverter converter;
  protected static SqlDialect DREMIO_DIALECT =
      new SqlDialect(
          SqlDialect.DatabaseProduct.UNKNOWN,
          "Dremio",
          Character.toString(SqlUtils.QUOTE),
          NullCollation.FIRST);

  // ===========================================================================
  // Test class and Test cases setUp and tearDown
  // ===========================================================================
  @BeforeClass
  public static void setUp() throws Exception {
    SabotContext context = getSabotContext();

    UserSession session =
        UserSession.Builder.newBuilder()
            .withSessionOptionManager(
                new SessionOptionManagerImpl(getSabotContext().getOptionValidatorListing()),
                getSabotContext().getOptionManager())
            .withUserProperties(UserProtos.UserProperties.getDefaultInstance())
            .withCredentials(
                UserBitShared.UserCredentials.newBuilder().setUserName(SYSTEM_USERNAME).build())
            .setSupportComplexTypes(true)
            .build();

    final QueryContext queryContext =
        new QueryContext(session, context, UserBitShared.QueryId.getDefaultInstance());
    queryContext.setGroupResourceInformation(context.getClusterResourceInformation());
    final AttemptObserver observer =
        new PassthroughQueryObserver(ExecTest.mockUserClientConnection(null));

    converter =
        new SqlConverter(
            queryContext.getPlannerSettings(),
            queryContext.getOperatorTable(),
            queryContext,
            queryContext.getMaterializationProvider(),
            queryContext.getFunctionRegistry(),
            queryContext.getSession(),
            observer,
            queryContext.getSubstitutionProviderFactory(),
            queryContext.getConfig(),
            queryContext.getScanResult(),
            queryContext.getRelMetadataQuerySupplier());
  }

  @BeforeClass
  public static void beforeClass() {
    setSystemOption(ExecConstants.ENABLE_ICEBERG_VACUUM, "true");
    setSystemOption(ExecConstants.ENABLE_ICEBERG_VACUUM_REMOVE_ORPHAN_FILES, "true");
    setSystemOption(ExecConstants.ENABLE_ICEBERG_SORT_ORDER, "true");
  }

  @AfterClass
  public static void afterClass() {
    setSystemOption(
        ExecConstants.ENABLE_ICEBERG_VACUUM,
        ExecConstants.ENABLE_ICEBERG_VACUUM.getDefault().getBoolVal().toString());
    setSystemOption(
        ExecConstants.ENABLE_ICEBERG_VACUUM_REMOVE_ORPHAN_FILES,
        ExecConstants.ENABLE_ICEBERG_VACUUM_REMOVE_ORPHAN_FILES
            .getDefault()
            .getBoolVal()
            .toString());
    setSystemOption(
        ExecConstants.ENABLE_ICEBERG_SORT_ORDER,
        ExecConstants.ENABLE_ICEBERG_SORT_ORDER.getDefault().getBoolVal().toString());
  }

  @Before
  public void before() throws Exception {
    // Note: dfs_hadoop is immutable.
    test("USE dfs_hadoop");
  }

  protected static void parseAndValidateSqlNode(String query, String expected) throws Exception {
    if (converter == null) {
      setUp();
    }
    final SqlNode node = converter.parse(query);
    SqlPrettyWriter writer = new SqlPrettyWriter(DREMIO_DIALECT);
    node.unparse(writer, 0, 0);
    String actual = writer.toString();
    Assert.assertEquals(expected.toLowerCase(), actual.toLowerCase());
  }

  protected static ManifestFile writeManifestFile(Table icebergTable, int noOfDataFiles)
      throws IOException {
    final OutputFile manifestLocation =
        icebergTable
            .io()
            .newOutputFile(
                String.format(
                    "%s/metadata/%s-mx.avro", icebergTable.location(), UUID.randomUUID()));
    ManifestWriter<DataFile> writer = ManifestFiles.write(icebergTable.spec(), manifestLocation);

    for (int i = 0; i < noOfDataFiles; i++) {
      writer.add(
          DataFiles.builder(icebergTable.spec())
              .withPath(String.format("/data/fake-%d.parquet", i))
              .withFormat(FileFormat.PARQUET)
              .withFileSizeInBytes(20 * 1024 * 1024L)
              .withRecordCount(1_000_000)
              .build());
    }

    writer.close();
    return writer.toManifestFile();
  }
}
