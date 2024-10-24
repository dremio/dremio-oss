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
package com.dremio.exec.physical.impl.writer;

import static com.dremio.exec.planner.common.TestPlanHelper.findNodes;
import static com.dremio.exec.planner.common.TestPlanHelper.findSingleNode;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createBasicTable;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.BaseTestQuery;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.ExecTest;
import com.dremio.exec.PassthroughQueryObserver;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.TestDml;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.SingleMergeExchangePrel;
import com.dremio.exec.planner.physical.SortPrel;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.UnionAllPrel;
import com.dremio.exec.planner.physical.WriterPrel;
import com.dremio.exec.planner.sql.DmlQueryTestUtils;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.query.OptimizeHandler;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.iceberg.IcebergManifestWriterPrel;
import com.dremio.exec.store.iceberg.IcebergTestTables;
import com.dremio.options.OptionValue;
import com.dremio.sabot.rpc.user.UserSession;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlNode;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestWriterPlan extends BaseTestQuery {
  private static SqlConverter converter;
  private static SqlHandlerConfig config;

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

    config = new SqlHandlerConfig(queryContext, converter, observer, null);

    config
        .getContext()
        .getOptions()
        .setOption(
            OptionValue.createBoolean(
                OptionValue.OptionType.SYSTEM,
                ExecConstants.ENABLE_ICEBERG_COMBINE_SMALL_FILES_FOR_DML.getOptionName(),
                true));

    config
        .getContext()
        .getOptions()
        .setOption(
            OptionValue.createBoolean(
                OptionValue.OptionType.SYSTEM,
                ExecConstants.ENABLE_ICEBERG_COMBINE_SMALL_FILES_FOR_OPTIMIZE.getOptionName(),
                true));

    config
        .getContext()
        .getOptions()
        .setOption(
            OptionValue.createBoolean(
                OptionValue.OptionType.SYSTEM,
                ExecConstants.ENABLE_ICEBERG_COMBINE_SMALL_FILES_FOR_PARTITIONED_TABLE_WRITES
                    .getOptionName(),
                true));

    config
        .getContext()
        .getOptions()
        .setOption(
            OptionValue.createBoolean(
                OptionValue.OptionType.SYSTEM, ExecConstants.ADAPTIVE_HASH.getOptionName(), true));
  }

  @Before
  public void setupTest() {
    config
        .getContext()
        .getOptions()
        .setOption(
            OptionValue.createBoolean(
                OptionValue.OptionType.SYSTEM,
                ExecConstants.ENABLE_ICEBERG_COMBINE_SMALL_FILES_FOR_DML.getOptionName(),
                true));

    config
        .getContext()
        .getOptions()
        .setOption(
            OptionValue.createBoolean(
                OptionValue.OptionType.SYSTEM,
                ExecConstants.ENABLE_ICEBERG_COMBINE_SMALL_FILES_FOR_OPTIMIZE.getOptionName(),
                true));

    config
        .getContext()
        .getOptions()
        .setOption(
            OptionValue.createBoolean(
                OptionValue.OptionType.SYSTEM,
                ExecConstants.ENABLE_ICEBERG_COMBINE_SMALL_FILES_FOR_PARTITIONED_TABLE_WRITES
                    .getOptionName(),
                true));

    config
        .getContext()
        .getOptions()
        .setOption(
            OptionValue.createBoolean(
                OptionValue.OptionType.SYSTEM, ExecConstants.ADAPTIVE_HASH.getOptionName(), true));
  }

  private void resetSmallFileCombinationFlags() {
    config
        .getContext()
        .getOptions()
        .setOption(
            OptionValue.createBoolean(
                OptionValue.OptionType.SYSTEM,
                ExecConstants.ENABLE_ICEBERG_COMBINE_SMALL_FILES_FOR_DML.getOptionName(),
                ExecConstants.ENABLE_ICEBERG_COMBINE_SMALL_FILES_FOR_DML
                    .getDefault()
                    .getBoolVal()));
    config
        .getContext()
        .getOptions()
        .setOption(
            OptionValue.createBoolean(
                OptionValue.OptionType.SYSTEM,
                ExecConstants.ENABLE_ICEBERG_COMBINE_SMALL_FILES_FOR_OPTIMIZE.getOptionName(),
                ExecConstants.ENABLE_ICEBERG_COMBINE_SMALL_FILES_FOR_OPTIMIZE
                    .getDefault()
                    .getBoolVal()));
    config
        .getContext()
        .getOptions()
        .setOption(
            OptionValue.createBoolean(
                OptionValue.OptionType.SYSTEM,
                ExecConstants.ENABLE_ICEBERG_COMBINE_SMALL_FILES_FOR_PARTITIONED_TABLE_WRITES
                    .getOptionName(),
                ExecConstants.ENABLE_ICEBERG_COMBINE_SMALL_FILES_FOR_PARTITIONED_TABLE_WRITES
                    .getDefault()
                    .getBoolVal()));
  }

  @After
  public void tearDownTest() throws Exception {
    resetSmallFileCombinationFlags();

    config
        .getContext()
        .getOptions()
        .setOption(
            OptionValue.createBoolean(
                OptionValue.OptionType.SYSTEM,
                ExecConstants.ADAPTIVE_HASH.getOptionName(),
                ExecConstants.ADAPTIVE_HASH.getDefault().getBoolVal()));
  }

  @Test
  public void testCombiningSmallFilesWriterPlan() throws Exception {
    try (IcebergTestTables.Table table = IcebergTestTables.V2_ORDERS.get()) {
      final String delete = "Delete from " + table.getTableName();

      Prel plan = TestDml.getDmlPlan(config, converter.parse(delete));

      List<UnionAllPrel> unions = findNodes(plan, UnionAllPrel.class, null);
      assertThat(unions.size()).isEqualTo(2);
      UnionAllPrel smallFileCombiningUnion = unions.get(1);
      // the small file combining sub-plan dos not start with RR Exchange for trivial plan
      assertThat(smallFileCombiningUnion.getInput(0)).isInstanceOf(WriterPrel.class);

      SingleMergeExchangePrel singleMergeExchangePrel =
          findSingleNode(plan, SingleMergeExchangePrel.class, null);

      // verify that SortPrel is the input of the file count SingleMergeExchangePrel
      assertThat(singleMergeExchangePrel.getInput(0)).isInstanceOf(SortPrel.class);

      // verify small file conditions
      //       1. they are added data files
      //       2. there are two or more small file per partition value
      //       3. the files are not small files
      int operationTypeColumnIndex =
          RecordWriter.SCHEMA.getFieldId(
                  SchemaPath.getSimplePath(RecordWriter.OPERATION_TYPE_COLUMN))
              .getFieldIds()[0];

      // note: "operationTypeColumnIndex +4... +5" are referencing fields spanning beyond
      // RecordWriter's Schema.
      String expectedConditions =
          String.format(
              "[=($%s, 0), <>($%s, 'p0$DREMIO_notSmallFilePartitionTableValue':VARCHAR(41)), >($%s, 1)]",
              operationTypeColumnIndex, operationTypeColumnIndex + 4, operationTypeColumnIndex + 5);
      Map<String, String> attributes = ImmutableMap.of("conjunctions", expectedConditions);
      findSingleNode(plan, FilterPrel.class, attributes);
    }
  }

  @Test
  public void testCombiningSmallFilesIsOffByDefaultForDMLPlan() throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(TEMP_SCHEMA_HADOOP, 2, 2)) {
      resetSmallFileCombinationFlags();
      final String delete = "Delete from " + table.fqn;

      Prel plan = TestDml.getDmlPlan(config, converter.parse(delete));

      // only one writer is expected since small-file-combination is disabled for DML plan
      List<WriterPrel> writers =
          findNodes(plan, WriterPrel.class, null).stream()
              .filter(w -> !(w instanceof IcebergManifestWriterPrel))
              .collect(Collectors.toList());
      assertThat(writers.size()).isEqualTo(1);
    }
  }

  @Test
  public void testCombiningSmallFilesIsOnByDefaultForOptimizePlan() throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(TEMP_SCHEMA_HADOOP, 2, 2)) {
      resetSmallFileCombinationFlags();
      final String optimizeQuery = String.format("OPTIMIZE TABLE %s", table.fqn);

      OptimizeHandler optimizeHandler = new OptimizeHandler();
      SqlNode sqlNode = converter.parse(optimizeQuery);
      Prel plan =
          optimizeHandler.getNonPhysicalPlan(
              config.getConverter().getPlannerCatalog(),
              config,
              sqlNode,
              optimizeHandler.getTargetTablePath(sqlNode));
      // two writers are expected since small-file-combination is turned on for Optimize plan
      List<WriterPrel> writers = findNodes(plan, WriterPrel.class, null);
      assertThat(writers.size()).isEqualTo(2);
    }
  }

  private boolean findIcebergPartitionTransformPrel(Prel plan) {
    List<TableFunctionPrel> tfs = findNodes(plan, TableFunctionPrel.class, null);

    for (TableFunctionPrel tf : tfs) {
      if (tf.getTableFunctionConfig()
          .getType()
          .equals(TableFunctionConfig.FunctionType.ICEBERG_PARTITION_TRANSFORM)) {
        return true;
      }
    }

    return false;
  }

  private void testPartitionTransformationPlan(boolean hasNonIdentityPartitionColumns)
      throws Exception {
    try (DmlQueryTestUtils.Table t = createBasicTable(TEMP_SCHEMA_HADOOP, 2, 2)) {

      if (hasNonIdentityPartitionColumns) {
        // add a non-identity partition column
        String addPartitionQuery =
            String.format("ALTER TABLE  %s add PARTITION FIELD BUCKET(10, id)", t.fqn);
        BaseTestQuery.test(addPartitionQuery);
      }

      final String delete = "Delete from " + t.fqn;

      Prel plan = TestDml.getDmlPlan(config, converter.parse(delete));

      assertThat(findIcebergPartitionTransformPrel(plan)).isEqualTo(hasNonIdentityPartitionColumns);
    }
  }

  @Test
  public void testIdentityPartitionColumnNoPartitionTransformationPlan() throws Exception {
    testPartitionTransformationPlan(false);
  }

  @Test
  public void testPartitionTransformationPlan() throws Exception {
    testPartitionTransformationPlan(true);
  }
}
