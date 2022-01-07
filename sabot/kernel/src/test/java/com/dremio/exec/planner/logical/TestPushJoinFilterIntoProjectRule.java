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
package com.dremio.exec.planner.logical;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelHintsPropagator;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.MaterializedSplitsPointer;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.TableMetadataImpl;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.FilesystemScanDrel;
import com.dremio.options.OptionResolver;
import com.dremio.resource.ClusterResourceInformation;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.test.DremioTest;
import com.dremio.test.specs.OptionResolverSpec;
import com.dremio.test.specs.OptionResolverSpecBuilder;
import com.google.common.collect.ImmutableList;

/**
 * Unit tests for PushJoinFilterIntoProjectRule
 */
public class TestPushJoinFilterIntoProjectRule extends DremioTest {
  @Mock
  private StoragePluginId pluginId;
  @Mock
  private RelOptTable table;
  @Mock
  private RelOptPlanner planner;

  private RelOptCluster cluster;
  private TableMetadata metadata;

  private final RelTraitSet traits = RelTraitSet.createEmpty().plus(Rel.LOGICAL);
  private final RelDataTypeFactory factory = JavaTypeFactoryImpl.INSTANCE;
  private final RexBuilder builder = new RexBuilder(factory);
  private final RelOptRule relOptRule = PushJoinFilterIntoProjectRule.INSTANCE;

  private final BatchSchema schema = BatchSchema.newBuilder()
    .addField(Field.nullable("col1", new ArrowType.Int(32, true)))
    .addField(Field.nullable("col2", new ArrowType.Int(32, true)))
    .addField(Field.nullable("col3", new ArrowType.Int(32, true)))
    .build();

  private final ImmutableList<SchemaPath> projectedColumns = ImmutableList.of(
    SchemaPath.getSimplePath("col1"),
    SchemaPath.getSimplePath("col2"),
    SchemaPath.getSimplePath("col3"));

  private final DatasetConfig datasetConfig = new DatasetConfig()
    .setId(new EntityId(UUID.randomUUID().toString()))
    .setFullPathList(Arrays.asList("test", "foo"))
    .setName("foo")
    .setOwner("testuser")
    .setPhysicalDataset(
      new PhysicalDataset()
        .setFormatSettings(new FileConfig().setType(FileType.PARQUET)))
    .setRecordSchema(schema.toByteString());

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    OptionResolver optionResolver = OptionResolverSpecBuilder.build(new OptionResolverSpec());

    ClusterResourceInformation info = mock(ClusterResourceInformation.class);
    when(info.getExecutorNodeCount()).thenReturn(1);

    PlannerSettings plannerSettings = new PlannerSettings(DremioTest.DEFAULT_SABOT_CONFIG, optionResolver, () -> info);
    planner = new VolcanoPlanner(plannerSettings);
    cluster = RelOptCluster.create(planner, builder);

    metadata = new TableMetadataImpl(pluginId, datasetConfig, "testuser", MaterializedSplitsPointer.of(0, Collections.emptyList(), 0));
    SourceType newType = mock(SourceType.class);
    when(newType.value()).thenReturn("TestSource");
    when(pluginId.getType()).thenReturn(newType);
  }

  @Test
  public void testPushJoinFilterOnLeftSide() {
    FilesystemScanDrel leftScan = new FilesystemScanDrel(cluster, traits, table, pluginId, metadata, projectedColumns, 0, false);
    ProjectRel leftProj = ProjectRel.create(cluster, traits, leftScan,
      ImmutableList.of(builder.makeInputRef(leftScan, 0), builder.makeInputRef(leftScan, 1), builder.makeInputRef(leftScan, 2)), leftScan.getRowType());
    FilesystemScanDrel rightScan = new FilesystemScanDrel(cluster, traits, table, pluginId, metadata, projectedColumns, 0, false);
    ProjectRel rightProj = ProjectRel.create(cluster, traits, rightScan,
      ImmutableList.of(builder.makeInputRef(rightScan, 0), builder.makeInputRef(rightScan, 1), builder.makeInputRef(rightScan, 2)), rightScan.getRowType());

    RexNode joinConditionWithoutCast = builder.makeCall(AND,
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 0), builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 3)),
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 1), builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 4)),
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 2), builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 5)),
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 3), getInt(300)), // Constant filters on right sided fields, which will be pushed down on left
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 4), getInt(400))
    );

    RexNode joinConditionWithCast = builder.makeCall(AND,
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 0), builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 3)),
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 1), builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 4)),
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 2), builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 5)),
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 3), builder.makeCast(factory.createSqlType(SqlTypeName.INTEGER), builder.makeLiteral("300"))), // Constant filters on right sided fields, which will be pushed down on left
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 4), builder.makeCast(factory.createSqlType(SqlTypeName.INTEGER), builder.makeLiteral("400")))
    );

    // Join conditions before transformation
    assertEquals("AND(=($0, $3), =($1, $4), =($2, $5), =($3, 300), =($4, 400))", joinConditionWithoutCast.toString());
    assertEquals("AND(=($0, $3), =($1, $4), =($2, $5), =($3, CAST('300'):INTEGER NOT NULL), =($4, CAST('400'):INTEGER NOT NULL))", joinConditionWithCast.toString());

    // After transformation, the constants should be pushed below on left side, which should shift all the right fields
    String expected = "AND(=($0, $4), =($1, $5), =($2, $6), =($3, $7))";

    Join joinWithoutCast = JoinRel.create(cluster, traits, leftProj, rightProj, joinConditionWithoutCast, JoinRelType.INNER);
    Join joinWithCast = JoinRel.create(cluster, traits, leftProj, rightProj, joinConditionWithCast, JoinRelType.INNER);

    validate(joinWithoutCast, expected);
    validate(joinWithCast, expected);
  }

  @Test
  public void testPushJoinFilterOnRightSide() {
    FilesystemScanDrel leftScan = new FilesystemScanDrel(cluster, traits, table, pluginId, metadata, projectedColumns, 0, false);
    ProjectRel leftProj = ProjectRel.create(cluster, traits, leftScan,
      ImmutableList.of(builder.makeInputRef(leftScan, 0), builder.makeInputRef(leftScan, 1), builder.makeInputRef(leftScan, 2)), leftScan.getRowType());
    FilesystemScanDrel rightScan = new FilesystemScanDrel(cluster, traits, table, pluginId, metadata, projectedColumns, 0, false);
    ProjectRel rightProj = ProjectRel.create(cluster, traits, rightScan,
      ImmutableList.of(builder.makeInputRef(rightScan, 0), builder.makeInputRef(rightScan, 1), builder.makeInputRef(rightScan, 2)), rightScan.getRowType());

    RexNode joinConditionWithoutCast = builder.makeCall(AND,
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 0), builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 3)),
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 1), builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 4)),
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 2), builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 5)),
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 1), getInt(100)), // Constant filters on left sided fields, which will be pushed down on right
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 2), getInt(200))
    );

    RexNode joinConditionWithCast = builder.makeCall(AND,
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 0), builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 3)),
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 1), builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 4)),
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 2), builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 5)),
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 1), builder.makeCast(factory.createSqlType(SqlTypeName.INTEGER), builder.makeLiteral("100"))), // Constant filters on left sided fields, which will be pushed down on right
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 2), builder.makeCast(factory.createSqlType(SqlTypeName.INTEGER), builder.makeLiteral("200")))
    );

    // Join condition2 before transformation
    assertEquals("AND(=($0, $3), =($1, $4), =($2, $5), =($1, 100), =($2, 200))", joinConditionWithoutCast.toString());
    assertEquals("AND(=($0, $3), =($1, $4), =($2, $5), =($1, CAST('100'):INTEGER NOT NULL), =($2, CAST('200'):INTEGER NOT NULL))", joinConditionWithCast.toString());

    String expected = "AND(=($0, $4), =($1, $5), =($2, $6), =($3, $7))";

    Join joinWithoutCast = JoinRel.create(cluster, traits, leftProj, rightProj, joinConditionWithoutCast, JoinRelType.INNER);
    Join joinWithCast = JoinRel.create(cluster, traits, leftProj, rightProj, joinConditionWithCast, JoinRelType.INNER);

    validate(joinWithoutCast, expected);
    validate(joinWithCast, expected);
  }

  @Test
  public void testPushJoinFilterBothSides() {
    FilesystemScanDrel leftScan = new FilesystemScanDrel(cluster, traits, table, pluginId, metadata, projectedColumns, 0, false);
    ProjectRel leftProj = ProjectRel.create(cluster, traits, leftScan,
      ImmutableList.of(builder.makeInputRef(leftScan, 0), builder.makeInputRef(leftScan, 1), builder.makeInputRef(leftScan, 2)), leftScan.getRowType());
    FilesystemScanDrel rightScan = new FilesystemScanDrel(cluster, traits, table, pluginId, metadata, projectedColumns, 0, false);
    ProjectRel rightProj = ProjectRel.create(cluster, traits, rightScan,
      ImmutableList.of(builder.makeInputRef(rightScan, 0), builder.makeInputRef(rightScan, 1), builder.makeInputRef(rightScan, 2)), rightScan.getRowType());

    RexNode joinConditionWithoutCast = builder.makeCall(AND,
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 0), builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 3)),
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 1), builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 4)),
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 2), builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 5)),
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 1), getInt(100)), // Constant filters on left sided fields, which will be pushed down on right
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 2), getInt(200)),
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 3), getInt(300)), // Constant filters on right sided fields, which will be pushed down on left
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 4), getInt(400))
    );

    RexNode joinConditionWithCast = builder.makeCall(AND,
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 0), builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 3)),
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 1), builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 4)),
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 2), builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 5)),
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 1), builder.makeCast(factory.createSqlType(SqlTypeName.INTEGER), builder.makeLiteral("100"))), // Constant filters on left sided fields, which will be pushed down on right
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 2), builder.makeCast(factory.createSqlType(SqlTypeName.INTEGER), builder.makeLiteral("200"))),
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 3), builder.makeCast(factory.createSqlType(SqlTypeName.INTEGER), builder.makeLiteral("300"))), // Constant filters on right sided fields, which will be pushed down on left
      builder.makeCall(EQUALS,
        builder.makeInputRef(factory.createSqlType(SqlTypeName.INTEGER), 4), builder.makeCast(factory.createSqlType(SqlTypeName.INTEGER), builder.makeLiteral("400")))
    );

    // Join conditions before transformation
    assertEquals("AND(=($0, $3), =($1, $4), =($2, $5), =($1, 100), =($2, 200), =($3, 300), =($4, 400))", joinConditionWithoutCast.toString());
    assertEquals("AND(=($0, $3), =($1, $4), =($2, $5), =($1, CAST('100'):INTEGER NOT NULL), =($2, CAST('200'):INTEGER NOT NULL), =($3, CAST('300'):INTEGER NOT NULL), =($4, CAST('400'):INTEGER NOT NULL))", joinConditionWithCast.toString());

    // After transformation, the constants should be pushed below on both sides, which should shift all the right fields
    String expected = "AND(=($0, $5), =($1, $6), =($2, $7), =($3, $8), =($4, $9))";

    Join joinWithoutCast = JoinRel.create(cluster, traits, leftProj, rightProj, joinConditionWithoutCast, JoinRelType.INNER);
    Join joinWithCast = JoinRel.create(cluster, traits, leftProj, rightProj, joinConditionWithCast, JoinRelType.INNER);

    validate(joinWithoutCast, expected);
    validate(joinWithCast, expected);
  }

  private void validate(Join join, String expected) {
    TransformCollectingCall call = new TransformCollectingCall(null, relOptRule.getOperand(), new RelNode[]{join});
    relOptRule.onMatch(call);

    RelNode transformed = call.transformed;

    assertNotNull(String.format("PushJoinFilterIntoProjectRule did not transform the RelNode:\n%s", RelOptUtil.toString(join)), transformed);
    assertTrue(String.format("Top project missing from RelNode:\n%s", RelOptUtil.toString(transformed)), transformed instanceof ProjectRel);
    assertTrue(String.format("Row types don't match after transformation\nBefore: %s\nAfter: %s", join.getRowType(), transformed.getRowType()),
      MoreRelOptUtil.areRowTypesEqual(join.getRowType(), transformed.getRowType(), false, true));

    Join transformedJoin = (Join) transformed.getInput(0);

    // Join condition after transformation
    assertEquals(expected, transformedJoin.getCondition().toString());
  }

  public RexNode getInt(int i) {
    return builder.makeLiteral(i, factory.createSqlType(SqlTypeName.INTEGER), false);
  }

  private static class TransformCollectingCall extends RelOptRuleCall {
    private RelNode transformed;

    public TransformCollectingCall(RelOptPlanner planner, RelOptRuleOperand operand, RelNode[] rels) {
      super(planner, operand, rels, Collections.emptyMap());
    }

    @Override
    public void transformTo(RelNode rel, Map<RelNode, RelNode> equiv, RelHintsPropagator handler) {
      transformed = rel;
    }
  }
}
