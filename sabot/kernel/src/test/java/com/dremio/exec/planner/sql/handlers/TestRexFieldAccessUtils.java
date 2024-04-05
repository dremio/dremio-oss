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
package com.dremio.exec.planner.sql.handlers;

import static com.dremio.exec.planner.sql.handlers.RexFieldAccessUtils.STRUCTURED_WRAPPER;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.MaterializedSplitsPointer;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.TableMetadataImpl;
import com.dremio.exec.ops.SnapshotDiffContext;
import com.dremio.exec.planner.logical.ProjectRel;
import com.dremio.exec.planner.logical.Rel;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestRexFieldAccessUtils {
  @Mock private StoragePluginId pluginId;
  @Mock private RelOptTable table;
  @Mock private RelOptPlanner planner;

  private RelOptCluster cluster;
  private TableMetadata metadata;

  private final RelTraitSet traits = RelTraitSet.createEmpty().plus(Rel.LOGICAL);
  private final RelDataTypeFactory factory = JavaTypeFactoryImpl.INSTANCE;
  private final RexBuilder builder = new RexBuilder(factory);
  private final BatchSchema schema =
      BatchSchema.newBuilder()
          .addField(Field.nullable("col1", new ArrowType.Int(32, true)))
          .addField(Field.nullable("col2", new ArrowType.Int(32, true)))
          .addField(Field.nullable("col3", new ArrowType.Int(32, true)))
          .build();

  private final ImmutableList<SchemaPath> projectedColumns =
      ImmutableList.of(
          SchemaPath.getSimplePath("col1"),
          SchemaPath.getSimplePath("col2"),
          SchemaPath.getSimplePath("col3"));

  private final DatasetConfig datasetConfig =
      new DatasetConfig()
          .setId(new EntityId(UUID.randomUUID().toString()))
          .setFullPathList(Arrays.asList("test", "foo"))
          .setName("foo")
          .setOwner("testuser")
          .setPhysicalDataset(
              new PhysicalDataset().setFormatSettings(new FileConfig().setType(FileType.PARQUET)))
          .setRecordSchema(schema.toByteString());

  @Before
  public void setUp() {
    final OptionResolver optionResolver = OptionResolverSpecBuilder.build(new OptionResolverSpec());

    final ClusterResourceInformation info = mock(ClusterResourceInformation.class);

    final PlannerSettings plannerSettings =
        new PlannerSettings(DremioTest.DEFAULT_SABOT_CONFIG, optionResolver, () -> info);
    planner = new VolcanoPlanner(plannerSettings);
    cluster = RelOptCluster.create(planner, builder);

    metadata =
        new TableMetadataImpl(
            pluginId,
            datasetConfig,
            "testuser",
            MaterializedSplitsPointer.of(0, Collections.emptyList(), 0),
            null);
  }

  @Test
  public void testWrapUnwrapNoNestedFields() {
    final FilesystemScanDrel leftScan =
        new FilesystemScanDrel(
            cluster,
            traits,
            table,
            pluginId,
            metadata,
            projectedColumns,
            0,
            ImmutableList.of(),
            false,
            SnapshotDiffContext.NO_SNAPSHOT_DIFF);
    final ProjectRel originalProject =
        ProjectRel.create(
            cluster,
            traits,
            leftScan,
            ImmutableList.of(
                builder.makeInputRef(leftScan, 0),
                builder.makeInputRef(leftScan, 1),
                builder.makeInputRef(leftScan, 2)),
            leftScan.getRowType());
    final RelNode wrapped = RexFieldAccessUtils.wrap(originalProject);
    final RelNode unwrappend = RexFieldAccessUtils.unwrap(wrapped);
    assertEquals(
        "Wrap then Unwrap resulted in an unexpected change",
        RelOptUtil.toString(originalProject),
        RelOptUtil.toString(unwrappend));
  }

  @Test
  public void testUnwrapWithNestedFields() {
    final FilesystemScanDrel leftScan =
        new FilesystemScanDrel(
            cluster,
            traits,
            table,
            pluginId,
            metadata,
            projectedColumns,
            0,
            ImmutableList.of(),
            false,
            SnapshotDiffContext.NO_SNAPSHOT_DIFF);

    final ProjectRel originalProject =
        ProjectRel.create(
            cluster,
            traits,
            leftScan,
            ImmutableList.of(
                builder.makeInputRef(leftScan, 0),
                builder.makeInputRef(leftScan, 1),
                builder.makeInputRef(leftScan, 2)),
            leftScan.getRowType());

    final RexInputRef innerMost = builder.makeInputRef(leftScan, 2);

    final RexCall rexCallInner =
        (RexCall)
            builder.makeCall(
                leftScan.getRowType().getField("col3", false, false).getType(),
                STRUCTURED_WRAPPER,
                ImmutableList.of(innerMost));
    final RexCall rexCallMiddle =
        (RexCall)
            builder.makeCall(
                leftScan.getRowType().getField("col3", false, false).getType(),
                STRUCTURED_WRAPPER,
                ImmutableList.of(rexCallInner));
    final RexCall rexCallOuter =
        (RexCall)
            builder.makeCall(
                leftScan.getRowType().getField("col3", false, false).getType(),
                STRUCTURED_WRAPPER,
                ImmutableList.of(rexCallMiddle));
    final RexFieldAccessUtils.StructuredReferenceWrapper unwrapper =
        new RexFieldAccessUtils.StructuredReferenceWrapper(
            originalProject.getCluster().getRexBuilder(), false);

    final RexNode resultSingleWrapped = unwrapper.visitCall(rexCallInner);
    assertEquals("Failed to unwrap a single wrapped field", innerMost, resultSingleWrapped);

    final RexNode resultDoubleWrapped = unwrapper.visitCall(rexCallMiddle);
    assertEquals("Failed to unwrap a double wrapped field", innerMost, resultDoubleWrapped);

    final RexNode resultTrippleWrapped = unwrapper.visitCall(rexCallOuter);
    assertEquals("Failed to unwrap a tripple wrapped field", innerMost, resultTrippleWrapped);
  }
}
