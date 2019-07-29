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
package com.dremio.exec.planner.physical;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.MaterializedSplitsPointer;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.TableMetadataImpl;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.ClusterResourceInformation;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.PruneableScan;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.Affinity;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.NormalizedPartitionInfo;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.PartitionValue;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.test.DremioTest;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

/**
 * Test for {@code PushLimitToPruneableScan}
 */
@RunWith(Parameterized.class)
public class TestPushLimitToPruneableScan extends DremioTest {

  private static final ImmutableList<SchemaPath> PROJECTED_COLUMNS = ImmutableList.of(SchemaPath.getSimplePath("b"));
  private static final TestPartitionChunkMetadata TEST_PARTITION_CHUNK_METADATA_1 = new TestPartitionChunkMetadata(100);
  private static final TestPartitionChunkMetadata TEST_PARTITION_CHUNK_METADATA_2 = new TestPartitionChunkMetadata(200);
  private static final TestPartitionChunkMetadata TEST_PARTITION_CHUNK_METADATA_3 = new TestPartitionChunkMetadata(300);

  private final BatchSchema SCHEMA = BatchSchema.newBuilder()
      .addField(Field.nullable("a", new ArrowType.Utf8()))
      .addField(Field.nullable("b", new ArrowType.Utf8()))
      .build();

  private final DatasetConfig DATASET_CONFIG = new DatasetConfig()
      .setId(new EntityId(UUID.randomUUID().toString()))
      .setFullPathList(Arrays.asList("test", "foo"))
      .setName("foo")
      .setOwner("testuser")
      .setRecordSchema(SCHEMA.toByteString());

  /**
   * A mock partition chunk metadata implementation
   */
  private static final class TestPartitionChunkMetadata implements PartitionChunkMetadata {
    final int rowCount;
    private TestPartitionChunkMetadata(int rowCount) {
      this.rowCount = rowCount;
    }

    @Override
    public long getSize() {
      return 0;
    }

    @Override
    public long getRowCount() {
      return rowCount;
    }

    @Override
    public Iterable<PartitionValue> getPartitionValues() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getSplitKey() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getSplitCount() {
      return 1;
    }

    @Override
    public Iterable<DatasetSplit> getDatasetSplits() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ByteString getPartitionExtendedProperty() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Affinity> getAffinities() {
      throw new UnsupportedOperationException();
    }

    @Override
    public NormalizedPartitionInfo getNormalizedPartitionInfo() { throw new UnsupportedOperationException(); }
  }

  /**
   * A pruneable physical scan
   */
  private static final class TestScanPrel extends ScanPrelBase implements PruneableScan {
    private final boolean hasFilter;

    public TestScanPrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, StoragePluginId pluginId,
        TableMetadata dataset, List<SchemaPath> projectedColumns, double observedRowcountAdjustment, boolean hasFilter) {
      super(cluster, traitSet, table, pluginId, dataset, projectedColumns, observedRowcountAdjustment);
      this.hasFilter = hasFilter;
    }

    @Override
    public boolean hasFilter() {
      return hasFilter;
    }

    @Override
    public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
      return null;
    }

    @Override
    public RelNode applyDatasetPointer(TableMetadata newDatasetPointer) {
      return new TestScanPrel(getCluster(), traitSet, getTable(), pluginId, newDatasetPointer,
          projectedColumns, observedRowcountAdjustment, hasFilter);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new TestScanPrel(getCluster(), traitSet, getTable(), pluginId, tableMetadata,
          projectedColumns, observedRowcountAdjustment, hasFilter);
    }

    @Override
    public ScanRelBase cloneWithProject(List<SchemaPath> projection) {
      return new TestScanPrel(getCluster(), traitSet, getTable(), pluginId, tableMetadata,
          projectedColumns, observedRowcountAdjustment, hasFilter);
    }
  }

  /**
   * Helper interface to validate results for {@code testRule} method
   */
  interface Checker {

    public static final Checker NO_TRANSFORM = new Checker() {
      @Override
      public boolean shouldTransform() {
        return false;
      }

      @Override
      public void check(LimitPrel originalLimit, TestScanPrel originalScan, RelNode newNode) {
        fail("Rule unexpectedly invoked transformTo");
      }

      @Override
      public String toString() {
        return "No transformation";
      };
    };

    public static final Checker EMPTY_PREL = new Checker() {
      @Override
      public boolean shouldTransform() {
        return true;
      }

      @Override
      public void check(LimitPrel originalLimit, TestScanPrel originalScan, RelNode newNode) {
        assertThat(newNode, is(instanceOf(EmptyPrel.class)));
        assertThat(newNode.getRowType(), is(originalLimit.getRowType()));
        // Check that empty schema is the projected schema of the scan
        assertThat(((EmptyPrel) newNode).getBatchSchema(), is(originalScan.getProjectedSchema()));
      }

      @Override
      public String toString() {
        return "Transformation to empty";
      };
    };

    public static Checker transformed(int newOffset, int newFetch, PartitionChunkMetadata... newChunks) {
      return new Checker() {
        @Override
        public boolean shouldTransform() {
          return true;
        }

        @Override
        public void check(LimitPrel originalLimit, TestScanPrel originalScan, RelNode newNode) {
          assertThat(newNode, is(instanceOf(LimitPrel.class)));
          assertThat(RexLiteral.intValue(((LimitPrel) newNode).getOffset()), is(newOffset));
          assertThat(RexLiteral.intValue(((LimitPrel) newNode).getFetch()), is(newFetch));
          assertThat(newNode.getRowType(), is(originalLimit.getRowType()));

          RelNode input = newNode.getInput(0);
          assertThat(input, is(instanceOf(TestScanPrel.class)));
          assertThat(input.getRowType(), is(originalLimit.getRowType()));
          assertThat(((TestScanPrel)input).getTableMetadata().getSplitCount(), is(newChunks.length));
          final List<PartitionChunkMetadata> chunks = new ArrayList<>();
          ((TestScanPrel)input).getTableMetadata().getSplits().forEachRemaining(chunks::add);
          assertThat(chunks, Matchers.contains(newChunks));
        }

        @Override
        public String toString() {
          return String.format("Transformation to limit(offset: %d, fetch: %d) + scan with %d chunk(s)", newOffset, newFetch, newChunks.length);
        }
      };
    }

    boolean shouldTransform();
    void check(LimitPrel originalLimit, TestScanPrel originalScan, RelNode newNode);
  }

  private static final RelTraitSet TRAITS = RelTraitSet.createEmpty().plus(Prel.PHYSICAL);
  private static final RelDataTypeFactory TYPE_FACTORY = JavaTypeFactoryImpl.INSTANCE;
  private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

  @Mock
  private OptionManager optionManager;
  @Mock
  private StoragePluginId pluginId;
  private PlannerSettings plannerSettings;
  private RelOptCluster cluster;
  @Mock
  private RelOptTable table;
  private TableMetadata metadata;

  private final Checker checker;
  private final int offset;
  private final int fetch;

  /**
   * Get the test cases
   * @return
   */
  @Parameterized.Parameters(name = "{index}: {2} for offset {0} and fetch {1}")
  public static final Iterable<Object[]> getTestCases() {
    return ImmutableList.<Object[]>builder()
        // Simple checks to test partition pruning based on limit works
        .add(new Object[] { 0,   100, Checker.transformed(0,   100, TEST_PARTITION_CHUNK_METADATA_1) })
        .add(new Object[] { 0,    99, Checker.transformed(0,    99, TEST_PARTITION_CHUNK_METADATA_1) })
        .add(new Object[] { 0,   101, Checker.transformed(0,   101, TEST_PARTITION_CHUNK_METADATA_1, TEST_PARTITION_CHUNK_METADATA_2) })
        .add(new Object[] { 10,   50, Checker.transformed(10,   50, TEST_PARTITION_CHUNK_METADATA_1) })
        .add(new Object[] { 100,  50, Checker.transformed(0,    50, TEST_PARTITION_CHUNK_METADATA_2) })
        .add(new Object[] { 200, 300, Checker.transformed(100, 300, TEST_PARTITION_CHUNK_METADATA_2, TEST_PARTITION_CHUNK_METADATA_3) })
        .add(new Object[] { 350, 300, Checker.transformed(50,  300, TEST_PARTITION_CHUNK_METADATA_3) })
        // Verifying no new nodes are created
        .add(new Object[] { 0,   600, Checker.NO_TRANSFORM })
        .add(new Object[] { 0,   700, Checker.NO_TRANSFORM })
        .add(new Object[] { 90,  500, Checker.NO_TRANSFORM })
        .add(new Object[] { 0,   301, Checker.NO_TRANSFORM })
        // Verifying that scan + limit are replaced by empty
        .add(new Object[] { 0,     0, Checker.EMPTY_PREL })
        .add(new Object[] { 100,   0, Checker.EMPTY_PREL })
        .add(new Object[] { 600, 500, Checker.EMPTY_PREL })
        .build();
  }

  public TestPushLimitToPruneableScan(int offset, int fetch, Checker checker) {
    this.checker = checker;
    this.offset = offset;
    this.fetch = fetch;
  }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    when(optionManager.getOption(eq(ExecConstants.SLICE_TARGET)))
    .thenReturn(ExecConstants.SLICE_TARGET_OPTION.getDefault());
    when(optionManager.getOption(eq(PlannerSettings.ENABLE_LEAF_LIMITS.getOptionName())))
    .thenReturn(PlannerSettings.ENABLE_LEAF_LIMITS.getDefault());
    when(optionManager.getOption(eq(PlannerSettings.ENABLE_TRIVIAL_SINGULAR.getOptionName())))
    .thenReturn(PlannerSettings.ENABLE_TRIVIAL_SINGULAR.getDefault());
    ClusterResourceInformation info = mock(ClusterResourceInformation.class);
    when(info.getExecutorNodeCount()).thenReturn(1);

    plannerSettings = new PlannerSettings(DremioTest.DEFAULT_SABOT_CONFIG, optionManager, info);
    cluster = RelOptCluster.create(new VolcanoPlanner(plannerSettings), REX_BUILDER);
    metadata = new TableMetadataImpl(pluginId, DATASET_CONFIG, "testuser", MaterializedSplitsPointer.of(0, Arrays.asList(
        TEST_PARTITION_CHUNK_METADATA_1, TEST_PARTITION_CHUNK_METADATA_2, TEST_PARTITION_CHUNK_METADATA_3
         ), 3));
  }

  @Test
  public void testRule() throws Exception {
    final TestScanPrel scan = new TestScanPrel(cluster, TRAITS, table, pluginId, metadata, PROJECTED_COLUMNS, 0, false);
    final LimitPrel limitNode = new LimitPrel(cluster, TRAITS, scan,
        REX_BUILDER.makeExactLiteral(BigDecimal.valueOf(offset)),
        REX_BUILDER.makeExactLiteral(BigDecimal.valueOf(fetch)));

    boolean[] transformed = { false };
    final RelOptRuleCall call = newCall(rel -> {
      transformed[0] = true;
      checker.check(limitNode, scan, rel);
    }, limitNode, scan);

    assertTrue(PushLimitToPruneableScan.INSTANCE.matches(call));
    PushLimitToPruneableScan.INSTANCE.onMatch(call);
    assertThat(checker.shouldTransform(), is(transformed[0]));
  }

  @Test
  public void testRuleNoMatch() throws Exception {
    final TestScanPrel scan = new TestScanPrel(cluster, TRAITS, table, pluginId, metadata, PROJECTED_COLUMNS, 0, true);
    final LimitPrel limitNode = new LimitPrel(cluster, TRAITS, scan,
        REX_BUILDER.makeExactLiteral(BigDecimal.valueOf(offset)),
        REX_BUILDER.makeExactLiteral(BigDecimal.valueOf(fetch)));

    final RelOptRuleCall call = newCall(rel -> fail("Unexpected call to transformTo"), limitNode, scan);
    assertFalse(PushLimitToPruneableScan.INSTANCE.matches(call));
  }

  @Test
  public void testRuleIsIdempotent() throws Exception {
    final TestScanPrel scan = new TestScanPrel(cluster, TRAITS, table, pluginId, metadata, PROJECTED_COLUMNS, 0, false);
    final LimitPrel limitNode = new LimitPrel(cluster, TRAITS, scan,
        REX_BUILDER.makeExactLiteral(BigDecimal.valueOf(offset)),
        REX_BUILDER.makeExactLiteral(BigDecimal.valueOf(fetch)));

    // pointer to save result
    final RelNode[] result = new RelNode[] { null };
    final RelOptRuleCall call = newCall(rel -> {
      if (result[0] != null) {
        fail("transformTo called multiple times");
      }
      result[0] = rel;
    }, limitNode, scan);


    assertTrue(PushLimitToPruneableScan.INSTANCE.matches(call));
    PushLimitToPruneableScan.INSTANCE.onMatch(call);

    // Check that result[0] can be matched agaist the rule again
    if (!(result[0] instanceof LimitPrel)) {
      return;
    }
    final RelNode newLimit = result[0];
    if (!(newLimit.getInput(0) instanceof ScanPrelBase)) {
      return;
    }

    final RelNode newScan = newLimit.getInput(0);
    // Check that a second call does not create a new tree
    final RelOptRuleCall newCall = newCall(rel -> fail("Rule should not transform again tree"), newLimit, newScan);
    if (PushLimitToPruneableScan.INSTANCE.matches(newCall)) {
      PushLimitToPruneableScan.INSTANCE.onMatch(newCall);
    }
  }

  private RelOptRuleCall newCall(Consumer<RelNode> assertion, RelNode... operands) {
    return new RelOptRuleCall(null, PushLimitToPruneableScan.INSTANCE.getOperand(), operands, Collections.emptyMap()) {
      @Override
      public void transformTo(RelNode rel, Map<RelNode, RelNode> equiv) {
        assertion.accept(rel);
      }
    };
  }

}
