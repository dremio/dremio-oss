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
package com.dremio;


import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.dremio.common.expression.SchemaPath;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes;
import com.dremio.exec.catalog.AbstractSplitsPointer;
import com.dremio.exec.catalog.MaterializedSplitsPointer;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.TableMetadataImpl;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.EmptyRel;
import com.dremio.exec.planner.logical.FilterRel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.SampleRel;
import com.dremio.exec.planner.logical.partition.PruneScanRuleBase;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.PruneableScan;
import com.dremio.options.OptionList;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.resource.ClusterResourceInformation;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.Affinity;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.NormalizedPartitionInfo;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.PartitionValue;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.test.DremioTest;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

/**
 * Test for index based pruning in {@code PruneScanRuleBase}.
 * This test uses custom splits pointer that verifies if expected search query is passed.
 */
@RunWith(Parameterized.class)
public class TestIndexBasedPruning extends DremioTest {

  private static final ImmutableList<SchemaPath> PROJECTED_COLUMNS = ImmutableList.of(SchemaPath.getSimplePath("date_col"), SchemaPath.getSimplePath("int_col"));
  private static final TestPartitionChunkMetadata TEST_PARTITION_CHUNK_METADATA_1 = new TestPartitionChunkMetadata(100);
  private static final TestPartitionChunkMetadata TEST_PARTITION_CHUNK_METADATA_2 = new TestPartitionChunkMetadata(200);

  private final BatchSchema SCHEMA = BatchSchema.newBuilder()
      .addField(Field.nullable("date_col", new ArrowType.Date(DateUnit.MILLISECOND)))
      .addField(Field.nullable("int_col", new ArrowType.Int(32, true)))
      .build();

  private final DatasetConfig DATASET_CONFIG = new DatasetConfig()
      .setId(new EntityId(UUID.randomUUID().toString()))
      .setFullPathList(Arrays.asList("test", "foo"))
      .setName("foo")
      .setOwner("testuser")
      .setReadDefinition(new ReadDefinition().setSplitVersion(0L).setPartitionColumnsList(ImmutableList.of("date_col","int_col")))
      .setRecordSchema(SCHEMA.toByteString());

  /**
   * Prunable SplitsPointer for testing
   */
  public class TestSplitsPointer extends AbstractSplitsPointer {
    private final long splitVersion;
    private final int totalSplitCount;
    private final List<PartitionChunkMetadata> materializedPartitionChunks;

    TestSplitsPointer(long splitVersion, Iterable<PartitionChunkMetadata> partitionChunks, int totalSplitCount) {
      this.splitVersion = splitVersion;
      this.materializedPartitionChunks = ImmutableList.copyOf(partitionChunks);
      this.totalSplitCount = totalSplitCount;
    }

    @Override
    public SplitsPointer prune(SearchTypes.SearchQuery partitionFilterQuery) {
      assertTrue("Given search query is not expected", partitionFilterQuery.equals(expected));
      indexPruned = true;
      //return an empty pointer after pruning
      return MaterializedSplitsPointer.of(0,Arrays.asList(),0);
    }

    @Override
    public long getSplitVersion() {
      return splitVersion;
    }

    @Override
    public Iterable<PartitionChunkMetadata> getPartitionChunks() {
      return materializedPartitionChunks;
    }

    @Override
    public int getTotalSplitsCount() {
      return totalSplitCount;
    }
  }

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
      return "key";
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
  private static final class TestScanRel extends ScanRelBase implements PruneableScan {
    private final boolean hasFilter;

    public TestScanRel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, StoragePluginId pluginId,
        TableMetadata dataset, List<SchemaPath> projectedColumns, double observedRowcountAdjustment, boolean hasFilter) {
      super(cluster, traitSet, table, pluginId, dataset, projectedColumns, observedRowcountAdjustment);
      this.hasFilter = hasFilter;
    }

    @Override
    public RelNode applyDatasetPointer(TableMetadata newDatasetPointer) {
      return new TestScanRel(getCluster(), traitSet, getTable(), pluginId, newDatasetPointer,
          getProjectedColumns(), observedRowcountAdjustment, hasFilter);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new TestScanRel(getCluster(), traitSet, getTable(), pluginId, tableMetadata,
          getProjectedColumns(), observedRowcountAdjustment, hasFilter);
    }

    @Override
    public ScanRelBase cloneWithProject(List<SchemaPath> projection) {
      return new TestScanRel(getCluster(), traitSet, getTable(), pluginId, tableMetadata,
          getProjectedColumns(), observedRowcountAdjustment, hasFilter);
    }
  }

  private static final RelTraitSet TRAITS = RelTraitSet.createEmpty().plus(Rel.LOGICAL);
  private static final RelDataTypeFactory TYPE_FACTORY = JavaTypeFactoryImpl.INSTANCE;
  private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

  @Mock
  private OptionManager optionManager;
  @Mock
  private StoragePluginId pluginId;
  @Mock
  private RelOptTable table;
  @Mock
  private RelOptPlanner planner;
  private TestScanRel indexPrunableScan;
  private FilterRel filterAboveScan;
  private PruneScanRuleBase scanRule;
  private FilterRel filterAboveSample;
  private SampleRel sampleRel;
  private PruneScanRuleBase sampleScanRule;
  private RexNode rexNode;
  private SearchTypes.SearchQuery expected;
  private boolean indexPruned;
  private PlannerSettings plannerSettings;

  @Parameterized.Parameters(name = "{index}: Doing index pruning on {0}. Following condition is expected to be passed: {1}")
  public static Iterable<Object[]> getTestCases() {
    RexInputRef dateCol = REX_BUILDER.makeInputRef(TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.DATE), true),0);
    RexNode dateLiteral = REX_BUILDER.makeDateLiteral(new DateString("2010-01-01"));
    RexInputRef intCol = REX_BUILDER.makeInputRef(TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), true),1);
    RexNode intLiteral = REX_BUILDER.makeLiteral(2, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), false);
    RexNode castDate = REX_BUILDER.makeCast(dateLiteral.getType(), intLiteral);

    long longVal = ((GregorianCalendar) ((RexLiteral) dateLiteral).getValue()).getTimeInMillis();
    SearchTypes.SearchQuery q1 = SearchQueryUtils.and(SearchQueryUtils.newRangeLong("$D$::LONG-date_col", longVal, longVal, true, true));
    RexNode cond1 = REX_BUILDER.makeCall(EQUALS, dateCol, dateLiteral);

    int intVal = ((BigDecimal) ((RexLiteral) intLiteral).getValue()).setScale(0, BigDecimal.ROUND_HALF_UP).intValue();
    SearchTypes.SearchQuery q2 = SearchQueryUtils.and(SearchQueryUtils.newRangeInt("$D$::INTEGER-int_col", intVal, intVal, true, true));
    RexNode cond2 = REX_BUILDER.makeCall(EQUALS, intCol, intLiteral);

    RexNode cond3 = REX_BUILDER.makeCall(EQUALS, dateCol, castDate);

    RexNode cond4 = REX_BUILDER.makeCall(GREATER_THAN, dateCol, castDate);

    // equivalent to where $0 = "2010-01-01" and $1 = 1 => both filters can be index pruned
    RexNode testCondition1 = REX_BUILDER.makeCall(AND, cond1, cond2);

    // equivalent to where $0 = CAST(1 as DATE) and $1 = 1 => only the second filter can be index pruned
    RexNode testCondition2 = REX_BUILDER.makeCall(AND, cond3, cond2);

    // equivalent to where $0 = CAST(1 as DATE) and $0 > CAST(1 as DATE) => none of them can be index pruned
    RexNode testCondition3 = REX_BUILDER.makeCall(AND, cond3, cond4);

    return ImmutableList.<Object[]>builder()
        .add(new Object[] { testCondition1, SearchQueryUtils.and(ImmutableList.of(q2, q1)) })
        .add(new Object[] { testCondition2, SearchQueryUtils.and(ImmutableList.of(q2)) })
        .add(new Object[] { testCondition3, null })
        .build();
  }

  public TestIndexBasedPruning(RexNode rexNode, SearchTypes.SearchQuery expected) {
    this.rexNode = rexNode;
    this.expected = expected;
  }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(optionManager.getOptionValidatorListing()).thenReturn(mock(OptionValidatorListing.class));
    when(optionManager.getOption(eq(PlannerSettings.FILTER_MAX_SELECTIVITY_ESTIMATE_FACTOR.getOptionName())))
            .thenReturn(PlannerSettings.FILTER_MAX_SELECTIVITY_ESTIMATE_FACTOR.getDefault());
    when(optionManager.getOption(eq(PlannerSettings.FILTER_MIN_SELECTIVITY_ESTIMATE_FACTOR.getOptionName())))
            .thenReturn(PlannerSettings.FILTER_MIN_SELECTIVITY_ESTIMATE_FACTOR.getDefault());
    when(optionManager.getOption(eq(PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getOptionName())))
      .thenReturn(PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault());
    OptionList optionList = new OptionList();
    optionList.add(PlannerSettings.FILTER_MAX_SELECTIVITY_ESTIMATE_FACTOR.getDefault());
    optionList.add(PlannerSettings.FILTER_MIN_SELECTIVITY_ESTIMATE_FACTOR.getDefault());
    when(optionManager.getNonDefaultOptions()).thenReturn(optionList);

    ClusterResourceInformation info = mock(ClusterResourceInformation.class);
    when(info.getExecutorNodeCount()).thenReturn(1);

    plannerSettings = new PlannerSettings(DremioTest.DEFAULT_SABOT_CONFIG, optionManager,
      () -> info);

    RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(plannerSettings), REX_BUILDER);
    SplitsPointer splitsPointer = new TestSplitsPointer(0, Arrays.asList(TEST_PARTITION_CHUNK_METADATA_1, TEST_PARTITION_CHUNK_METADATA_2), 2);

    TableMetadata indexPrunableMetadata = new TableMetadataImpl(pluginId, DATASET_CONFIG, "testuser", splitsPointer);
    SourceType newType = mock(SourceType.class);
    when(newType.value()).thenReturn("TestSource");
    when(pluginId.getType()).thenReturn(newType);

    indexPrunableScan = new TestScanRel(cluster, TRAITS, table, pluginId, indexPrunableMetadata, PROJECTED_COLUMNS, 0, false);
    filterAboveScan = new FilterRel(cluster, TRAITS, indexPrunableScan, rexNode);
    scanRule = new PruneScanRuleBase.PruneScanRuleFilterOnScan<>(pluginId.getType(), TestScanRel.class, mock(OptimizerRulesContext.class));
    sampleRel = new SampleRel(cluster, TRAITS, indexPrunableScan);
    filterAboveSample = new FilterRel(cluster, TRAITS, sampleRel, rexNode);
    sampleScanRule = new PruneScanRuleBase.PruneScanRuleFilterOnSampleScan<>(pluginId.getType(), TestScanRel.class, mock(OptimizerRulesContext.class));
  }


  @Test
  public void testIndexPruning() {
    indexPruned = false;
    RelOptRuleCall pruneCall = newCall(scanRule, filterAboveScan, indexPrunableScan);
    when(planner.getContext()).thenReturn(plannerSettings);
    scanRule.onMatch(pruneCall);
    if (expected == null) {
      assertTrue("Index pruned for a wrong condition", !indexPruned);
    }
  }

  @Test
  public void testIndexPruningSampleScan() {
    indexPruned = false;
    RelOptRuleCall pruneCall = newCall(sampleScanRule, filterAboveScan, sampleRel, indexPrunableScan);
    when(planner.getContext()).thenReturn(plannerSettings);
    sampleScanRule.onMatch(pruneCall);
    if (expected == null) {
      assertTrue("Index pruned for a wrong condition", !indexPruned);
    } else {
      assertTrue("Index not pruned", indexPruned);
    }
  }

  private RelOptRuleCall newCall(PruneScanRuleBase rule, RelNode... operands) {
    return new RelOptRuleCall(null, rule.getOperand(), operands, Collections.emptyMap()) {
      @Override
      public void transformTo(RelNode rel, Map<RelNode, RelNode> equiv) {
        if (rule instanceof PruneScanRuleBase.PruneScanRuleFilterOnSampleScan) {
          assertTrue("SampleRel is expected after pruning", rel instanceof SampleRel && ((SampleRel) rel).getInput() instanceof EmptyRel);
        } else {
          assertTrue("EmptyRel is expected after pruning", rel instanceof EmptyRel);
        }
      }
      @Override
      public RelOptPlanner getPlanner() {
        return planner;
      }
    };
  }
}
