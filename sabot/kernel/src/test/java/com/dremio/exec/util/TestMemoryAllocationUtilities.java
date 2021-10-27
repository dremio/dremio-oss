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
package com.dremio.exec.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.ExecTest;
import com.dremio.exec.physical.base.AbstractSingle;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.physical.config.EmptyValues;
import com.dremio.exec.physical.config.ExternalSort;
import com.dremio.exec.physical.config.HashJoinPOP;
import com.dremio.exec.physical.config.HashToRandomExchange;
import com.dremio.exec.physical.config.Project;
import com.dremio.exec.physical.config.SingleSender;
import com.dremio.exec.physical.config.UnorderedReceiver;
import com.dremio.exec.planner.fragment.Fragment;
import com.dremio.exec.planner.fragment.Wrapper;
import com.dremio.exec.proto.CoordExecRPC.MinorFragmentIndexEndpoint;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.options.DefaultOptionManager;
import com.dremio.exec.server.options.OptionManagerWrapper;
import com.dremio.exec.server.options.OptionValidatorListingImpl;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.TypeValidators;
import com.dremio.test.DremioTest;
import com.google.common.collect.ImmutableMap;

public class TestMemoryAllocationUtilities extends ExecTest {

  private static final EmptyValues ARBTRIARY_LEAF;
  private static final TypeValidators.DoubleValidator SORT_FACTOR = new TypeValidators.RangeDoubleValidator("planner.op.sort.factor", 0.0, 1000.0, 1.0d);
  private static final TypeValidators.BooleanValidator SORT_BOUNDED = new TypeValidators.BooleanValidator("planner.op.sort.bounded", true);
  static {
    ARBTRIARY_LEAF = new EmptyValues(OpProps.prototype(1, Long.MAX_VALUE), BatchSchema.SCHEMA_UNKNOWN_NO_DATA);
  }

  private static final NodeEndpoint N1 = NodeEndpoint.newBuilder().setAddress("n1").build();
  private static final NodeEndpoint N2 = NodeEndpoint.newBuilder().setAddress("n2").build();

  private OptionManager options;
  private SystemOptionManager systemOptionManager;
  private LegacyKVStoreProvider kvstoreprovider;

   @Before
   public void setup() throws Exception {
     kvstoreprovider =
         LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT);
     kvstoreprovider.start();
     final OptionValidatorListing optionValidatorListing = new OptionValidatorListingImpl(CLASSPATH_SCAN_RESULT);
     systemOptionManager = new SystemOptionManager(
       optionValidatorListing, new LogicalPlanPersistence(DEFAULT_SABOT_CONFIG, CLASSPATH_SCAN_RESULT), () -> kvstoreprovider, false);
     options = OptionManagerWrapper.Builder.newBuilder()
       .withOptionManager(new DefaultOptionManager(optionValidatorListing))
       .withOptionManager(systemOptionManager)
       .build();
     systemOptionManager.start();
  }

   @After
   public void teardown() throws Exception {
     AutoCloseables.close(systemOptionManager, kvstoreprovider);
   }

  /**
   * Check that we handle bounded and unbounded correctly. Also validate handling of weights correctly.
   */
  @Test
  public void syntheticSimple() {
    ConfigurableOperator cnb = new ConfigurableOperator(OpProps.prototype(1, Long.MAX_VALUE).cloneWithMemoryExpensive(true).cloneWithBound(false).cloneWithMemoryFactor(2.0d), ARBTRIARY_LEAF);
    ConfigurableOperator cb = new ConfigurableOperator(OpProps.prototype(1, Long.MAX_VALUE).cloneWithMemoryExpensive(true).cloneWithBound(true).cloneWithMemoryFactor(1.0d), cnb);
    Fragment f1 = new Fragment();
    f1.addOperator(cb);
    Wrapper w1 = new Wrapper(f1, 0);
    w1.overrideEndpoints(Collections.singletonList(N1));
    MemoryAllocationUtilities.setMemory(options, ImmutableMap.of(f1, w1), 10);
    assertEquals(Long.MAX_VALUE, cnb.getProps().getMemLimit());
    assertEquals(3, cb.getProps().getMemLimit());
  }

  @Test
  public void doubleSort() {
    ExternalSort es1 = new ExternalSort(OpProps.prototype().cloneWithNewReserve(0).cloneWithMemoryExpensive(true).cloneWithMemoryFactor(options.getOption(SORT_FACTOR)).cloneWithBound(options.getOption(SORT_BOUNDED)), ARBTRIARY_LEAF, Collections.emptyList(), false);
    ExternalSort es2 = new ExternalSort(OpProps.prototype().cloneWithNewReserve(0).cloneWithMemoryExpensive(true).cloneWithMemoryFactor(options.getOption(SORT_FACTOR)).cloneWithBound(options.getOption(SORT_BOUNDED)), es1, Collections.emptyList(), false);
    Fragment f1 = new Fragment();
    f1.addOperator(es2);
    Wrapper wrapper = new Wrapper(f1, 0);
    wrapper.overrideEndpoints(Collections.singletonList(N1));
    MemoryAllocationUtilities.setMemory(options, ImmutableMap.of(f1, wrapper), 10);
    assertEquals(4l, es1.getProps().getMemLimit());
    assertEquals(4l, es2.getProps().getMemLimit());
  }

  @Test
  public void doubleSortWithExchange() {
    ExternalSort es1 = new ExternalSort(OpProps.prototype(0, Long.MAX_VALUE).cloneWithMemoryExpensive(true).cloneWithMemoryFactor(options.getOption(SORT_FACTOR)).cloneWithBound(options.getOption(SORT_BOUNDED)), ARBTRIARY_LEAF, Collections.emptyList(), false);
    SingleSender ss = new SingleSender(OpProps.prototype(1, Long.MAX_VALUE).cloneWithMemoryFactor(options.getOption(SORT_FACTOR)).cloneWithBound(options.getOption(SORT_BOUNDED)), Mockito.mock(BatchSchema.class), es1, 0,
      MinorFragmentIndexEndpoint.newBuilder().setMinorFragmentId(0).build());
    Fragment f1 = new Fragment();
    f1.addOperator(ss);
    Wrapper w1 = new Wrapper(f1, 0);
    w1.overrideEndpoints(Collections.singletonList(N1));

    UnorderedReceiver or = new UnorderedReceiver(OpProps.prototype(1, Long.MAX_VALUE), Mockito.mock(BatchSchema.class), 0, Collections.emptyList(), false);
    ExternalSort es2 = new ExternalSort(OpProps.prototype(0, Long.MAX_VALUE).cloneWithMemoryExpensive(true).cloneWithMemoryFactor(options.getOption(SORT_FACTOR)).cloneWithBound(options.getOption(SORT_BOUNDED)), or, Collections.emptyList(), false);
    Fragment f2 = new Fragment();
    f2.addOperator(es2);
    Wrapper w2 = new Wrapper(f2, 0);
    w2.overrideEndpoints(Collections.singletonList(N1));


    MemoryAllocationUtilities.setMemory(options, ImmutableMap.of(f1, w1, f2, w2), 10);
    assertEquals(3l, es1.getProps().getMemLimit());
    assertEquals(3l, es2.getProps().getMemLimit());
  }

  /**
   * Even though N2 only has one sort, we set the overall limit to the worse case node (same as legacy algorithm).
   */
  @Test
  public void doubleSortWithExchangeUnbalancedNodes() {
    ExternalSort es1 = new ExternalSort(OpProps.prototype(0,  Long.MAX_VALUE).cloneWithMemoryExpensive(true).cloneWithMemoryFactor(options.getOption(SORT_FACTOR)).cloneWithBound(options.getOption(SORT_BOUNDED)), ARBTRIARY_LEAF, Collections.emptyList(), false);
    SingleSender ss = new SingleSender(OpProps.prototype(1,  Long.MAX_VALUE).cloneWithMemoryFactor(options.getOption(SORT_FACTOR)).cloneWithBound(options.getOption(SORT_BOUNDED)), Mockito.mock(BatchSchema.class), es1, 0,
      MinorFragmentIndexEndpoint.newBuilder().setMinorFragmentId(0).build());
    Fragment f1 = new Fragment();
    f1.addOperator(ss);
    Wrapper w1 = new Wrapper(f1, 0);
    w1.overrideEndpoints(Arrays.asList(N1, N2));

    UnorderedReceiver or = new UnorderedReceiver(OpProps.prototype(1,  Long.MAX_VALUE), Mockito.mock(BatchSchema.class), 0, Collections.emptyList(), false);
    ExternalSort es2 = new ExternalSort(OpProps.prototype(0,  Long.MAX_VALUE).cloneWithMemoryExpensive(true).cloneWithMemoryFactor(options.getOption(SORT_FACTOR)).cloneWithBound(options.getOption(SORT_BOUNDED)), or, Collections.emptyList(), false);
    Fragment f2 = new Fragment();
    f2.addOperator(es2);
    Wrapper w2 = new Wrapper(f2, 0);
    w2.overrideEndpoints(Collections.singletonList(N1));


    MemoryAllocationUtilities.setMemory(options, ImmutableMap.of(f1, w1, f2, w2), 10);
    assertEquals(3L, es1.getProps().getMemLimit());
    assertEquals(3L, es2.getProps().getMemLimit());
  }

  private OpProps prop(int majorFragmentId, int localOperatorId) {
    return OpProps.prototype(OpProps.buildOperatorId(majorFragmentId, localOperatorId));
  }

  @Test
  public void testConsideredOperators() {
      /*
                      HashJoin1-0
                    /             \
                   /               \
           Project1-1             Project1-3
              |                       |
          HashToRandomEx1-2     HashToRandomEx1-4
              |                       |
           Project2-0              Project3-0
              |                       |
           Project2-1               Empty3-1
              |
            Empty2-2

       */

    // Fragment 2
    EmptyValues secondFragmentLeaf  = new EmptyValues(prop(2, 2), BatchSchema.SCHEMA_UNKNOWN_NO_DATA);
    Project secondFragmentProject1 = new Project(prop(2, 1), secondFragmentLeaf, null);
    Project secondFragmentProject0 = new Project(prop(2, 0), secondFragmentProject1, null);

    // Fragment 3
    EmptyValues thirdFragmentLeaf1  = new EmptyValues(prop(3, 1), BatchSchema.SCHEMA_UNKNOWN_NO_DATA);
    Project thirdFragmentProject0 = new Project(prop(3, 0), thirdFragmentLeaf1, null);

    // Fragment 1
    HashToRandomExchange hashToRandomExchange2 = new HashToRandomExchange(prop(1, 2),
      secondFragmentProject0.getProps(), null, null, BatchSchema.SCHEMA_UNKNOWN_NO_DATA, secondFragmentProject1, null, options);
    Project firstFragmentProject1 = new Project(prop(1, 1), hashToRandomExchange2, null);
    HashToRandomExchange hashToRandomExchange4 = new HashToRandomExchange(prop(1, 4),
      thirdFragmentProject0.getProps(), null, null, BatchSchema.SCHEMA_UNKNOWN_NO_DATA, secondFragmentProject1, null, options);
    Project firstFragmentProject3 = new Project(prop(1, 3), hashToRandomExchange4, null);
    HashJoinPOP hashJoinPOP = new HashJoinPOP(prop(1, 0).cloneWithMemoryExpensive(true), firstFragmentProject1, firstFragmentProject3,
      null, null, null, false, null);

    MemoryAllocationUtilities.FindConsideredOperators fco = new MemoryAllocationUtilities.FindConsideredOperators(1);
    hashJoinPOP.accept(fco, null);
    List<PhysicalOperator> consideredOperators = fco.getConsideredOperators();
    List<PhysicalOperator> nonConsideredOperators = fco.getNonConsideredOperators();

    assertEquals(1, consideredOperators.size()); // HashJoin
    assertTrue(consideredOperators.stream().allMatch(op -> op.getProps().getMajorFragmentId() == 1));
    assertEquals(1, consideredOperators.stream().map(op -> op.getProps().getLocalOperatorId()).distinct().count());

    assertEquals(4, nonConsideredOperators.size());
    assertTrue(nonConsideredOperators.stream().allMatch(op -> op.getProps().getMajorFragmentId() == 1));
    // no operator is visited more than once
    assertEquals(4, nonConsideredOperators.stream().map(op -> op.getProps().getLocalOperatorId()).distinct().count());


    fco = new MemoryAllocationUtilities.FindConsideredOperators(2);
    secondFragmentProject0.accept(fco, null);
    consideredOperators = fco.getConsideredOperators();
    nonConsideredOperators = fco.getNonConsideredOperators();

    assertEquals(0, consideredOperators.size());

    assertEquals(3, nonConsideredOperators.size());
    assertTrue(nonConsideredOperators.stream().allMatch(op -> op.getProps().getMajorFragmentId() == 2));
    assertEquals(3, nonConsideredOperators.stream().map(op -> op.getProps().getLocalOperatorId()).distinct().count());

    fco = new MemoryAllocationUtilities.FindConsideredOperators(3);
    thirdFragmentProject0.accept(fco, null);
    consideredOperators = fco.getConsideredOperators();
    nonConsideredOperators = fco.getNonConsideredOperators();

    assertEquals(0, consideredOperators.size());

    assertEquals(2, nonConsideredOperators.size());
    assertTrue(nonConsideredOperators.stream().allMatch(op -> op.getProps().getMajorFragmentId() == 3));
    assertEquals(2, nonConsideredOperators.stream().map(op -> op.getProps().getLocalOperatorId()).distinct().count());


    // visiting wrong fragment root
    fco = new MemoryAllocationUtilities.FindConsideredOperators(3);
    secondFragmentProject0.accept(fco, null); // fco visiting fragment 2 root instead of fragment 3
    consideredOperators = fco.getConsideredOperators();
    nonConsideredOperators = fco.getNonConsideredOperators();

    assertEquals(0, consideredOperators.size());
    assertEquals(0, nonConsideredOperators.size());
  }

  private static class ConfigurableOperator extends AbstractSingle {

    public ConfigurableOperator(OpProps props, PhysicalOperator child) {
      super(props, child);
    }

    @Override
    public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
      return physicalVisitor.visitOp(this, value);
    }

    @Override
    public int getOperatorType() {
      return 0;
    }

    @Override
    protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
      return new ConfigurableOperator(props, child);
    }

  }
}
