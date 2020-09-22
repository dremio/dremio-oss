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

import java.util.Arrays;
import java.util.Collections;

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
    assertEquals(3l, es1.getProps().getMemLimit());
    assertEquals(3l, es2.getProps().getMemLimit());
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
