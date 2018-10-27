/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.exec.ExecTest;
import com.dremio.exec.physical.base.AbstractSingle;
import com.dremio.exec.physical.base.MemoryCalcConsidered;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.physical.config.EmptyValues;
import com.dremio.exec.physical.config.ExternalSort;
import com.dremio.exec.physical.config.SingleSender;
import com.dremio.exec.physical.config.UnorderedReceiver;
import com.dremio.exec.planner.fragment.Fragment;
import com.dremio.exec.planner.fragment.Wrapper;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.exec.store.sys.store.provider.KVPersistentStoreProvider;
import com.dremio.options.OptionManager;
import com.dremio.service.DirectProvider;
import com.dremio.test.UserExceptionMatcher;
import com.google.common.collect.ImmutableMap;

public class TestMemoryAllocationUtilities extends ExecTest {

  private static final EmptyValues ARBTRIARY_LEAF;
  static {
    ARBTRIARY_LEAF = new EmptyValues(BatchSchema.SCHEMA_UNKNOWN_NO_DATA);
    ARBTRIARY_LEAF.setInitialAllocation(1);
  }

  private static final NodeEndpoint N1 = NodeEndpoint.newBuilder().setAddress("n1").build();
  private static final NodeEndpoint N2 = NodeEndpoint.newBuilder().setAddress("n2").build();

  private SystemOptionManager options;
  private LocalKVStoreProvider kvstoreprovider;

   @Before
   public void setup() throws Exception {
     kvstoreprovider = new LocalKVStoreProvider(CLASSPATH_SCAN_RESULT, null, true, true);
     kvstoreprovider.start();
     options = new SystemOptionManager(
         CLASSPATH_SCAN_RESULT,
         new LogicalPlanPersistence(DEFAULT_SABOT_CONFIG, CLASSPATH_SCAN_RESULT),
         new KVPersistentStoreProvider(DirectProvider.wrap(kvstoreprovider)));
     options.init();
  }

   @After
   public void teardown() throws Exception {
     AutoCloseables.close(options, kvstoreprovider);
   }

  /**
   * Check that we handle bounded and unbounded correctly. Also validate handling of weights correctly.
   */
  @Test
  public void syntheticSimple() {
    ConfigurableOperator cnb = new ConfigurableOperator(ARBTRIARY_LEAF, 2.0d, false);
    cnb.setInitialAllocation(1l);
    ConfigurableOperator cb = new ConfigurableOperator(cnb, 1.0d, true);
    cb.setInitialAllocation(1l);
    Fragment f1 = new Fragment();
    f1.addOperator(cb);
    Wrapper w1 = new Wrapper(f1, 0);
    w1.overrideEndpoints(Collections.singletonList(N1));
    MemoryAllocationUtilities.setMemory(options, ImmutableMap.of(f1, w1), 10);
    assertEquals(Long.MAX_VALUE, cnb.getMaxAllocation());
    assertEquals(3, cb.getMaxAllocation());
  }

  @Test
  public void doubleSort() {
    ExternalSort es1 = new ExternalSort(ARBTRIARY_LEAF, Collections.emptyList(), false);
    es1.setInitialAllocation(0);
    ExternalSort es2 = new ExternalSort(es1, Collections.emptyList(), false);
    es2.setInitialAllocation(0);
    Fragment f1 = new Fragment();
    f1.addOperator(es2);
    Wrapper wrapper = new Wrapper(f1, 0);
    wrapper.overrideEndpoints(Collections.singletonList(N1));
    MemoryAllocationUtilities.setMemory(options, ImmutableMap.of(f1, wrapper), 10);
    assertEquals(4l, es1.getMaxAllocation());
    assertEquals(4l, es2.getMaxAllocation());
  }

  @Test
  public void doubleSortWithExchange() {
    ExternalSort es1 = new ExternalSort(ARBTRIARY_LEAF, Collections.emptyList(), false);
    es1.setInitialAllocation(0);
    SingleSender ss = new SingleSender(0, es1, N1, Mockito.mock(BatchSchema.class));
    ss.setInitialAllocation(1);
    Fragment f1 = new Fragment();
    f1.addOperator(ss);
    Wrapper w1 = new Wrapper(f1, 0);
    w1.overrideEndpoints(Collections.singletonList(N1));

    UnorderedReceiver or = new UnorderedReceiver(0, Collections.emptyList(), false, Mockito.mock(BatchSchema.class));
    or.setInitialAllocation(1);
    ExternalSort es2 = new ExternalSort(or, Collections.emptyList(), false);
    es2.setInitialAllocation(0);
    Fragment f2 = new Fragment();
    f2.addOperator(es2);
    Wrapper w2 = new Wrapper(f2, 0);
    w2.overrideEndpoints(Collections.singletonList(N1));


    MemoryAllocationUtilities.setMemory(options, ImmutableMap.of(f1, w1, f2, w2), 10);
    assertEquals(3l, es1.getMaxAllocation());
    assertEquals(3l, es2.getMaxAllocation());
  }

  /**
   * Even though N2 only has one sort, we set the overall limit to the worse case node (same as legacy algorithm).
   */
  @Test
  public void doubleSortWithExchangeUnbalancedNodes() {
    ExternalSort es1 = new ExternalSort(ARBTRIARY_LEAF, Collections.emptyList(), false);
    es1.setInitialAllocation(0);
    SingleSender ss = new SingleSender(0, es1, N1, Mockito.mock(BatchSchema.class));
    ss.setInitialAllocation(1);
    Fragment f1 = new Fragment();
    f1.addOperator(ss);
    Wrapper w1 = new Wrapper(f1, 0);
    w1.overrideEndpoints(Arrays.asList(N1, N2));

    UnorderedReceiver or = new UnorderedReceiver(0, Collections.emptyList(), false, Mockito.mock(BatchSchema.class));
    or.setInitialAllocation(1);
    ExternalSort es2 = new ExternalSort(or, Collections.emptyList(), false);
    es2.setInitialAllocation(0);
    Fragment f2 = new Fragment();
    f2.addOperator(es2);
    Wrapper w2 = new Wrapper(f2, 0);
    w2.overrideEndpoints(Collections.singletonList(N1));


    MemoryAllocationUtilities.setMemory(options, ImmutableMap.of(f1, w1, f2, w2), 10);
    assertEquals(3l, es1.getMaxAllocation());
    assertEquals(3l, es2.getMaxAllocation());
  }

  @Test
  public void initialMemoryTooHigh() {
    ConfigurableOperator cnb = new ConfigurableOperator(ARBTRIARY_LEAF, 2.0d, false);
    cnb.setInitialAllocation(2_000_000l);
    ConfigurableOperator cb = new ConfigurableOperator(cnb, 1.0d, true);
    cb.setInitialAllocation(3_000_000l);
    Fragment f1 = new Fragment();
    f1.addOperator(cb);
    Wrapper w1 = new Wrapper(f1, 0);
    w1.overrideEndpoints(Collections.singletonList(N1));
    w1.setMaxAllocation(4_000_000l); // Too low to fit the two operators
    thrownException.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.RESOURCE,
      "Query was cancelled because the initial memory requirement (5MB) is greater than the job memory limit set by the administrator (4MB)"));
    MemoryAllocationUtilities.setMemory(options, ImmutableMap.of(f1, w1), 10_000_000l);
  }

  private static class ConfigurableOperator extends AbstractSingle implements MemoryCalcConsidered {

    private final double memoryFactor;
    private final boolean isBounded;

    public ConfigurableOperator(PhysicalOperator child, double memoryFactor, boolean isBounded) {
      super(child);
      this.memoryFactor = memoryFactor;
      this.isBounded = isBounded;
    }

    @Override
    public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
      return physicalVisitor.visitOp(this, value);
    }

    @Override
    public double getMemoryFactor(OptionManager options) {
      return memoryFactor;
    }

    @Override
    public int getOperatorType() {
      return 0;
    }

    @Override
    public boolean shouldBeMemoryBounded(OptionManager options) {
      return isBounded;
    }

    @Override
    protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
      return new ConfigurableOperator(child, memoryFactor, isBounded);
    }

  }
}
