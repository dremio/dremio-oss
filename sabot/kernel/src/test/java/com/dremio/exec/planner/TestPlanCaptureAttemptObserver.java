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
package com.dremio.exec.planner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.acceleration.RelWithInfo;
import com.dremio.exec.planner.acceleration.descriptor.MaterializationDescriptor;
import com.dremio.exec.planner.acceleration.descriptor.ReflectionInfo;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionInfo;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionInfo.Substitution;
import com.dremio.exec.planner.common.PlannerMetrics;
import com.dremio.exec.planner.serialization.RelSerializerFactory;
import com.dremio.exec.proto.UserBitShared.ReflectionType;
import com.google.common.collect.ImmutableList;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestPlanCaptureAttemptObserver {

  @Mock private Meter.MeterProvider<Counter> provider;
  @Mock private Counter counter;
  @Mock private SubstitutionInfo info;

  private PlanCaptureAttemptObserver observer;

  @Before
  public void setup() {
    when(provider.withTag(any(), any())).thenReturn(counter);
    observer =
        new PlanCaptureAttemptObserver(
            true,
            true,
            Mockito.mock(FunctionImplementationRegistry.class),
            Mockito.mock(AccelerationDetailsPopulator.class),
            Mockito.mock(RelSerializerFactory.class));
  }

  @Test
  public void testCounterIncrement() {

    try (MockedStatic<PlannerMetrics> mockedPlannerMetrics = mockStatic(PlannerMetrics.class)) {
      mockedPlannerMetrics.when(PlannerMetrics::getAcceleratedQueriesCounter).thenReturn(provider);

      // Verify counter is incremented on accelerated query
      observer.setNumJoinsInUserQuery(7);
      DremioMaterialization materialization = Mockito.mock(DremioMaterialization.class);
      when(materialization.getReflectionId()).thenReturn("reflectionId");
      when(materialization.getMaterializationId()).thenReturn("materializationId");
      when(materialization.getLayoutInfo())
          .thenReturn(
              new ReflectionInfo(
                  "reflectionId",
                  ReflectionType.RAW,
                  "reflectionName",
                  false,
                  null,
                  null,
                  null,
                  null,
                  null,
                  null));
      SubstitutionInfo.Substitution substitution = Mockito.mock(Substitution.class);
      MaterializationDescriptor descriptor = Mockito.mock(MaterializationDescriptor.class);
      when(descriptor.getLayoutId()).thenReturn("reflectionId");
      when(substitution.getMaterialization()).thenReturn(descriptor);
      when(info.getSubstitutions()).thenReturn(ImmutableList.of(substitution));
      List<RelWithInfo> substitutions = ImmutableList.of();
      RelWithInfo relWithInfo = Mockito.mock(RelWithInfo.class);
      when(relWithInfo.getInfo()).thenReturn("foo_matching_target");
      observer.planSubstituted(materialization, substitutions, relWithInfo, 0, false);
      observer.planAccelerated(info);
      verify(counter, times(1)).increment();
      verify(provider, times(1)).withTag(PlannerMetrics.TAG_TARGET, "[foo_matching]");
    }
  }

  @Test
  public void testNoDoubleCount() {

    PlanCaptureAttemptObserver observer =
        new PlanCaptureAttemptObserver(
            true,
            true,
            Mockito.mock(FunctionImplementationRegistry.class),
            Mockito.mock(AccelerationDetailsPopulator.class),
            Mockito.mock(RelSerializerFactory.class));

    try (MockedStatic<PlannerMetrics> mockedPlannerMetrics = mockStatic(PlannerMetrics.class)) {
      mockedPlannerMetrics.when(PlannerMetrics::getAcceleratedQueriesCounter).thenReturn(provider);

      // Verify if joins is not set then no counter is incremented.
      // This simulates the replay on a FlightSQL execute job.
      observer.planAccelerated(info);
      verify(counter, never()).increment();
    }
  }
}
