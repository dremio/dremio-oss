/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.service.accelerator.pipeline;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ConcurrentModificationException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationId;
import com.dremio.service.accelerator.proto.AccelerationMode;
import com.dremio.service.accelerator.proto.AccelerationState;
import com.dremio.service.accelerator.proto.pipeline.AccelerationPipeline;
import com.dremio.service.accelerator.proto.pipeline.PipelineState;
import com.dremio.service.accelerator.store.AccelerationStore;
import com.google.common.collect.ImmutableList;

/**
 * Unit tests {@link PipelineDriver}
 */
@RunWith(MockitoJUnitRunner.class)
public class TestPipelineDriver {

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private PipelineContext context;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private PipelineStatusListener listener;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private AccelerationStore accelerationStore;

  @Test
  public void testPipeline() {
    final AccelerationId id = new AccelerationId("acc-id");
    final Acceleration acceleration = new Acceleration()
      .setId(id)
      .setPipeline(new AccelerationPipeline());
    final StageException exception = new StageException();
    final Stage stage = Mockito.mock(Stage.class);
    final Pipeline pipeline = Pipeline.of(acceleration, ImmutableList.of(stage));

    // setup
    when(context.getAccelerationStore()).thenReturn(accelerationStore);

    // test
    final PipelineDriver driver = new PipelineDriver(pipeline, context, listener);
    driver.run();

    // verify
    verify(listener, times(1)).onStart(pipeline);
    verify(listener, times(1)).onComplete(pipeline);
    verify(listener, never()).onPreemption(pipeline);
    verify(listener, never()).onFailure(pipeline, exception);

    verify(accelerationStore, never()).save(acceleration);

    assertEquals(AccelerationMode.AUTO, acceleration.getMode());
    assertEquals(null, acceleration.getState());
    assertEquals(PipelineState.PENDING, acceleration.getPipeline().getState());
  }

  @Test
  public void testPipelineFailure() {
    final AccelerationId id = new AccelerationId("acc-id");
    final Acceleration acceleration = new Acceleration()
        .setId(id)
        .setPipeline(new AccelerationPipeline());
    final StageException exception = new StageException();
    final Stage failingStage = Mockito.mock(Stage.class);
    final Pipeline pipeline = Pipeline.of(acceleration, ImmutableList.of(failingStage));

    // setup
    when(context.getAccelerationStore()).thenReturn(accelerationStore);
    doThrow(exception).when(failingStage).execute(any(StageContext.class));

    // test
    final PipelineDriver driver = new PipelineDriver(pipeline, context, listener);
    driver.run();

    // verify
    verify(listener).onStart(pipeline);
    verify(listener).onFailure(pipeline, exception);
    final ArgumentCaptor<Acceleration> captor = ArgumentCaptor.forClass(Acceleration.class);
    verify(accelerationStore).save(captor.capture());
    assertEquals(AccelerationState.ERROR, captor.getValue().getState());
    assertEquals(PipelineState.FAILED, captor.getValue().getPipeline().getState());
    assertEquals(exception.getMessage(), captor.getValue().getPipeline().getFailureDetails().getMessage());
    assertEquals(AccelerationUtils.getStackTrace(exception), captor.getValue().getPipeline().getFailureDetails().getStackTrace());
  }

  @Test
  public void testConcurrentModificationException() {
    final AccelerationId id = new AccelerationId("acc-id");
    final Acceleration acceleration = new Acceleration()
      .setId(id)
      .setPipeline(new AccelerationPipeline());
    final Stage stage = Mockito.mock(Stage.class);
    final Pipeline pipeline = Pipeline.of(acceleration, ImmutableList.of(stage));

    doThrow(ConcurrentModificationException.class).when(stage).execute(any(StageContext.class));

    // test
    final PipelineDriver driver = new PipelineDriver(pipeline, context, listener);
    driver.run();

    try {
      // Probably not the best way, but since the driver runs an async task ensure it's completed before verifying
      // behaviour.
      Thread.sleep(250);
    } catch (InterruptedException e) {
      Assert.fail();
    }

    final StageException exception = new StageException();
    verify(listener, times(1)).onStart(pipeline);
    verify(listener, times(1)).onComplete(pipeline);
    verify(listener, times(1)).onPreemption(pipeline);
    verify(listener, never()).onFailure(pipeline, exception);

    verify(accelerationStore, never()).save(acceleration);

    assertEquals(AccelerationMode.AUTO, acceleration.getMode());
    assertEquals(null, acceleration.getState());
    assertEquals(PipelineState.PENDING, acceleration.getPipeline().getState());
  }

  @Test
  public void testPipelineRetired() {
    final AccelerationId id = new AccelerationId("acc-id");
    final Acceleration acceleration = new Acceleration()
      .setId(id)
      .setPipeline(new AccelerationPipeline());
    final Stage stage = Mockito.mock(Stage.class);
    final Pipeline pipeline = Pipeline.of(acceleration, ImmutableList.of(stage));

    doThrow(ConcurrentModificationException.class).when(stage).execute(any(StageContext.class));

    // test
    final PipelineDriver driver = new PipelineDriver(pipeline, context, listener);
    driver.retire();
    driver.run();

    final StageException exception = new StageException();
    verify(listener, times(1)).onStart(pipeline);
    verify(listener, times(1)).onComplete(pipeline);
    verify(listener, never()).onPreemption(pipeline);
    verify(listener, never()).onFailure(pipeline, exception);

    verify(accelerationStore, never()).save(acceleration);

    assertEquals(AccelerationMode.AUTO, acceleration.getMode());
    assertEquals(null, acceleration.getState());
    assertEquals(PipelineState.PENDING, acceleration.getPipeline().getState());
  }

  private static class StageException extends RuntimeException { }
}
