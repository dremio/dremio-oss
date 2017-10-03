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
package com.dremio.dac.resource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.dremio.dac.proto.model.acceleration.AccelerationApiDescriptor;
import com.dremio.dac.proto.model.acceleration.AccelerationStateApiDescriptor;
import com.dremio.dac.proto.model.acceleration.ApiErrorCode;
import com.dremio.dac.proto.model.acceleration.ApiErrorDetails;
import com.dremio.dac.proto.model.acceleration.MaterializationFailureDetails;
import com.dremio.service.accelerator.AccelerationService;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationContextDescriptor;
import com.dremio.service.accelerator.proto.AccelerationDescriptor;
import com.dremio.service.accelerator.proto.AccelerationEntry;
import com.dremio.service.accelerator.proto.AccelerationId;
import com.dremio.service.accelerator.proto.AccelerationState;
import com.dremio.service.accelerator.proto.AccelerationStateDescriptor;
import com.dremio.service.accelerator.proto.JobDetails;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.LayoutContainer;
import com.dremio.service.accelerator.proto.LayoutContainerDescriptor;
import com.dremio.service.accelerator.proto.LayoutDescriptor;
import com.dremio.service.accelerator.proto.LayoutDetailsDescriptor;
import com.dremio.service.accelerator.proto.LayoutId;
import com.dremio.service.accelerator.proto.Materialization;
import com.dremio.service.accelerator.proto.MaterializationId;
import com.dremio.service.accelerator.proto.MaterializationState;
import com.dremio.service.accelerator.proto.MaterializatonFailure;
import com.dremio.service.accelerator.proto.MaterializedLayout;
import com.dremio.service.accelerator.proto.MaterializedLayoutState;
import com.dremio.service.accelerator.proto.RowType;
import com.dremio.service.accelerator.proto.pipeline.AccelerationPipeline;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Unit tests {@link AccelerationResource}
 */
@RunWith(MockitoJUnitRunner.class)
public class TestAccelerationResourceUnit {

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private AccelerationService accelerationService;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private NamespaceService namespaceService;

  @Test
  public void testMaterializationFailureGetsReported() throws Exception {
    final AccelerationId id = new AccelerationId("acc-id");
    final LayoutId layoutId = new LayoutId("layout-id");

    DatasetConfig ds = new DatasetConfig()
        .setFullPathList(Lists.newArrayList("ds1"));
    final AccelerationDescriptor descriptor = new AccelerationDescriptor()
        .setId(id)
        .setState(AccelerationStateDescriptor.ENABLED)
        .setContext(
            new AccelerationContextDescriptor()
            .setDatasetSchema(new RowType()))
        .setRawLayouts(new LayoutContainerDescriptor()
            .setEnabled(true)
            .setLayoutList(
                ImmutableList.of(new LayoutDescriptor(new LayoutDetailsDescriptor()).setId(layoutId))
                ));
    final AccelerationEntry entry = new AccelerationEntry()
        .setDescriptor(descriptor);

    final Acceleration acceleration = new Acceleration()
        .setId(id)
        .setState(AccelerationState.ENABLED)
        .setRawLayouts(new LayoutContainer()
            .setEnabled(true)
            .setLayoutList(
                ImmutableList.of(new Layout().setId(layoutId))
                )
            );

    final MaterializationId materializationId = new MaterializationId("mat-id");
    final String jobId = "job-id";
    final String message = "some-message";
    final String trace = "some";
    MaterializedLayout materializedLayout = new MaterializedLayout();
    materializedLayout.setLayoutId(layoutId);
    final List<Materialization> materializations = ImmutableList.of(new Materialization()
        .setState(MaterializationState.FAILED)
        .setId(materializationId)
        .setLayoutId(layoutId)
        .setJob(new JobDetails().setJobId(jobId))
        .setFailure(new MaterializatonFailure()
            .setMessage(message)
            .setStackTrace(trace)
            )
        );
    materializedLayout.setMaterializationList(materializations);

    // setup
    when(accelerationService.getAccelerationEntryById(id)).thenReturn(Optional.of(entry));
    when(accelerationService.getAccelerationById(id)).thenReturn(Optional.of(acceleration));
    when(accelerationService.getMaterializedLayout(layoutId)).thenReturn(Optional.of(materializedLayout));
    when(accelerationService.getMaterializations(layoutId)).thenReturn(materializations);
    when(namespaceService.findDatasetByUUID(id.getId())).thenReturn(ds);

    // test
    final AccelerationResource resource = new AccelerationResource(accelerationService, namespaceService);
    final AccelerationApiDescriptor response = resource.getAcceleration(id);

    //verify
    ApiErrorDetails apiErrorDetails = response.getRawLayouts().getLayoutList().get(0).getError();

    MaterializationFailureDetails materializationFailure = apiErrorDetails.getMaterializationFailure();
    assertNotNull(materializationFailure);
    assertEquals(materializationId.getId(), materializationFailure.getMaterializationId());
    assertEquals(jobId, materializationFailure.getJobId());
    assertEquals(ApiErrorCode.MATERIALIZATION_FAILURE, apiErrorDetails.getCode());
    assertEquals(message, apiErrorDetails.getMessage());
    assertEquals(trace, apiErrorDetails.getStackTrace());

    assertEquals(MaterializedLayoutState.ACTIVE,response.getRawLayouts().getLayoutList().get(0).getState());
  }

  @Test
  public void testLayoutFailureGetsReported() throws Exception {
    final AccelerationId id = new AccelerationId("acc-id");
    final LayoutId layoutId = new LayoutId("layout-id");

    DatasetConfig ds = new DatasetConfig()
        .setFullPathList(Lists.newArrayList("ds1"));
    final AccelerationDescriptor descriptor = new AccelerationDescriptor()
        .setId(id)
        .setState(AccelerationStateDescriptor.ENABLED)
        .setContext(
            new AccelerationContextDescriptor()
            .setDatasetSchema(new RowType()))
        .setRawLayouts(new LayoutContainerDescriptor()
            .setEnabled(true)
            .setLayoutList(
                ImmutableList.of(new LayoutDescriptor(new LayoutDetailsDescriptor()).setId(layoutId))
                ));
    final AccelerationEntry entry = new AccelerationEntry()
        .setDescriptor(descriptor);

    final Acceleration acceleration = new Acceleration()
        .setId(id)
        .setState(AccelerationState.ENABLED)
        .setRawLayouts(new LayoutContainer()
            .setEnabled(true)
            .setLayoutList(
                ImmutableList.of(new Layout().setId(layoutId))
                )
            );

    final MaterializationId materializationId = new MaterializationId("mat-id");
    final String jobId = "job-id";
    final String message = "some-message";
    final String trace = "some";
    MaterializedLayout materializedLayout = new MaterializedLayout();
    materializedLayout.setLayoutId(layoutId);
    final List<Materialization> materializations = ImmutableList.of(new Materialization()
        .setState(MaterializationState.FAILED)
        .setId(materializationId)
        .setLayoutId(layoutId)
        .setJob(new JobDetails().setJobId(jobId))
        .setFailure(new MaterializatonFailure()
            .setMessage(message)
            .setStackTrace(trace)
            )
        );
    materializedLayout.setMaterializationList(materializations);
    materializedLayout.setState(MaterializedLayoutState.FAILED);

    // setup
    when(accelerationService.getAccelerationEntryById(id)).thenReturn(Optional.of(entry));
    when(accelerationService.getAccelerationById(id)).thenReturn(Optional.of(acceleration));
    when(accelerationService.getMaterializedLayout(layoutId)).thenReturn(Optional.of(materializedLayout));
    when(accelerationService.getMaterializations(layoutId)).thenReturn(materializations);
    when(namespaceService.findDatasetByUUID(id.getId())).thenReturn(ds);

    // test
    final AccelerationResource resource = new AccelerationResource(accelerationService, namespaceService);
    final AccelerationApiDescriptor response = resource.getAcceleration(id);

    //verify
    ApiErrorDetails apiErrorDetails = response.getRawLayouts().getLayoutList().get(0).getError();

    MaterializationFailureDetails materializationFailure = apiErrorDetails.getMaterializationFailure();
    assertNotNull(materializationFailure);
    assertEquals(materializationId.getId(), materializationFailure.getMaterializationId());
    assertEquals(jobId, materializationFailure.getJobId());
    assertEquals(ApiErrorCode.MATERIALIZATION_FAILURE, apiErrorDetails.getCode());
    assertEquals(message, apiErrorDetails.getMessage());
    assertEquals(trace, apiErrorDetails.getStackTrace());

    assertEquals(MaterializedLayoutState.FAILED,response.getRawLayouts().getLayoutList().get(0).getState());
  }

  @Test
  public void testPipelineFailureGetsReported() throws Exception {
    final AccelerationId id = new AccelerationId("acc-id");
    final AccelerationDescriptor descriptor = new AccelerationDescriptor()
        .setId(id)
        .setState(AccelerationStateDescriptor.ERROR)
        .setContext(
            new AccelerationContextDescriptor()
            .setDatasetSchema(new RowType())
            );
    final AccelerationEntry entry = new AccelerationEntry()
        .setDescriptor(descriptor);
    DatasetConfig ds = new DatasetConfig()
        .setFullPathList(Lists.newArrayList("ds1"));

    final String message = "pipeline-failed";
    final String trace  = "some";
    final Acceleration acceleration = new Acceleration()
        .setId(id)
        .setState(AccelerationState.ERROR)
        .setPipeline(new AccelerationPipeline()
            .setFailureDetails(new AccelerationPipeline.FailureDetails()
                .setMessage(message)
                .setStackTrace(trace)
                )
            );


    // setup
    when(accelerationService.getAccelerationEntryById(id)).thenReturn(Optional.of(entry));
    when(accelerationService.getAccelerationById(id)).thenReturn(Optional.of(acceleration));
    when(namespaceService.findDatasetByUUID(id.getId())).thenReturn(ds);

    // test
    final AccelerationResource resource = new AccelerationResource(accelerationService, namespaceService);
    final AccelerationApiDescriptor response = resource.getAcceleration(id);

    // verify
    assertEquals(AccelerationStateApiDescriptor.ERROR, response.getState());
    assertNotNull(response.getErrorList());
    assertFalse(response.getErrorList().isEmpty());

    final ApiErrorDetails failure = response.getErrorList().get(0);
    assertNotNull(failure);
    assertEquals(ApiErrorCode.PIPELINE_FAILURE, failure.getCode());
    assertEquals(message, failure.getMessage());
    assertEquals(trace, failure.getStackTrace());
  }

}
