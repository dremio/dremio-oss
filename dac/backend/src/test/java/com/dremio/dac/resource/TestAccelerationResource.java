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

import static com.dremio.dac.resource.ApiIntentMessageMapper.toLayoutId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.proto.model.acceleration.AccelerationApiDescriptor;
import com.dremio.dac.proto.model.acceleration.AccelerationStateApiDescriptor;
import com.dremio.dac.proto.model.acceleration.LayoutApiDescriptor;
import com.dremio.dac.proto.model.acceleration.LayoutContainerApiDescriptor;
import com.dremio.dac.proto.model.acceleration.LayoutDetailsApiDescriptor;
import com.dremio.dac.proto.model.acceleration.LayoutDimensionFieldApiDescriptor;
import com.dremio.dac.proto.model.acceleration.LayoutFieldApiDescriptor;
import com.dremio.dac.proto.model.acceleration.LogicalAggregationApiDescriptor;
import com.dremio.dac.proto.model.source.NASConfig;
import com.dremio.dac.server.GenericErrorMessage;
import com.dremio.dac.server.UserExceptionMapper.ErrorMessageWithContext;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.service.accelerator.AccelerationTestUtil;
import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.accelerator.DependencyGraph;
import com.dremio.service.accelerator.proto.AccelerationId;
import com.dremio.service.accelerator.proto.LayoutId;
import com.dremio.service.accelerator.proto.LayoutType;
import com.dremio.service.accelerator.proto.Materialization;
import com.dremio.service.accelerator.proto.MaterializationState;
import com.dremio.service.accelerator.proto.PartitionDistributionStrategy;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.NoOpJobStatusListener;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.proto.TimePeriod;
import com.dremio.service.users.SystemUser;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 * Test class for {@code AccelerationResource}
 */
public class TestAccelerationResource extends AccelerationTestUtil {

  private final DatasetPath physicalFileAtHome = new DatasetPath(Arrays.asList(HOME_NAME, "biz"));
  // 100 years
  static final long IGNORE_REFRESH_TTL_MILLIS = AccelerationUtils.toMillis(new TimePeriod().setDuration(1200L).setUnit(TimePeriod.TimeUnit.MONTHS));

  private static boolean created = false;

  @Before
  public void setup() throws Exception {
    if (created) {
      return;
    }
    created = true;

    addCPSource();
    addEmployeesJson();
    uploadHomeFile();

    l(MaterializationDescriptorProvider.class).setDebug(true);
  }

  @After
  public void clear() throws Exception {
    getAccelerationService().developerService().clearAllAccelerations();
  }

  private void uploadHomeFile() throws Exception {
    final FormDataMultiPart form = new FormDataMultiPart();
    final FormDataBodyPart fileBody = new FormDataBodyPart("file", com.dremio.common.util.FileUtils.getResourceAsString("/testfiles/yelp_biz.json"), MediaType.MULTIPART_FORM_DATA_TYPE);
    form.bodyPart(fileBody);
    form.bodyPart(new FormDataBodyPart("fileName", "biz"));

    com.dremio.file.File file1 = expectSuccess(getBuilder(getAPIv2().path("home/" + HOME_NAME + "/upload_start/").queryParam("extension", "json"))
        .buildPost(Entity.entity(form, form.getMediaType())), com.dremio.file.File.class);
    file1 = expectSuccess(getBuilder(getAPIv2().path("home/" + HOME_NAME + "/upload_finish/biz"))
        .buildPost(Entity.json(file1.getFileFormat().getFileFormat())), com.dremio.file.File.class);

    FileFormat fileFormat = file1.getFileFormat().getFileFormat();
    assertEquals(physicalFileAtHome.getLeaf().getName(), fileFormat.getName());
    assertEquals(physicalFileAtHome.toPathList(), fileFormat.getFullPath());
    assertEquals(FileType.JSON, fileFormat.getFileType());

    fileBody.cleanup();
  }

  @Test
  public void testRawNonScalarDistributionField() throws Exception {
    testAddSchoolField(false, TestFieldType.DIST);
  }
  @Test
  public void testRawNonScalarPartitionField() throws Exception {
    testAddSchoolField(false, TestFieldType.PART);
  }
  @Test
  public void testRawNonScalarSortField() throws Exception {
    testAddSchoolField(false, TestFieldType.SORT);
  }
  @Test
  public void testAggNonScalarDistributionField() throws Exception {
    testAddSchoolField(true, TestFieldType.DIST);
  }
  @Test
  public void testAggNonScalarPartitionField() throws Exception {
    testAddSchoolField(true, TestFieldType.PART);
  }
  @Test
  public void testAggNonScalarSortField() throws Exception {
    testAddSchoolField(true, TestFieldType.SORT);
  }

  enum TestFieldType {
    DIST,
    PART,
    SORT
  }

  private void testAddSchoolField(boolean agg, TestFieldType type) throws Exception {
    final DatasetPath path = physicalFileAtHome;
    final AccelerationApiDescriptor newApiDescriptor = createNewAcceleration(path);
    assertEquals(path, new DatasetPath(newApiDescriptor.getContext().getDataset().getPathList()));
    final AccelerationApiDescriptor completed = waitForLayoutGeneration(newApiDescriptor.getId());
    // Field "schools" is a list
    final LayoutFieldApiDescriptor schoolsField = new LayoutFieldApiDescriptor("schools");

    final LayoutDetailsApiDescriptor details;
    if (agg) {
      details = completed.getAggregationLayouts().getLayoutList().get(0).getDetails();
      details.getDimensionFieldList().add(new LayoutDimensionFieldApiDescriptor("schools"));
      completed.getAggregationLayouts().setEnabled(true);
    } else {
      details = completed.getRawLayouts().getLayoutList().get(0).getDetails();
      completed.getRawLayouts().setEnabled(true);
    }
    switch (type) {
      case DIST:
        details.getDistributionFieldList().add(schoolsField);
        break;
      case PART:
        details.getPartitionFieldList().add(schoolsField);
        break;
      case SORT:
        details.getSortFieldList().add(schoolsField);
        break;
      default:
        assertTrue("unknown type, " + type, false);
    }

    // should fail to save:
    expectStatus(Status.BAD_REQUEST,
      getBuilder(getAPIv2().path(String.format("/accelerations/%s", completed.getId().getId()))).buildPut(Entity.entity(MAPPER.toIntentMessage(completed), JSON)),
      ErrorMessageWithContext.class);
  }

  @Test
  public void testHomeFileAcceleration() throws Exception {
    final DatasetPath path = physicalFileAtHome;

    final AccelerationApiDescriptor newApiDescriptor = createNewAcceleration(path);
    assertEquals(path, new DatasetPath(newApiDescriptor.getContext().getDataset().getPathList()));

    final AccelerationApiDescriptor existingApiDescriptor = pollAcceleration(newApiDescriptor.getId());
    assertEquals(newApiDescriptor.getId(), existingApiDescriptor.getId());
    assertEquals(newApiDescriptor.getType(), existingApiDescriptor.getType());

    final AccelerationApiDescriptor completed = waitForLayoutGeneration(newApiDescriptor.getId());
    completed.getRawLayouts().setEnabled(true);

    saveAcceleration(completed.getId(), completed);

    waitForMaterialization(completed.getId(), false);

    //make sure that expiry time is more than a 100 years
    Iterable<Materialization> materializations = getAccelerationService().getMaterializations(
      toLayoutId(completed.getRawLayouts().getLayoutList().get(0).getId()));
    for (Materialization materialization : materializations) {
      assertTrue(materialization.getExpiration() > System.currentTimeMillis() + IGNORE_REFRESH_TTL_MILLIS);
    }

    //make sure that dependency graph do not contain home folders
    DependencyGraph graph = getAccelerationService().developerService().getDependencyGraph();
    assertEquals(0, graph.getChainExecutors().size());
  }

  @Test
  public void testMaterializationAttributes() throws Exception {
    final DatasetPath path = physicalFileAtHome;

    final AccelerationApiDescriptor newApiDescriptor = createNewAcceleration(path);
    assertEquals(path, new DatasetPath(newApiDescriptor.getContext().getDataset().getPathList()));

    final AccelerationApiDescriptor existingApiDescriptor = pollAcceleration(newApiDescriptor.getId());
    assertEquals(newApiDescriptor.getId(), existingApiDescriptor.getId());
    assertEquals(newApiDescriptor.getType(), existingApiDescriptor.getType());

    final AccelerationApiDescriptor completed = waitForLayoutGeneration(newApiDescriptor.getId());
    completed.getRawLayouts().setEnabled(true);

    saveAcceleration(completed.getId(), completed);

    waitForMaterialization(completed.getId(), false);
    waitForCachedMaterialization(completed.getId());

    AccelerationApiDescriptor updatedApiDescriptor = pollAcceleration(completed.getId());

    LayoutApiDescriptor rawLayout = updatedApiDescriptor.getRawLayouts().getLayoutList().get(0);
    assertTrue(rawLayout.getHasValidMaterialization());
    assertEquals(MaterializationState.DONE, rawLayout.getLatestMaterializationState());
    assertTrue(rawLayout.getCurrentByteSize() > 0);
    assertEquals(rawLayout.getCurrentByteSize(), rawLayout.getTotalByteSize());
  }


  @Test
  public void testAccelerationNotExists() throws Exception {
    final String id = "non-existing-path";
    expectStatus(Response.Status.NOT_FOUND,
        getBuilder(getAPIv2().path(String.format("/accelerations/%s", id))).buildGet(),
        GenericErrorMessage.class
    );
  }

  @Test
  public void testDoubleCreateThrowsConflict() throws Exception {
    final AccelerationApiDescriptor newApiDescriptor = createNewAcceleration(EMPLOYEES_VIRTUAL);

    assertEquals(EMPLOYEES_VIRTUAL, new DatasetPath(newApiDescriptor.getContext().getDataset().getPathList()));

    expectStatus(Response.Status.CONFLICT,
        getBuilder(getAPIv2().path("/accelerations")).buildPost(Entity.entity(EMPLOYEES_VIRTUAL.toPathList(), JSON)),
        GenericErrorMessage.class
    );

    while(!getAccelerationService().isPipelineCompletedOrNotStarted(newApiDescriptor.getId())) {
      Thread.sleep(1000);
    }
  }

  @Test
  public void testPhysicalDatasetAcceleration() throws Exception {
    testDataset(EMPLOYEES);
  }

  @Test
  public void testVirtualDatasetAcceleration() throws Exception {
    testDataset(EMPLOYEES_VIRTUAL);
  }

  @Test
  public void testLogicalAggregationUpdate() throws Exception {
    final DatasetPath path = EMPLOYEES_VIRTUAL;

    final AccelerationApiDescriptor newApiDescriptor = createNewAcceleration(path);
    assertEquals(path, new DatasetPath(newApiDescriptor.getContext().getDataset().getPathList()));

    final AccelerationApiDescriptor completed = waitForLayoutGeneration(newApiDescriptor.getId());
    assertEquals(newApiDescriptor.getId(), completed.getId());
    assertEquals(AccelerationStateApiDescriptor.DISABLED, completed.getState());
    assertFalse("aggregation layout generation failed", completed.getAggregationLayouts().getLayoutList().isEmpty());
    assertFalse("raw layout generation failed", completed.getRawLayouts().getLayoutList().isEmpty());
    assertFalse("dataset schema is required", completed.getContext().getDatasetSchema().getFieldList().isEmpty());

    final LogicalAggregationApiDescriptor logical = completed.getContext().getLogicalAggregation();
    assertFalse("logical dimensions are required", logical.getDimensionList().isEmpty());
    assertFalse("logical measures are required", logical.getMeasureList().isEmpty());

    logical.setDimensionList(logical.getDimensionList().subList(1, logical.getDimensionList().size()));
    logical.setMeasureList(logical.getMeasureList().subList(1, logical.getMeasureList().size()));

    final AccelerationApiDescriptor updated = saveAcceleration(completed.getId(), completed);

    final LogicalAggregationApiDescriptor updatedLogical = updated.getContext().getLogicalAggregation();
    assertEquals(logical.getDimensionList(), updatedLogical.getDimensionList());
    assertEquals(logical.getMeasureList(), updatedLogical.getMeasureList());

    {
      final AccelerationApiDescriptor finalApiDescriptor = waitForLayoutGeneration(newApiDescriptor.getId());
      assertEquals(newApiDescriptor.getId(), finalApiDescriptor.getId());
      assertEquals(AccelerationStateApiDescriptor.DISABLED, finalApiDescriptor.getState());
      assertFalse("aggregation layout generation failed", finalApiDescriptor.getAggregationLayouts().getLayoutList().isEmpty());
      assertFalse("raw layout generation failed", finalApiDescriptor.getRawLayouts().getLayoutList().isEmpty());
      assertFalse("dataset schema is required", finalApiDescriptor.getContext().getDatasetSchema().getFieldList().isEmpty());

      final LogicalAggregationApiDescriptor finalLogical = finalApiDescriptor.getContext().getLogicalAggregation();
      assertEquals(logical.getDimensionList(), finalLogical.getDimensionList());
      assertEquals(logical.getMeasureList(), finalLogical.getMeasureList());
    }
  }

  @Test
  public void testIdPreservationOnUpdate() throws Exception {
    final DatasetPath path = EMPLOYEES_VIRTUAL;

    final AccelerationApiDescriptor newApiDescriptor = createNewAcceleration(path);
    assertEquals(path, new DatasetPath(newApiDescriptor.getContext().getDataset().getPathList()));

    final AccelerationApiDescriptor completed = waitForLayoutGeneration(newApiDescriptor.getId());
    assertEquals(newApiDescriptor.getId(), completed.getId());

    final LogicalAggregationApiDescriptor logical = completed.getContext().getLogicalAggregation();

    //make sure there is only one aggregation layout suggested by the system
    assertEquals(1, completed.getAggregationLayouts().getLayoutList().size());

    //now get the aggregate layout id and save it
    LayoutApiDescriptor aggLayout = completed.getAggregationLayouts().getLayoutList().get(0);
    LayoutId aggLayoutId = toLayoutId(aggLayout.getId());
    assertNotNull(aggLayoutId);

    //make sure that there is an empty name string
    assertEquals("", aggLayout.getName());

    //make sure there is only one aggregation layout suggested by the system
    assertEquals(1, completed.getRawLayouts().getLayoutList().size());

    //now get the raw layout id and save it
    LayoutApiDescriptor rawLayout = completed.getRawLayouts().getLayoutList().get(0);
    LayoutId rawLayoutId = toLayoutId(rawLayout.getId());
    assertNotNull(rawLayoutId);

    //make sure that there is an empty name string
    assertEquals("", rawLayout.getName());

    logical.setDimensionList(logical.getDimensionList().subList(1, logical.getDimensionList().size()));
    logical.setMeasureList(logical.getMeasureList().subList(1, logical.getMeasureList().size()));

    final AccelerationApiDescriptor updated = saveAcceleration(completed.getId(), completed);

    final LogicalAggregationApiDescriptor updatedLogical = updated.getContext().getLogicalAggregation();
    assertEquals(logical.getDimensionList(), updatedLogical.getDimensionList());
    assertEquals(logical.getMeasureList(), updatedLogical.getMeasureList());

    assertEquals(1, updated.getAggregationLayouts().getLayoutList().size());

    //now get the aggregate layout id and compare it
    LayoutApiDescriptor updatedAggLayout = updated.getAggregationLayouts().getLayoutList().get(0);
    LayoutId updatedAggLayoutId = toLayoutId(updatedAggLayout.getId());
    assertNotNull(updatedAggLayoutId);
    assertEquals(aggLayoutId, updatedAggLayoutId);
  }

  @Test
  public void testInvalidIdentifierInApiDescriptor() throws Exception {
    final DatasetPath path = EMPLOYEES_VIRTUAL;

    final AccelerationApiDescriptor newApiDescriptor = createNewAcceleration(path);

    assertEquals(path, new DatasetPath(newApiDescriptor.getContext().getDataset().getPathList()));

    final AccelerationApiDescriptor finalApiDescriptor = waitForLayoutGeneration(newApiDescriptor.getId());
    assertEquals(newApiDescriptor.getId(), finalApiDescriptor.getId());

    // invalid id
    {
      finalApiDescriptor.setId(null);
      expectStatus(Response.Status.BAD_REQUEST,
          getBuilder(getAPIv2().path(String.format("/accelerations/%s", newApiDescriptor.getId().getId())))
              .buildPut(Entity.entity(MAPPER.toIntentMessage(finalApiDescriptor), JSON)),
          GenericErrorMessage.class
      );

      finalApiDescriptor.setId(new AccelerationId());
      expectStatus(Response.Status.BAD_REQUEST,
          getBuilder(getAPIv2().path(String.format("/accelerations/%s", newApiDescriptor.getId().getId())))
              .buildPut(Entity.entity(MAPPER.toIntentMessage(finalApiDescriptor), JSON)),
          GenericErrorMessage.class
      );

      finalApiDescriptor.setId(new AccelerationId().setId("---"));
      expectStatus(Response.Status.BAD_REQUEST,
          getBuilder(getAPIv2().path(String.format("/accelerations/%s", newApiDescriptor.getId().getId())))
              .buildPut(Entity.entity(MAPPER.toIntentMessage(finalApiDescriptor), JSON)),
          GenericErrorMessage.class
      );
    }
  }

  @Test
  public void testInvalidLogicalMeasureAndDimensionInApiDescriptor() throws Exception {
    final DatasetPath path = EMPLOYEES_VIRTUAL;

    final AccelerationApiDescriptor newApiDescriptor = createNewAcceleration(path);

    assertEquals(path, new DatasetPath(newApiDescriptor.getContext().getDataset().getPathList()));

    waitForLayoutGeneration(newApiDescriptor.getId());

    // invalid logical measures and dimensions: null case
    {
      final AccelerationApiDescriptor descriptor = pollAcceleration(newApiDescriptor.getId());

      descriptor.getAggregationLayouts().setEnabled(true);
      descriptor.getContext().getLogicalAggregation().setMeasureList(null);
      descriptor.getContext().getLogicalAggregation().setDimensionList(null);
      saveUpdateExpectError(descriptor, newApiDescriptor.getId().getId());
    }

    // invalid logical measures and dimensions: empty case
    {
      final AccelerationApiDescriptor descriptor = pollAcceleration(newApiDescriptor.getId());

      descriptor.getAggregationLayouts().setEnabled(true);
      descriptor.getContext().getLogicalAggregation().setMeasureList(ImmutableList.<LayoutFieldApiDescriptor>of());
      descriptor.getContext().getLogicalAggregation().setDimensionList(ImmutableList.<LayoutFieldApiDescriptor>of());
      saveUpdateExpectError(descriptor, newApiDescriptor.getId().getId());
    }

    // invalid logical measure: invalid field name
    {
      final AccelerationApiDescriptor descriptor = pollAcceleration(newApiDescriptor.getId());

      descriptor.getAggregationLayouts().setEnabled(true);
      descriptor.getContext().getLogicalAggregation().setMeasureList(ImmutableList.of(new LayoutFieldApiDescriptor().setName("xxx")));
      saveUpdateExpectError(descriptor, newApiDescriptor.getId().getId());
    }

    // invalid logical dimension: invalid field name
    {
      final AccelerationApiDescriptor descriptor = pollAcceleration(newApiDescriptor.getId());

      descriptor.getAggregationLayouts().setEnabled(true);
      descriptor.getContext().getLogicalAggregation().setDimensionList(ImmutableList.of(new LayoutFieldApiDescriptor().setName("xxx")));
      saveUpdateExpectError(descriptor, newApiDescriptor.getId().getId());
    }
  }

  public void saveUpdateExpectError(final AccelerationApiDescriptor descriptor, String id) {
    expectStatus(Response.Status.BAD_REQUEST,
      getBuilder(getAPIv2().path(String.format("/accelerations/%s", id)))
        .buildPut(Entity.entity(MAPPER.toIntentMessage(descriptor), JSON)),
      GenericErrorMessage.class
    );
  }

  public void saveUpdateAndValidate(final AccelerationApiDescriptor descriptor, String id) {
    final AccelerationApiDescriptor response = expectSuccess(
      getBuilder(getAPIv2().path(String.format("/accelerations/%s", id)))
        .buildPut(Entity.entity(MAPPER.toIntentMessage(descriptor), JSON)),
      AccelerationApiDescriptor.class
    );
    // version is increased as part of successful modification
    // TODO DX-7795 - this check fails in the multi-node run of these tests, seems like some updates are happening
    // concurrently when the tests are run in this configuration
    //assertEquals(descriptor.getVersion() + 1, (long)response.getVersion());
    response.setVersion(descriptor.getVersion());
    // API canonicalizes null to empty list
    descriptor.getContext().getLogicalAggregation().setDimensionList(ImmutableList.<LayoutFieldApiDescriptor>of());
    assertEquals(descriptor, response);
  }

  @Ignore("DX-8716")
  @Test
  public void testMissingLogicalDimensionOrMeasureInApiDescriptor() throws Exception {
    final DatasetPath path = EMPLOYEES_VIRTUAL;

    final AccelerationApiDescriptor newApiDescriptor = createNewAcceleration(path);

    assertEquals(path, new DatasetPath(newApiDescriptor.getContext().getDataset().getPathList()));

    waitForLayoutGeneration(newApiDescriptor.getId());

    // logical dimension: null case
    {
      final AccelerationApiDescriptor descriptor = pollAcceleration(newApiDescriptor.getId());
      descriptor.getContext().getLogicalAggregation().setDimensionList(null);
      saveUpdateAndValidate(descriptor, newApiDescriptor.getId().getId());
    }

    // logical dimension: empty case
    {
      final AccelerationApiDescriptor descriptor = pollAcceleration(newApiDescriptor.getId());

      descriptor.getContext().getLogicalAggregation().setDimensionList(ImmutableList.<LayoutFieldApiDescriptor>of());
      saveUpdateAndValidate(descriptor, newApiDescriptor.getId().getId());
    }

    // both can be missing if aggregation acceleration is disabled
    {
      final AccelerationApiDescriptor descriptor = pollAcceleration(newApiDescriptor.getId());

      descriptor.getAggregationLayouts().setEnabled(false);
      descriptor.getContext().getLogicalAggregation().setMeasureList(ImmutableList.<LayoutFieldApiDescriptor>of());
      descriptor.getContext().getLogicalAggregation().setDimensionList(ImmutableList.<LayoutFieldApiDescriptor>of());
      saveUpdateAndValidate(descriptor, newApiDescriptor.getId().getId());
    }
  }

  @Test
  public void ensurePartitionDistributionStrategyOnLayoutsIsAsRequested() throws Exception {
    final DatasetPath path = EMPLOYEES_VIRTUAL;
    final AccelerationApiDescriptor newApiDescriptor = createNewAcceleration(path);
    final AccelerationApiDescriptor finalApiDescriptor = waitForLayoutGeneration(newApiDescriptor.getId());
    assertEquals(newApiDescriptor.getId(), finalApiDescriptor.getId());
    assertEquals(AccelerationStateApiDescriptor.DISABLED, finalApiDescriptor.getState());

    assertFalse("aggregation layout generation failed", finalApiDescriptor.getAggregationLayouts().getLayoutList().isEmpty());
    assertFalse("raw layout generation failed", finalApiDescriptor.getRawLayouts().getLayoutList().isEmpty());
    assertFalse("dataset schema is required", finalApiDescriptor.getContext().getDatasetSchema().getFieldList().isEmpty());

    // enable both
    {
      finalApiDescriptor.getRawLayouts().setEnabled(true);
      finalApiDescriptor.getAggregationLayouts().setEnabled(true);
      final AccelerationApiDescriptor updatedApiDescriptor = saveAcceleration(finalApiDescriptor.getId(), finalApiDescriptor);
      assertEquals(AccelerationStateApiDescriptor.ENABLED, updatedApiDescriptor.getState());

      assertTrue(updatedApiDescriptor.getRawLayouts().getEnabled());
      assertTrue(updatedApiDescriptor.getAggregationLayouts().getEnabled());
    }

    // adhere to partition policy as requested
    for (int i = 0; i < finalApiDescriptor.getRawLayouts().getLayoutList().size(); i++) {
      final LayoutApiDescriptor layoutApiDescriptor = finalApiDescriptor.getRawLayouts().getLayoutList().get(i);

      // randomly set policy
      layoutApiDescriptor.getDetails()
          .setPartitionDistributionStrategy(
              Math.random() < 0.5 ? PartitionDistributionStrategy.CONSOLIDATED : PartitionDistributionStrategy.STRIPED);

      final AccelerationApiDescriptor updated = saveAcceleration(finalApiDescriptor.getId(), finalApiDescriptor);

      final PartitionDistributionStrategy actual = updated.getRawLayouts()
          .getLayoutList().get(i)
          .getDetails()
          .getPartitionDistributionStrategy();

      // ensure the policy is correctly set
      assertEquals(layoutApiDescriptor.getDetails().getPartitionDistributionStrategy(), actual);
      finalApiDescriptor.setVersion(updated.getVersion());
    }

    // adhere to partition policy as requested
    for (int i = 0; i < finalApiDescriptor.getAggregationLayouts().getLayoutList().size(); i++) {
      final LayoutApiDescriptor layoutApiDescriptor = finalApiDescriptor.getAggregationLayouts()
          .getLayoutList().get(i);

      // randomly set policy
      layoutApiDescriptor.getDetails()
          .setPartitionDistributionStrategy(
              Math.random() < 0.5 ? PartitionDistributionStrategy.CONSOLIDATED : PartitionDistributionStrategy.STRIPED);

      final AccelerationApiDescriptor updated = saveAcceleration(finalApiDescriptor.getId(), finalApiDescriptor);

      final PartitionDistributionStrategy actual = updated.getAggregationLayouts()
          .getLayoutList().get(i)
          .getDetails()
          .getPartitionDistributionStrategy();

      // ensure the policy is correctly set
      assertEquals(layoutApiDescriptor.getDetails().getPartitionDistributionStrategy(), actual);
      finalApiDescriptor.setVersion(updated.getVersion());
    }

    // for now, no need to reset the policy back
  }

  protected void testDataset(final DatasetPath path) throws Exception {
    final AccelerationApiDescriptor newApiDescriptor = createNewAcceleration(path);

    assertEquals(path, new DatasetPath(newApiDescriptor.getContext().getDataset().getPathList()));

    final AccelerationApiDescriptor existingApiDescriptor = pollAcceleration(newApiDescriptor.getId());

    assertEquals(newApiDescriptor.getId(), existingApiDescriptor.getId());
    assertEquals(newApiDescriptor.getType(), existingApiDescriptor.getType());

    final AccelerationApiDescriptor finalApiDescriptor = waitForLayoutGeneration(newApiDescriptor.getId());
    assertEquals(newApiDescriptor.getId(), finalApiDescriptor.getId());
    assertEquals(AccelerationStateApiDescriptor.DISABLED, finalApiDescriptor.getState());
    assertFalse("aggregation layout generation failed", finalApiDescriptor.getAggregationLayouts().getLayoutList().isEmpty());
    assertFalse("raw layout generation failed", finalApiDescriptor.getRawLayouts().getLayoutList().isEmpty());
    assertFalse("dataset schema is required", finalApiDescriptor.getContext().getDatasetSchema().getFieldList().isEmpty());

    for (final LayoutContainerApiDescriptor container : new LayoutContainerApiDescriptor[]{
        finalApiDescriptor.getRawLayouts(),
        finalApiDescriptor.getAggregationLayouts()
    }) {
      // check if acceleration state becomes enabled
      {
        container.setEnabled(true);

        final AccelerationApiDescriptor updated = saveAcceleration(finalApiDescriptor.getId(), finalApiDescriptor);
        assertEquals(AccelerationStateApiDescriptor.ENABLED, updated.getState());

        final boolean actual;
        if (container.getType() == LayoutType.RAW) {
          actual = updated.getRawLayouts().getEnabled();
        } else {
          actual = updated.getAggregationLayouts().getEnabled();
        }
        assertEquals(container.getEnabled(), actual);
        finalApiDescriptor.setVersion(updated.getVersion());
      }

      // check if acceleration state becomes disabled
      {
        container.setEnabled(false);

        final AccelerationApiDescriptor updated = saveAcceleration(finalApiDescriptor.getId(), finalApiDescriptor);
        assertEquals(AccelerationStateApiDescriptor.DISABLED, updated.getState());

        final boolean actual;
        if (container.getType() == LayoutType.RAW) {
          actual = updated.getRawLayouts().getEnabled();
        } else {
          actual = updated.getAggregationLayouts().getEnabled();
        }
        assertEquals(container.getEnabled(), actual);
        finalApiDescriptor.setVersion(updated.getVersion());
      }
    }

    // enable both
    {
      finalApiDescriptor.getRawLayouts().setEnabled(true);
      finalApiDescriptor.getAggregationLayouts().setEnabled(true);
      final AccelerationApiDescriptor updatedApiDescriptor = saveAcceleration(finalApiDescriptor.getId(), finalApiDescriptor);
      assertEquals(AccelerationStateApiDescriptor.ENABLED, updatedApiDescriptor.getState());

      assertTrue(updatedApiDescriptor.getRawLayouts().getEnabled());
      assertTrue(updatedApiDescriptor.getAggregationLayouts().getEnabled());
    }
  }

  @Test
  public void testRequestNotFound() throws Exception {
    final DatasetPath path = EMPLOYEES_VIRTUAL;
    final String endpoint = String.format("/accelerations/dataset/%s", path.toPathString());
    expectStatus(Response.Status.NOT_FOUND,
        getBuilder(getAPIv2().path(endpoint))
            .buildGet()
    );
  }

  @Test
  public void testMaterializationSchemaLearning() throws Exception {
    SourceUI source = new SourceUI();
    source.setName("src1");
    source.setCtime(1000L);

    TemporaryFolder folder = new TemporaryFolder();
    folder.create();

    final NASConfig config1 = new NASConfig();
    config1.setPath(folder.getRoot().getAbsolutePath());
    source.setConfig(config1);
    String sourceResource = "source/src1";

    File srcFolder = folder.getRoot();
    File tableFolder = new File(srcFolder.getAbsolutePath(), "t1");
    tableFolder.mkdir();

    PrintStream f1 = new PrintStream(new File(tableFolder.getAbsolutePath(), "file1.json"));
    for (int i = 0; i < 1000; i++) {
      f1.println("{a:{b:[1,2]}}");
    }
    f1.close();

    PrintStream f2 = new PrintStream(new File(tableFolder.getAbsolutePath(), "file2.json"));
    for (int i = 0; i < 1000; i++) {
      f2.println("{a:{b:[1.0,2.0]}}");
    }
    f2.close();

    final SourceUI putSource1 = expectSuccess(getBuilder(getAPIv2().path(sourceResource)).buildPut(Entity.json(source)), SourceUI.class);

    getJobsService()
        .submitJob(JobRequest.newBuilder()
            .setSqlQuery(new SqlQuery("select * from src1.t1", SystemUser.SYSTEM_USERNAME))
            .setQueryType(QueryType.UI_RUN)
            .build(), NoOpJobStatusListener.INSTANCE);

    DatasetPath path = new DatasetPath(ImmutableList.of("src1", "t1"));

    final DatasetConfig dataset = new DatasetConfig()
      .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER)
      .setFullPathList(path.toPathList())
      .setName(path.getLeaf().getName())
      .setCreatedAt(System.currentTimeMillis())
      .setVersion(null)
      .setOwner(DEFAULT_USERNAME)
      .setPhysicalDataset(new PhysicalDataset()
        .setFormatSettings(new FileConfig().setType(FileType.JSON))
      );
    final NamespaceService nsService = getNamespaceService();
    nsService.addOrUpdateDataset(path.toNamespaceKey(), dataset);

    AccelerationApiDescriptor accel1 = createNewAcceleration(new DatasetPath(ImmutableList.of("src1", "t1")));

    accel1.getRawLayouts().setEnabled(true);

    saveAcceleration(accel1.getId(), accel1);

    waitForLayoutGeneration(accel1.getId());

    // if the job completes successfully, that means it was able to handle the schema learning

    getJobsService().submitJob(JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery("select * from src1.t1", SystemUser.SYSTEM_USERNAME))
        .setQueryType(QueryType.UI_RUN)
        .build(), NoOpJobStatusListener.INSTANCE).getData().loadIfNecessary();
  }

  @Test
  @Ignore
  public void testDependencyCycle() throws Exception {
    String[] queries = new String[] {
      "select sum(l_extendedprice) as price, l_returnflag, l_linestatus from cp.\"tpch/lineitem.parquet\" group by l_returnflag, l_linestatus",
      "select sum(l_extendedprice) as p, l_returnflag, l_linestatus from cp.\"tpch/lineitem.parquet\" group by l_returnflag, l_linestatus"
    };
    for (String query : queries) {
      accelerateQuery(query, TEST_SPACE);
    }
    DependencyGraph graph = getAccelerationService().developerService().getDependencyGraph();
    graph.buildAndSchedule();
    assertNotNull(graph.getChainExecutors());
    assertEquals(1, graph.getChainExecutors().size());
    assertEquals(2, FluentIterable.from(graph.getChainExecutors().get(0)).toList().size());
  }
}
