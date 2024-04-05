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
package com.dremio.dac.api;

import static com.dremio.exec.ExecConstants.LAYOUT_REFRESH_MAX_ATTEMPTS;
import static com.dremio.exec.ExecConstants.PARQUET_MAXIMUM_PARTITIONS_VALIDATOR;
import static com.dremio.exec.planner.acceleration.IncrementalUpdateUtils.UPDATE_COLUMN;
import static com.dremio.service.reflection.ReflectionStatus.AVAILABILITY_STATUS.NONE;
import static com.dremio.service.reflection.ReflectionStatus.REFRESH_STATUS.GIVEN_UP;
import static com.dremio.service.reflection.ReflectionStatus.REFRESH_STATUS.MANUAL;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.dac.service.reflection.ReflectionStatusUI;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.service.accelerator.AccelerationTestUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.reflection.ReflectionMonitor;
import com.dremio.service.reflection.ReflectionService;
import com.dremio.service.reflection.ReflectionStatusService;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.PartitionDistributionStrategy;
import com.dremio.service.reflection.proto.ReflectionDetails;
import com.dremio.service.reflection.proto.ReflectionDimensionField;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.ReflectionPartitionField;
import com.dremio.service.reflection.proto.ReflectionState;
import com.dremio.service.reflection.proto.ReflectionType;
import com.dremio.service.reflection.store.MaterializationStore;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

/** Test class for {@code ReflectionResource} */
public class TestReflectionResource extends AccelerationTestUtil {
  private static final String REFLECTIONS_PATH = "/reflection/";
  private static final String DEV_OPTIONS_PATH = "/development_options/acceleration/";
  private static final String CATALOG_PATH = "/catalog/";

  private static boolean created = false;

  public static final String HOME_NAME =
      HomeName.getUserHomePath(SampleDataPopulator.DEFAULT_USER_NAME).getName();
  private final DatasetPath physicalFileAtHome = new DatasetPath(Arrays.asList(HOME_NAME, "biz"));

  private static String homeFileId;
  private static String datasetId;
  private static ReflectionMonitor monitor;

  @Before
  public void setup() throws Exception {
    if (created) {
      return;
    }
    created = true;

    monitor =
        new ReflectionMonitor(
            l(ReflectionService.class),
            l(ReflectionStatusService.class),
            l(MaterializationDescriptorProvider.class),
            l(JobsService.class),
            new MaterializationStore(p(LegacyKVStoreProvider.class)),
            SECONDS.toMillis(1),
            SECONDS.toMillis(30));

    addCPSource();
    addEmployeesJson();
    uploadHomeFile();
    Dataset dataset = createDataset();
    datasetId = dataset.getId();
  }

  @After
  public void clear() {
    l(ReflectionService.class).clearAll();
  }

  @AfterClass
  public static void teardown() {
    // wait until the reflection manager doesn't submit any new query, otherwise we may get an
    // exception trying to
    // close query allocators that are still being used
    monitor.waitUntilNoMaterializationsAvailable();
  }

  @Test
  public void testCreateReflection() {
    Reflection newReflection = createReflection();

    Reflection response =
        expectSuccess(
            getBuilder(getPublicAPI(3).path(REFLECTIONS_PATH))
                .buildPost(Entity.entity(newReflection, JSON)),
            Reflection.class);

    assertEquals(response.getDatasetId(), newReflection.getDatasetId());
    assertEquals(response.getName(), newReflection.getName());
    assertEquals(response.getType(), newReflection.getType());
    assertEquals(response.isEnabled(), newReflection.isEnabled());
    assertNotNull(response.getCreatedAt());
    assertNotNull(response.getUpdatedAt());
    assertNotNull(response.getStatus());
    assertNotNull(response.getTag());
    assertNotNull(response.getId());
    assertEquals(response.getDisplayFields(), newReflection.getDisplayFields());
    assertNull(response.getDimensionFields());
    assertNull(response.getDistributionFields());
    assertNull(response.getMeasureFields());
    assertNull(response.getPartitionFields());
    assertNull(response.getSortFields());
    assertEquals(
        response.getPartitionDistributionStrategy(),
        newReflection.getPartitionDistributionStrategy());

    newReflectionServiceHelper().removeReflection(response.getId());
  }

  @Test
  public void testGetReflection() {
    Reflection newReflection = createReflection();
    Reflection response =
        expectSuccess(
            getBuilder(getPublicAPI(3).path(REFLECTIONS_PATH))
                .buildPost(Entity.entity(newReflection, JSON)),
            Reflection.class);

    Reflection response2 =
        expectSuccess(
            getBuilder(getPublicAPI(3).path(REFLECTIONS_PATH).path(response.getId())).buildGet(),
            Reflection.class);

    assertEquals(response2.getId(), response.getId());
    assertEquals(response2.getName(), response.getName());
    assertEquals(response2.getType(), response.getType());
    assertEquals(response2.getCreatedAt(), response.getCreatedAt());
    assertEquals(response2.getUpdatedAt(), response.getUpdatedAt());
    assertEquals(response2.getTag(), response.getTag());
    assertEquals(response2.getDisplayFields(), response.getDisplayFields());
    assertEquals(response2.getDimensionFields(), response.getDimensionFields());
    assertEquals(response2.getDistributionFields(), response.getDistributionFields());
    assertEquals(response2.getMeasureFields(), response.getMeasureFields());
    assertEquals(response2.getPartitionFields(), response.getPartitionFields());
    assertEquals(response2.getSortFields(), response.getSortFields());
    assertEquals(
        response2.getPartitionDistributionStrategy(), response.getPartitionDistributionStrategy());
  }

  @Test
  public void testUpdateReflection() {
    Reflection newReflection = createReflection();
    Reflection response =
        expectSuccess(
            getBuilder(getPublicAPI(3).path(REFLECTIONS_PATH))
                .buildPost(Entity.entity(newReflection, JSON)),
            Reflection.class);

    String betterName = "much better name";
    response.setName(betterName);
    Reflection response2 =
        expectSuccess(
            getBuilder(getPublicAPI(3).path(REFLECTIONS_PATH).path(response.getId()))
                .buildPut(Entity.entity(response, JSON)),
            Reflection.class);

    assertEquals(response2.getId(), response.getId());
    assertEquals(response2.getName(), betterName);
    assertEquals(response2.getType(), response.getType());
    assertEquals(response2.getCreatedAt(), response.getCreatedAt());
    assertTrue(response2.getUpdatedAt() > response.getUpdatedAt());
    assertNotEquals(response2.getTag(), response.getTag());
    assertEquals(response2.getDisplayFields(), response.getDisplayFields());
    assertEquals(response2.getDimensionFields(), response.getDimensionFields());
    assertEquals(response2.getDistributionFields(), response.getDistributionFields());
    assertEquals(response2.getMeasureFields(), response.getMeasureFields());
    assertEquals(response2.getPartitionFields(), response.getPartitionFields());
    assertEquals(response2.getSortFields(), response.getSortFields());
    assertEquals(
        response2.getPartitionDistributionStrategy(), response.getPartitionDistributionStrategy());

    newReflectionServiceHelper().removeReflection(response2.getId());
  }

  @Test
  public void testBoostToggleOnRawReflection() {
    Reflection newReflection = createReflection();

    Reflection response =
        expectSuccess(
            getBuilder(getPublicAPI(3).path(REFLECTIONS_PATH))
                .buildPost(Entity.entity(newReflection, JSON)),
            Reflection.class);
    assertFalse(response.isArrowCachingEnabled());
    response.setArrowCachingEnabled(true);

    response =
        expectSuccess(
            getBuilder(getPublicAPI(3).path(REFLECTIONS_PATH).path(response.getId()))
                .buildPut(Entity.entity(response, JSON)),
            Reflection.class);
    assertTrue(response.isArrowCachingEnabled());
  }

  @Test
  public void testBoostToggleOnAggReflection() {
    Reflection newReflection = createAggReflection();

    Reflection response =
        expectSuccess(
            getBuilder(getPublicAPI(3).path(REFLECTIONS_PATH))
                .buildPost(Entity.entity(newReflection, JSON)),
            Reflection.class);
    assertFalse(response.isArrowCachingEnabled());
    response.setArrowCachingEnabled(true);

    response =
        expectSuccess(
            getBuilder(getPublicAPI(3).path(REFLECTIONS_PATH).path(response.getId()))
                .buildPut(Entity.entity(response, JSON)),
            Reflection.class);
    assertTrue(response.isArrowCachingEnabled());
  }

  @Test
  public void testDeleteReflection() {
    Reflection newReflection = createReflection();
    Reflection response =
        expectSuccess(
            getBuilder(getPublicAPI(3).path(REFLECTIONS_PATH))
                .buildPost(Entity.entity(newReflection, JSON)),
            Reflection.class);

    expectSuccess(
        getBuilder(getPublicAPI(3).path(REFLECTIONS_PATH).path(response.getId())).buildDelete());

    assertFalse(newReflectionServiceHelper().getReflectionById(response.getId()).isPresent());
  }

  @Test
  public void testRetryUnavailableManual() throws Exception {
    try (AutoCloseable option =
        withSystemOption(PARQUET_MAXIMUM_PARTITIONS_VALIDATOR.getOptionName(), "2")) {
      // Create reflection on table that has never refresh policy
      Reflection newReflection = createReflection();
      ReflectionPartitionField partitionField = new ReflectionPartitionField();
      partitionField.setName("full_address");
      newReflection.setPartitionFields(ImmutableList.of(partitionField));
      Reflection createResponse =
          expectSuccess(
              getBuilder(getPublicAPI(3).path(REFLECTIONS_PATH))
                  .buildPost(Entity.entity(newReflection, JSON)),
              Reflection.class);
      assertNotNull(createResponse.getPartitionFields());

      // Verify reflection fails
      ReflectionEntry entry =
          monitor.waitForState(new ReflectionId(createResponse.getId()), ReflectionState.ACTIVE);
      ReflectionStatusUI status =
          newReflectionServiceHelper().getStatusForReflection(createResponse.getId());
      assertEquals(1, entry.getNumFailures().intValue());
      assertEquals(true, entry.getDontGiveUp());
      assertEquals(NONE, status.getAvailability());
      assertEquals(MANUAL, status.getRefresh());

      // Retry the reflection
      Response retryResponse = getBuilder(DEV_OPTIONS_PATH + "retryunavailable").post(null);
      assertEquals(Response.Status.NO_CONTENT.getStatusCode(), retryResponse.getStatus());

      // Verify a new materialization is generated and it fails as well
      boolean expectedException = false;
      try {
        monitor.waitUntilMaterialized(new ReflectionId(createResponse.getId()));
      } catch (RuntimeException e) {
        expectedException = true;
      }
      if (!expectedException) {
        fail("Refresh job should fail");
      }
      ReflectionEntry entry2 =
          monitor.waitForState(new ReflectionId(createResponse.getId()), ReflectionState.ACTIVE);
      ReflectionStatusUI status2 =
          newReflectionServiceHelper().getStatusForReflection(createResponse.getId());
      assertEquals(1, entry2.getNumFailures().intValue());
      assertEquals(true, entry2.getDontGiveUp());
      assertEquals(NONE, status2.getAvailability());
      assertEquals(MANUAL, status2.getRefresh());
      assertEquals(1, status2.getFailureCount());

      newReflectionServiceHelper().removeReflection(createResponse.getId());
    }
  }

  @Test
  public void testRetryUnavailableScheduled() throws Exception {
    try (AutoCloseable option =
            withSystemOption(PARQUET_MAXIMUM_PARTITIONS_VALIDATOR.getOptionName(), "2");
        AutoCloseable option2 =
            withSystemOption(LAYOUT_REFRESH_MAX_ATTEMPTS.getOptionName(), "1")) {

      // Create reflection on table that has a 1 hour default refresh policy
      final List<NameSpaceContainer> entities =
          getNamespaceService()
              .getEntities(
                  Arrays.asList(new NamespaceKey(TEST_SOURCE), EMPLOYEES.toNamespaceKey()));
      final NameSpaceContainer dataset = entities.get(1);
      ReflectionId testReflectionId =
          l(ReflectionService.class)
              .create(
                  new ReflectionGoal()
                      .setType(ReflectionType.RAW)
                      .setDatasetId(dataset.getDataset().getId().getId())
                      .setName("reflection")
                      .setDetails(
                          new ReflectionDetails()
                              .setDisplayFieldList(
                                  Collections.singletonList(new ReflectionField("employee_id")))
                              .setPartitionFieldList(
                                  Collections.singletonList(
                                      new ReflectionPartitionField("employee_id")))));

      // Verify reflection fails
      Materialization m1 = monitor.waitUntilMaterializationFails(testReflectionId);
      ReflectionEntry entry = monitor.waitForState(testReflectionId, ReflectionState.FAILED);
      ReflectionStatusUI status =
          newReflectionServiceHelper().getStatusForReflection(testReflectionId.getId());
      assertEquals(1, entry.getNumFailures().intValue());
      assertEquals(false, entry.getDontGiveUp());
      assertEquals(NONE, status.getAvailability());
      assertEquals(GIVEN_UP, status.getRefresh());

      // Retry the reflection
      Response retryResponse = getBuilder(DEV_OPTIONS_PATH + "retryunavailable").post(null);
      assertEquals(Response.Status.NO_CONTENT.getStatusCode(), retryResponse.getStatus());

      // Verify a new materialization is generated and it fails as well
      Materialization m2 = monitor.waitUntilMaterializationFails(testReflectionId, m1);
      ReflectionEntry entry2 = monitor.waitForState(testReflectionId, ReflectionState.FAILED);
      ReflectionStatusUI status2 =
          newReflectionServiceHelper().getStatusForReflection(testReflectionId.getId());
      assertEquals(1, entry2.getNumFailures().intValue());
      assertEquals(false, entry2.getDontGiveUp());
      assertEquals(NONE, status2.getAvailability());
      assertEquals(GIVEN_UP, status2.getRefresh());
      assertEquals(1, status2.getFailureCount());

      newReflectionServiceHelper().removeReflection(testReflectionId.getId());
    }
  }

  private Reflection createAggReflection() {
    // create an agg reflection
    List<ReflectionDimensionField> dimensionFields = new ArrayList<>();

    DremioTable table =
        newCatalogService()
            .getCatalog(
                MetadataRequestOptions.of(
                    SchemaConfig.newBuilder(CatalogUser.from(SYSTEM_USERNAME)).build()))
            .getTable(datasetId);
    for (int i = 0; i < table.getSchema().getFieldCount(); i++) {
      Field field = table.getSchema().getColumn(i);
      if (field.getType().getTypeID() == ArrowType.ArrowTypeID.Utf8) {
        dimensionFields.add(new ReflectionDimensionField(field.getName()));
      }
    }

    return Reflection.newAggReflection(
        null,
        "My Agg",
        null,
        null,
        null,
        table.getDatasetConfig().getId().getId(),
        null,
        null,
        true,
        false,
        null,
        dimensionFields,
        null,
        null,
        null,
        null,
        PartitionDistributionStrategy.CONSOLIDATED);
  }

  private Reflection createReflection() {
    // create a reflection
    List<ReflectionField> displayFields = new ArrayList<>();

    DremioTable table =
        newCatalogService()
            .getCatalog(
                MetadataRequestOptions.of(
                    SchemaConfig.newBuilder(CatalogUser.from(SYSTEM_USERNAME)).build()))
            .getTable(datasetId);
    for (int i = 0; i < table.getSchema().getFieldCount(); i++) {
      Field field = table.getSchema().getColumn(i);
      if (field.getName().equals(UPDATE_COLUMN)) {
        continue;
      }
      displayFields.add(new ReflectionField(field.getName()));
    }

    return Reflection.newRawReflection(
        null,
        "My Raw",
        null,
        null,
        null,
        table.getDatasetConfig().getId().getId(),
        null,
        null,
        true,
        false,
        null,
        displayFields,
        null,
        null,
        null,
        PartitionDistributionStrategy.CONSOLIDATED);
  }

  private Dataset createDataset() {
    return expectSuccess(
        getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(homeFileId)).buildGet(), Dataset.class);
  }

  private void uploadHomeFile() throws Exception {
    final FormDataMultiPart form = new FormDataMultiPart();
    final FormDataBodyPart fileBody =
        new FormDataBodyPart(
            "file",
            readResourceAsString("/testfiles/yelp_biz.json"),
            MediaType.MULTIPART_FORM_DATA_TYPE);
    form.bodyPart(fileBody);
    form.bodyPart(new FormDataBodyPart("fileName", "biz"));

    com.dremio.file.File file1 =
        expectSuccess(
            getBuilder(
                    getAPIv2()
                        .path("home/" + HOME_NAME + "/upload_start/")
                        .queryParam("extension", "json"))
                .buildPost(Entity.entity(form, form.getMediaType())),
            com.dremio.file.File.class);
    file1 =
        expectSuccess(
            getBuilder(getAPIv2().path("home/" + HOME_NAME + "/upload_finish/biz"))
                .buildPost(Entity.json(file1.getFileFormat().getFileFormat())),
            com.dremio.file.File.class);
    homeFileId = file1.getId();
    FileFormat fileFormat = file1.getFileFormat().getFileFormat();
    assertEquals(physicalFileAtHome.getLeaf().getName(), fileFormat.getName());
    assertEquals(physicalFileAtHome.toPathList(), fileFormat.getFullPath());
    assertEquals(FileType.JSON, fileFormat.getFileType());

    fileBody.cleanup();
  }
}
