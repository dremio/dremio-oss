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

import static com.dremio.options.OptionValue.OptionType.SYSTEM;
import static com.dremio.service.reflection.ReflectionOptions.REFLECTION_PERIODIC_WAKEUP_ONLY;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;

import org.apache.arrow.vector.types.pojo.Field;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.server.ContextService;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.options.OptionValue;
import com.dremio.service.accelerator.AccelerationTestUtil;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.reflection.ReflectionMonitor;
import com.dremio.service.reflection.ReflectionService;
import com.dremio.service.reflection.ReflectionStatusService;
import com.dremio.service.reflection.proto.PartitionDistributionStrategy;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.users.SystemUser;

/**
 * Test class for {@code ReflectionResource}
 */
public class TestReflectionResource extends AccelerationTestUtil {
  private static final String REFLECTIONS_PATH = "/reflection/";
  private static final String CATALOG_PATH = "/catalog/";

  private static boolean created = false;

  public static final String HOME_NAME = HomeName.getUserHomePath(SampleDataPopulator.DEFAULT_USER_NAME).getName();
  private final DatasetPath physicalFileAtHome = new DatasetPath(Arrays.asList(HOME_NAME, "biz"));

  private static String homeFileId;
  private static String datasetId;

  @Before
  public void setup() throws Exception {
    if (created) {
      return;
    }
    created = true;

    // We don’t care about the reflection manager taking too long to respond in these tests so we disable wakeup events to avoid timing issues.
    l(ContextService.class).get().getOptionManager().setOption( OptionValue.createBoolean(SYSTEM, REFLECTION_PERIODIC_WAKEUP_ONLY.getOptionName(), true));

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
    // wait until the reflection manager doesn't submit any new query, otherwise we may get an exception trying to
    // close query allocators that are still being used
    ReflectionMonitor monitor = new ReflectionMonitor(
      l(ReflectionService.class),
      l(ReflectionStatusService.class),
      l(MaterializationDescriptorProvider.class),
      new MaterializationStore(p(KVStoreProvider.class)),
      SECONDS.toMillis(1),
      SECONDS.toMillis(10)
    );
    monitor.waitUntilNoMaterializationsAvailable();
  }

  @Test
  public void testCreateReflection() {
    Reflection newReflection = createReflection();

    Reflection response = expectSuccess(getBuilder(getPublicAPI(3).path(REFLECTIONS_PATH)).buildPost(Entity.entity(newReflection, JSON)), Reflection.class);

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
    assertEquals(response.getPartitionDistributionStrategy(), newReflection.getPartitionDistributionStrategy());

    newReflectionServiceHelper().removeReflection(response.getId());
  }

  @Test
  public void testGetReflection() {
    Reflection newReflection = createReflection();
    Reflection response = expectSuccess(getBuilder(getPublicAPI(3).path(REFLECTIONS_PATH)).buildPost(Entity.entity(newReflection, JSON)), Reflection.class);

    Reflection response2 = expectSuccess(getBuilder(getPublicAPI(3).path(REFLECTIONS_PATH).path(response.getId())).buildGet(), Reflection.class);

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
    assertEquals(response2.getPartitionDistributionStrategy(), response.getPartitionDistributionStrategy());
  }

  @Test
  public void testUpdateReflection() {
    Reflection newReflection = createReflection();
    Reflection response = expectSuccess(getBuilder(getPublicAPI(3).path(REFLECTIONS_PATH)).buildPost(Entity.entity(newReflection, JSON)), Reflection.class);

    String betterName = "much better name";
    response.setName(betterName);
    Reflection response2 = expectSuccess(getBuilder(getPublicAPI(3).path(REFLECTIONS_PATH).path(response.getId())).buildPut(Entity.entity(response, JSON)), Reflection.class);

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
    assertEquals(response2.getPartitionDistributionStrategy(), response.getPartitionDistributionStrategy());

    newReflectionServiceHelper().removeReflection(response2.getId());
  }

  @Test
  public void testDeleteReflection() {
    Reflection newReflection = createReflection();
    Reflection response = expectSuccess(getBuilder(getPublicAPI(3).path(REFLECTIONS_PATH)).buildPost(Entity.entity(newReflection, JSON)), Reflection.class);

    expectSuccess(getBuilder(getPublicAPI(3).path(REFLECTIONS_PATH).path(response.getId())).buildDelete());

    assertFalse(newReflectionServiceHelper().getReflectionById(response.getId()).isPresent());
  }

  private Reflection createReflection() {
    // create a reflection
    List<ReflectionField> displayFields = new ArrayList<>();

    DremioTable table = newCatalogService().getCatalog(
        MetadataRequestOptions.of(SchemaConfig.newBuilder(SystemUser.SYSTEM_USERNAME).build()))
        .getTable(datasetId);
    for (int i = 0; i < table.getSchema().getFieldCount(); i++) {
      Field field = table.getSchema().getColumn(i);
      displayFields.add(new ReflectionField(field.getName()));
    }

    return Reflection.newRawReflection(null, "My Raw", null, null, null, table.getDatasetConfig().getId().getId(), null, null, true, null, displayFields, null, null, null, PartitionDistributionStrategy.CONSOLIDATED);
  }

  private Dataset createDataset() {
    return expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(homeFileId)).buildGet(), Dataset.class);
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
    homeFileId = file1.getId();
    FileFormat fileFormat = file1.getFileFormat().getFileFormat();
    assertEquals(physicalFileAtHome.getLeaf().getName(), fileFormat.getName());
    assertEquals(physicalFileAtHome.toPathList(), fileFormat.getFullPath());
    assertEquals(FileType.JSON, fileFormat.getFileType());

    fileBody.cleanup();
  }
}
