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
package com.dremio.dac.resource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.dremio.dac.api.ResponseList;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.model.sources.UIMetadataPolicy;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.FamilyExpectation;
import com.dremio.dac.server.ValidationErrorMessage;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.namespace.physicaldataset.proto.AccelerationSettingsDescriptor;
import com.dremio.service.reflection.analysis.ReflectionSuggester.ReflectionSuggestionType;
import com.dremio.service.reflection.proto.DimensionGranularity;
import com.dremio.service.reflection.proto.MeasureType;
import com.dremio.service.reflection.proto.ReflectionDimensionField;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionMeasureField;
import com.dremio.service.reflection.proto.ReflectionType;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests {@link com.dremio.dac.explore.DatasetResource} API */
public class TestDatasetResource extends BaseTestServer {
  private static final String SOURCE_NAME = "mysrc";
  private static final String DATASET_NAME = "ds1.json";
  private static final String DATASET_NAME_2 = "ds2.json";
  private static final long DEFAULT_REFRESH_PERIOD = TimeUnit.HOURS.toMillis(4);
  private static final long DEFAULT_GRACE_PERIOD = TimeUnit.HOURS.toMillis(12);
  private static final DatasetPath DATASET_PATH =
      new DatasetPath(ImmutableList.of(SOURCE_NAME, DATASET_NAME));
  private static final DatasetPath DATASET_PATH_2 =
      new DatasetPath(ImmutableList.of(SOURCE_NAME, DATASET_NAME_2));

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private void createNewFile(String datasetName) throws Exception {
    final File file2 = folder.newFile(datasetName);
    try (PrintWriter writer = new PrintWriter(file2)) {
      writer.print("{ \"key\" : \"A\", \"value\" : 0 , \"loc\" : [1, 2]}");
    }
  }

  @Before
  public void setup() throws Exception {
    final NASConf nas = new NASConf();
    nas.path = folder.getRoot().getPath();

    final SourceUI source = new SourceUI();
    source.setName(SOURCE_NAME);
    source.setCtime(System.currentTimeMillis());
    source.setAccelerationRefreshPeriod(DEFAULT_REFRESH_PERIOD);
    source.setAccelerationGracePeriod(DEFAULT_GRACE_PERIOD);
    source.setMetadataPolicy(
        UIMetadataPolicy.of(CatalogService.NEVER_REFRESH_POLICY_WITH_AUTO_PROMOTE));
    source.setConfig(nas);
    getSourceService().registerSourceWithRuntime(source);

    createNewFile(DATASET_NAME);
    getHttpClient().getDatasetApi().getPreview(DATASET_PATH);

    final DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setName(DATASET_PATH_2.getLeaf().getName());
    datasetConfig.setFullPathList(DATASET_PATH_2.toPathList());
    datasetConfig.setType(DatasetType.PHYSICAL_DATASET);
    datasetConfig.setPhysicalDataset(new PhysicalDataset());
    getNamespaceService().addOrUpdateDataset(DATASET_PATH_2.toNamespaceKey(), datasetConfig);

    createNewFile(DATASET_NAME_2);
    getHttpClient().getDatasetApi().getPreview(DATASET_PATH_2);
  }

  @After
  public void clear() throws Exception {
    deleteSource(SOURCE_NAME);
  }

  @Test
  public void testAccelerationSettings() throws Exception {
    final String endpoint =
        String.format("/dataset/%s/acceleration/settings", DATASET_PATH.toPathString());
    {
      final AccelerationSettingsDescriptor descriptor =
          expectSuccess(
              getBuilder(getHttpClient().getAPIv2().path(endpoint)).buildGet(),
              AccelerationSettingsDescriptor.class);

      assertEquals((Long) DEFAULT_REFRESH_PERIOD, descriptor.getAccelerationRefreshPeriod());
      assertEquals((Long) DEFAULT_GRACE_PERIOD, descriptor.getAccelerationGracePeriod());
    }
  }

  @Test
  public void testAccelerationSettingsNeverRefreshOrExpire() throws Exception {
    final String endpoint =
        String.format("/dataset/%s/acceleration/settings", DATASET_PATH.toPathString());
    {
      final AccelerationSettingsDescriptor descriptor =
          expectSuccess(
              getBuilder(getHttpClient().getAPIv2().path(endpoint)).buildGet(),
              AccelerationSettingsDescriptor.class);

      assertEquals((Long) DEFAULT_REFRESH_PERIOD, descriptor.getAccelerationRefreshPeriod());
      assertEquals((Long) DEFAULT_GRACE_PERIOD, descriptor.getAccelerationGracePeriod());

      descriptor.setAccelerationNeverExpire(true);
      descriptor.setAccelerationNeverRefresh(false);

      expectSuccess(
          getBuilder(
                  getHttpClient()
                      .getAPIv2()
                      .path(
                          String.format(
                              "/dataset/%s/acceleration/settings", DATASET_PATH.toPathString())))
              .buildPut(Entity.entity(descriptor, JSON)));

      final AccelerationSettingsDescriptor middleDescriptor =
          expectSuccess(
              getBuilder(getHttpClient().getAPIv2().path(endpoint)).buildGet(),
              AccelerationSettingsDescriptor.class);

      assertTrue(middleDescriptor.getAccelerationNeverExpire());
      assertFalse(middleDescriptor.getAccelerationNeverRefresh());

      middleDescriptor.setAccelerationNeverExpire(false);
      middleDescriptor.setAccelerationNeverRefresh(true);

      expectSuccess(
          getBuilder(
                  getHttpClient()
                      .getAPIv2()
                      .path(
                          String.format(
                              "/dataset/%s/acceleration/settings", DATASET_PATH.toPathString())))
              .buildPut(Entity.entity(middleDescriptor, JSON)));

      final AccelerationSettingsDescriptor descriptorMod =
          expectSuccess(
              getBuilder(getHttpClient().getAPIv2().path(endpoint)).buildGet(),
              AccelerationSettingsDescriptor.class);

      assertFalse(descriptorMod.getAccelerationNeverExpire());
      assertTrue(descriptorMod.getAccelerationNeverRefresh());
    }
  }

  @Test
  public void testAccelerationSettingsRefreshLessthanExpire() throws Exception {
    final String endpoint =
        String.format("/dataset/%s/acceleration/settings", DATASET_PATH.toPathString());

    final AccelerationSettingsDescriptor goodDescriptor =
        expectSuccess(
            getBuilder(getHttpClient().getAPIv2().path(endpoint)).buildGet(),
            AccelerationSettingsDescriptor.class);

    goodDescriptor.setAccelerationRefreshPeriod(1L);
    goodDescriptor.setAccelerationGracePeriod(2L);
    goodDescriptor.setAccelerationNeverExpire(false);
    goodDescriptor.setAccelerationNeverRefresh(false);

    expectSuccess(
        getBuilder(
                getHttpClient()
                    .getAPIv2()
                    .path(
                        String.format(
                            "/dataset/%s/acceleration/settings", DATASET_PATH.toPathString())))
            .buildPut(Entity.entity(goodDescriptor, JSON)));

    final AccelerationSettingsDescriptor descriptor =
        new AccelerationSettingsDescriptor()
            .setAccelerationRefreshPeriod(DEFAULT_REFRESH_PERIOD)
            .setAccelerationGracePeriod(DEFAULT_GRACE_PERIOD)
            .setMethod(RefreshMethod.FULL);

    expectSuccess(
        getBuilder(
                getHttpClient()
                    .getAPIv2()
                    .path(
                        String.format(
                            "/dataset/%s/acceleration/settings", DATASET_PATH.toPathString())))
            .buildPut(Entity.entity(descriptor, JSON)));

    final AccelerationSettingsDescriptor badDescriptor =
        expectSuccess(
            getBuilder(getHttpClient().getAPIv2().path(endpoint)).buildGet(),
            AccelerationSettingsDescriptor.class);

    badDescriptor.setAccelerationRefreshPeriod(2L); // this is > than expiration
    badDescriptor.setAccelerationGracePeriod(1L);
    badDescriptor.setAccelerationNeverExpire(false);
    badDescriptor.setAccelerationNeverRefresh(false);

    expectError(
        FamilyExpectation.CLIENT_ERROR,
        getBuilder(
                getHttpClient()
                    .getAPIv2()
                    .path(
                        String.format(
                            "/dataset/%s/acceleration/settings", DATASET_PATH.toPathString())))
            .buildPut(Entity.entity(badDescriptor, JSON)),
        ValidationErrorMessage.class);
  }

  @Test
  public void testUpdateSettingsInFullMode() throws Exception {
    {
      final AccelerationSettingsDescriptor descriptor =
          new AccelerationSettingsDescriptor()
              .setAccelerationRefreshPeriod(DEFAULT_REFRESH_PERIOD)
              .setAccelerationGracePeriod(DEFAULT_GRACE_PERIOD)
              .setMethod(RefreshMethod.FULL);

      expectSuccess(
          getBuilder(
                  getHttpClient()
                      .getAPIv2()
                      .path(
                          String.format(
                              "/dataset/%s/acceleration/settings", DATASET_PATH.toPathString())))
              .buildPut(Entity.entity(descriptor, JSON)));

      final AccelerationSettingsDescriptor newDescriptor =
          expectSuccess(
              getBuilder(
                      getHttpClient()
                          .getAPIv2()
                          .path(
                              String.format(
                                  "/dataset/%s/acceleration/settings",
                                  DATASET_PATH.toPathString())))
                  .buildGet(),
              AccelerationSettingsDescriptor.class);

      assertNotNull(newDescriptor);
      assertEquals(
          descriptor.getAccelerationRefreshPeriod(), newDescriptor.getAccelerationRefreshPeriod());
      assertEquals(
          descriptor.getAccelerationGracePeriod(), newDescriptor.getAccelerationGracePeriod());
      assertEquals(descriptor.getMethod(), newDescriptor.getMethod());
      assertEquals(descriptor.getRefreshField(), newDescriptor.getRefreshField());
    }
  }

  @Test
  public void testUpdateSettingsInIncrementalMode() throws Exception {
    {
      final AccelerationSettingsDescriptor descriptor =
          new AccelerationSettingsDescriptor()
              .setAccelerationRefreshPeriod(DEFAULT_REFRESH_PERIOD)
              .setAccelerationGracePeriod(DEFAULT_GRACE_PERIOD)
              .setMethod(RefreshMethod.INCREMENTAL)
              .setRefreshField("test-field");

      expectSuccess(
          getBuilder(
                  getHttpClient()
                      .getAPIv2()
                      .path(
                          String.format(
                              "/dataset/%s/acceleration/settings", DATASET_PATH_2.toPathString())))
              .buildPut(Entity.entity(descriptor, JSON)));

      final AccelerationSettingsDescriptor newDescriptor =
          expectSuccess(
              getBuilder(
                      getHttpClient()
                          .getAPIv2()
                          .path(
                              String.format(
                                  "/dataset/%s/acceleration/settings",
                                  DATASET_PATH_2.toPathString())))
                  .buildGet(),
              AccelerationSettingsDescriptor.class);

      assertNotNull(newDescriptor);
      assertEquals(
          descriptor.getAccelerationRefreshPeriod(), newDescriptor.getAccelerationRefreshPeriod());
      assertEquals(
          descriptor.getAccelerationGracePeriod(), newDescriptor.getAccelerationGracePeriod());
      assertEquals(descriptor.getMethod(), newDescriptor.getMethod());
      assertEquals(descriptor.getRefreshField(), newDescriptor.getRefreshField());
    }
  }

  @Test
  public void testValidation() throws Exception {
    {
      final AccelerationSettingsDescriptor descriptor =
          new AccelerationSettingsDescriptor()
              .setAccelerationRefreshPeriod(DEFAULT_REFRESH_PERIOD)
              .setAccelerationGracePeriod(DEFAULT_GRACE_PERIOD)
              .setMethod(RefreshMethod.INCREMENTAL);

      expectStatus(
          Response.Status.BAD_REQUEST,
          getBuilder(
                  getHttpClient()
                      .getAPIv2()
                      .path(
                          String.format(
                              "/dataset/%s/acceleration/settings", DATASET_PATH_2.toPathString())))
              .buildPut(Entity.entity(descriptor, JSON)));
    }

    {
      final AccelerationSettingsDescriptor descriptor =
          new AccelerationSettingsDescriptor()
              .setAccelerationRefreshPeriod(DEFAULT_REFRESH_PERIOD)
              .setAccelerationGracePeriod(DEFAULT_GRACE_PERIOD)
              .setMethod(RefreshMethod.FULL)
              .setRefreshField("some-field");

      expectStatus(
          Response.Status.BAD_REQUEST,
          getBuilder(
                  getHttpClient()
                      .getAPIv2()
                      .path(
                          String.format(
                              "/dataset/%s/acceleration/settings", DATASET_PATH.toPathString())))
              .buildPut(Entity.entity(descriptor, JSON)));
    }

    {
      final AccelerationSettingsDescriptor descriptor =
          new AccelerationSettingsDescriptor()
              .setAccelerationRefreshPeriod(DEFAULT_REFRESH_PERIOD)
              .setAccelerationGracePeriod(DEFAULT_GRACE_PERIOD)
              .setMethod(RefreshMethod.INCREMENTAL)
              .setRefreshField("some-field");

      expectStatus(
          Response.Status.BAD_REQUEST,
          getBuilder(
                  getHttpClient()
                      .getAPIv2()
                      .path(
                          String.format(
                              "/dataset/%s/acceleration/settings", DATASET_PATH.toPathString())))
              .buildPut(Entity.entity(descriptor, JSON)));
    }

    {
      expectStatus(
          Response.Status.BAD_REQUEST,
          getBuilder(
                  getHttpClient()
                      .getAPIv2()
                      .path(
                          String.format(
                              "/dataset/%s/moveTo/%s",
                              DATASET_PATH.toPathString(), DATASET_PATH.toPathString())))
              .build("POST"));
    }

    {
      expectStatus(
          Response.Status.BAD_REQUEST,
          getBuilder(
                  getHttpClient()
                      .getAPIv2()
                      .path(String.format("/dataset/%s/rename/", DATASET_PATH.toPathString()))
                      .queryParam("renameTo", DATASET_PATH_2.getLeaf().toString()))
              .build("POST"));
    }
  }

  @Test
  public void testReflectionTypePathParameterValidation() throws Exception {
    String uuid = getNamespaceService().getDataset(DATASET_PATH_2.toNamespaceKey()).getId().getId();
    // test invalid reflection type path parameter
    expectStatus(
        Response.Status.BAD_REQUEST,
        getBuilder(
                getHttpClient()
                    .getAPIv3()
                    .path(
                        String.format(
                            "/dataset/%s/reflection/recommendation/%s", uuid, "invalidType")))
            .buildPost(null),
        ResponseList.class);
  }

  @Test
  public void testReflectionRawRecommendation() throws Exception {
    String uuid = getNamespaceService().getDataset(DATASET_PATH_2.toNamespaceKey()).getId().getId();
    ResponseList responseRecommendedReflections =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getAPIv3()
                        .path(
                            String.format(
                                "/dataset/%s/reflection/recommendation/%s",
                                uuid, ReflectionSuggestionType.RAW)))
                .buildPost(null),
            ResponseList.class);

    assertNotNull(responseRecommendedReflections);
    List<Map<String, Object>> recommendedReflections = responseRecommendedReflections.getData();
    assertEquals(1, recommendedReflections.size());

    verifyRawReflection(
        recommendedReflections.get(0),
        true,
        false,
        ImmutableList.of(
            new ReflectionField("key"), new ReflectionField("value"), new ReflectionField("loc")));
  }

  @Test
  public void testReflectionAggRecommendation() throws Exception {
    String uuid = getNamespaceService().getDataset(DATASET_PATH_2.toNamespaceKey()).getId().getId();
    ResponseList responseRecommendedReflectionsLowercase =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getAPIv3()
                        .path(
                            String.format("/dataset/%s/reflection/recommendation/%s", uuid, "agg")))
                .buildPost(null),
            ResponseList.class);

    assertNotNull(responseRecommendedReflectionsLowercase);
    List<Map<String, Object>> recommendedReflectionsLowercase =
        responseRecommendedReflectionsLowercase.getData();
    assertEquals(1, recommendedReflectionsLowercase.size());

    verifyAggReflection(
        recommendedReflectionsLowercase.get(0),
        true,
        false,
        ImmutableList.of(
            new ReflectionDimensionField("key").setGranularity(DimensionGranularity.DATE)),
        ImmutableList.of(
            new ReflectionMeasureField("value")
                .setMeasureTypeList(ImmutableList.of(MeasureType.COUNT, MeasureType.SUM))));
  }

  @Test
  public void testReflectionAllRecommendation() throws Exception {
    String uuid = getNamespaceService().getDataset(DATASET_PATH_2.toNamespaceKey()).getId().getId();
    ResponseList responseRecommendedReflections =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getAPIv3()
                        .path(
                            String.format(
                                "/dataset/%s/reflection/recommendation/%s",
                                uuid, ReflectionSuggestionType.ALL)))
                .buildPost(null),
            ResponseList.class);

    assertNotNull(responseRecommendedReflections);
    List<Map<String, Object>> recommendedReflections = responseRecommendedReflections.getData();
    assertEquals(2, recommendedReflections.size());

    verifyRawReflection(
        recommendedReflections.get(0),
        true,
        false,
        ImmutableList.of(
            new ReflectionField("key"), new ReflectionField("value"), new ReflectionField("loc")));

    verifyAggReflection(
        recommendedReflections.get(1),
        true,
        false,
        ImmutableList.of(
            new ReflectionDimensionField("key").setGranularity(DimensionGranularity.DATE)),
        ImmutableList.of(
            new ReflectionMeasureField("value")
                .setMeasureTypeList(ImmutableList.of(MeasureType.COUNT, MeasureType.SUM))));
  }

  private void verifyRawReflection(
      Map<String, Object> reflectionToVerify,
      boolean enabled,
      boolean arrowCachingEnabled,
      List<ReflectionField> displayFields) {
    assertEquals(ReflectionType.RAW.toString(), reflectionToVerify.get("type"));
    assertEquals(enabled, reflectionToVerify.get("enabled"));
    assertEquals(arrowCachingEnabled, reflectionToVerify.get("arrowCachingEnabled"));

    List<Map<String, String>> displayFieldsMap =
        (List<Map<String, String>>) reflectionToVerify.get("displayFields");
    assertEquals(displayFields.size(), displayFieldsMap.size());
    for (int i = 0; i < displayFields.size(); i++) {
      assertEquals(displayFields.get(i).getName(), displayFieldsMap.get(i).get("name"));
    }
  }

  private void verifyAggReflection(
      Map<String, Object> reflectionToVerify,
      boolean enabled,
      boolean arrowCachingEnabled,
      List<ReflectionDimensionField> dimensionFields,
      List<ReflectionMeasureField> measureFields) {
    assertEquals(ReflectionType.AGGREGATION.toString(), reflectionToVerify.get("type"));
    assertEquals(enabled, reflectionToVerify.get("enabled"));
    assertEquals(arrowCachingEnabled, reflectionToVerify.get("arrowCachingEnabled"));

    List<Map<String, String>> dimensionFieldsMap =
        (List<Map<String, String>>) reflectionToVerify.get("dimensionFields");
    assertEquals(dimensionFields.size(), dimensionFieldsMap.size());
    for (int i = 0; i < dimensionFields.size(); i++) {
      assertEquals(dimensionFields.get(i).getName(), dimensionFieldsMap.get(i).get("name"));
      assertEquals(
          dimensionFields.get(i).getGranularity().toString(),
          dimensionFieldsMap.get(i).get("granularity"));
    }

    List<Map<String, Object>> measureFieldsMap =
        (List<Map<String, Object>>) reflectionToVerify.get("measureFields");
    assertEquals(measureFields.size(), measureFieldsMap.size());
    for (int i = 0; i < measureFields.size(); i++) {
      assertEquals(measureFields.get(i).getName(), measureFieldsMap.get(i).get("name"));

      List<MeasureType> expectedMeasureTypeList = measureFields.get(i).getMeasureTypeList();
      List<String> measureTypeList = (List<String>) measureFieldsMap.get(i).get("measureTypeList");
      assertEquals(expectedMeasureTypeList.size(), measureTypeList.size());
      for (int j = 0; j < measureFields.size(); j++) {
        assertEquals(expectedMeasureTypeList.get(i).toString(), measureTypeList.get(i));
      }
    }
  }
}
