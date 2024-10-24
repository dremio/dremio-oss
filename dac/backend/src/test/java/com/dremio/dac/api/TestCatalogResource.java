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

import static java.util.Arrays.asList;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.TestTools;
import com.dremio.common.utils.PathUtils;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.homefiles.HomeFileConf;
import com.dremio.dac.model.common.Field;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.server.BaseTestServerJunit5;
import com.dremio.dac.server.FamilyExpectation;
import com.dremio.dac.server.ValidationErrorMessage;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.dac.util.DatasetsUtil;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.catalog.conf.AWSAuthenticationType;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.NessieAuthType;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.exec.store.dfs.PDFSConf;
import com.dremio.io.file.Path;
import com.dremio.options.OptionValue;
import com.dremio.options.OptionValue.OptionType;
import com.dremio.plugins.dataplane.store.NessiePluginConfig;
import com.dremio.plugins.s3.store.S3FileSystem;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.JobSummaryRequest;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.file.proto.JsonFileConfig;
import com.dremio.service.namespace.file.proto.ParquetFileConfig;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.proto.RefreshPolicyType;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.test.UserExceptionAssert;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import io.findify.s3mock.S3Mock;
import java.io.PrintStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.rules.TemporaryFolder;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Tag;
import org.projectnessie.tools.compatibility.api.NessieBaseUri;
import org.projectnessie.tools.compatibility.internal.OlderNessieServersExtension;
import software.amazon.awssdk.regions.Region;

/** Tests for CatalogResource */
@ExtendWith(OlderNessieServersExtension.class)
public class TestCatalogResource extends BaseTestServerJunit5 {
  private static final String CATALOG_PATH = "/catalog/";
  private static final int SRC_INFORMATION_SCHEMA = 1;
  private static final int SRC_SYS = 2;
  private static final int SRC_EXTERNAL = 3;

  public static final String BUCKET_NAME = "nessie-source-bucket";
  private static S3Mock s3Mock;
  private static int s3Port;
  private static AmazonS3 s3Client;
  public static final String NESSIE_SOURCE_NAME = "nessie-source";

  @NessieBaseUri private static URI nessieUri;

  @TempDir private java.nio.file.Path tempDir;

  @BeforeAll
  public static void setUpClass() throws Exception {
    // setup space
    NamespaceKey key = new NamespaceKey("mySpace");
    SpaceConfig spaceConfig = new SpaceConfig();
    spaceConfig.setName("mySpace");
    getNamespaceService().addOrUpdateSpace(key, spaceConfig);

    // setup S3 for Nessie
    setUpS3Fake();
  }

  @AfterAll
  public static void tearDownClass() throws Exception {
    // delete space
    NamespaceKey key = new NamespaceKey("mySpace");
    SpaceConfig space = getNamespaceService().getSpace(key);
    getNamespaceService().deleteSpace(key, space.getTag());

    // clean up S3.
    if (s3Mock != null) {
      s3Mock.shutdown();
      s3Mock = null;
    }
  }

  @BeforeEach
  public void setUp() {
    s3Client.createBucket(BUCKET_NAME);
    createNessieSource();
  }

  @AfterEach
  public void tearDown() throws Exception {
    // Nessie and S3.
    emptyNessieSource();
    s3Client.deleteBucket(BUCKET_NAME);

    // Namespace.
    ((CatalogServiceImpl) getCatalogService()).deleteSource("catalog-test");
    ((CatalogServiceImpl) getCatalogService()).deleteSource(NESSIE_SOURCE_NAME);
  }

  private void createNessieSource() {
    Catalog catalog = getCatalogService().getSystemUserCatalog();
    SourceConfig sourceConfig =
        new SourceConfig()
            .setConnectionConf(buildNessiePluginConfig(BUCKET_NAME))
            .setName(NESSIE_SOURCE_NAME)
            .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
    catalog.createSource(sourceConfig);
  }

  private void emptyNessieSource() throws Exception {
    try (NessieApiV2 nessieClient =
        NessieClientBuilder.createClientBuilder("HTTP", null)
            .withUri(createNessieURIString())
            .build(NessieApiV2.class)) {
      Branch defaultBranch = nessieClient.getDefaultBranch();
      nessieClient
          .assignReference()
          .reference(defaultBranch)
          .assignTo(
              Branch.of(
                  defaultBranch.getName(),
                  Hashing.sha256().hashString("empty", StandardCharsets.UTF_8).toString()))
          .assign();
      nessieClient.getAllReferences().stream()
          .forEach(
              ref -> {
                try {
                  if (ref instanceof Branch && !ref.getName().equals(defaultBranch.getName())) {
                    nessieClient.deleteReference().asBranch().reference(ref).delete();
                  } else if (ref instanceof Tag) {
                    nessieClient.deleteReference().asTag().reference(ref).delete();
                  }
                } catch (NessieConflictException | NessieNotFoundException e) {
                  throw new RuntimeException(e);
                }
              });
    }
  }

  private static void setUpS3Fake() {
    // Start fake S3 storage server.
    s3Mock = new S3Mock.Builder().withPort(0).withInMemoryBackend().build();
    s3Port = s3Mock.start().localAddress().getPort();

    // Create bucket.
    AwsClientBuilder.EndpointConfiguration endpoint =
        new AwsClientBuilder.EndpointConfiguration(
            String.format("http://localhost:%d", s3Port), Region.US_EAST_1.toString());
    s3Client =
        AmazonS3ClientBuilder.standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(endpoint)
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build();
  }

  private static NessiePluginConfig buildNessiePluginConfig(String bucket) {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.nessieEndpoint = createNessieURIString();
    nessiePluginConfig.nessieAuthType = NessieAuthType.NONE;
    nessiePluginConfig.secure = false;
    nessiePluginConfig.credentialType =
        AWSAuthenticationType.ACCESS_KEY; // Unused, just needs to be set
    nessiePluginConfig.awsAccessKey = "foo"; // Unused, just needs to be set
    nessiePluginConfig.awsAccessSecret = SecretRef.of("bar"); // Unused, just needs to be set
    nessiePluginConfig.awsRootPath = bucket;

    // S3Mock settings
    nessiePluginConfig.propertyList =
        Arrays.asList(
            new Property("fs.s3a.endpoint", "localhost:" + s3Port),
            new Property("fs.s3a.path.style.access", "true"),
            new Property("fs.s3a.connection.ssl.enabled", "false"),
            new Property(S3FileSystem.COMPATIBILITY_MODE, "true"));

    return nessiePluginConfig;
  }

  private static String createNessieURIString() {
    return nessieUri.resolve("api/v2").toString();
  }

  @Test
  public void testListTopLevelCatalog() throws Exception {
    // home space always exists
    int topLevelCount =
        getSourceService().getSources().size() + getNamespaceService().getSpaces().size() + 1;

    ResponseList<CatalogItem> items = getRootEntities(null);

    assertEquals(items.getData().size(), topLevelCount);

    int homeCount = 0;
    int spaceCount = 0;
    int sourceCount = 0;

    for (CatalogItem item : items.getData()) {
      if (item.getType() == CatalogItem.CatalogItemType.CONTAINER) {
        if (item.getContainerType() == CatalogItem.ContainerSubType.HOME) {
          homeCount++;
        }

        if (item.getContainerType() == CatalogItem.ContainerSubType.SPACE) {
          spaceCount++;
        }

        if (item.getContainerType() == CatalogItem.ContainerSubType.SOURCE) {
          sourceCount++;
        }
      }
    }

    assertEquals(homeCount, 1);
    assertEquals(spaceCount, getNamespaceService().getSpaces().size());
    assertEquals(sourceCount, getSourceService().getSources().size());
  }

  @Test
  public void testSpace() throws Exception {
    // create a new space
    Space newSpace = new Space(null, "final frontier", null, null, null);

    Space space =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi()).buildPost(Entity.json(newSpace)),
            new GenericType<Space>() {});
    SpaceConfig spaceConfig = getNamespaceService().getSpace(new NamespaceKey(newSpace.getName()));

    assertEquals(space.getId(), spaceConfig.getId().getId());
    assertEquals(space.getName(), spaceConfig.getName());
    assertNotEquals(newSpace.getCreatedAt(), spaceConfig.getCtime());

    // make sure that trying to create the space again fails
    expectStatus(
        Response.Status.CONFLICT,
        getBuilder(getHttpClient().getCatalogApi()).buildPost(Entity.json(newSpace)));

    Space newSpace1 = new Space(space.getId(), space.getName(), space.getTag(), null, null);
    expectSuccess(
        getBuilder(getHttpClient().getCatalogApi().path(space.getId()))
            .buildPut(Entity.json(newSpace1)));
    Space space1 =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(space.getId())).buildGet(),
            new GenericType<Space>() {});
    assertEquals(spaceConfig.getCtime(), space1.getCreatedAt());

    // delete the space
    expectSuccess(
        getBuilder(getHttpClient().getCatalogApi().path(spaceConfig.getId().getId()))
            .buildDelete());
    assertThatThrownBy(
            () -> getNamespaceService().getSpace(new NamespaceKey(spaceConfig.getName())))
        .isInstanceOf(NamespaceException.class);
  }

  @Test
  public void testFoldersInSpace() throws Exception {
    for (boolean deleteFolderFirst : new boolean[] {false, true}) {
      // create a new space
      Space newSpace = new Space(null, "final frontier", null, null, null);
      Space space =
          expectSuccess(
              getBuilder(getHttpClient().getCatalogApi()).buildPost(Entity.json(newSpace)),
              new GenericType<Space>() {});

      // no children at this point
      assertEquals(space.getChildren().size(), 0);

      // add a folder
      Folder newFolder = getFolderConfig(Arrays.asList(space.getName(), "myFolder"));
      Folder folder = createFolder(newFolder);
      assertEquals(newFolder.getPath(), folder.getPath());

      // make sure folder shows up under space
      space =
          expectSuccess(
              getBuilder(getHttpClient().getCatalogApi().path(space.getId())).buildGet(),
              new GenericType<Space>() {});

      // make sure that trying to create the folder again fails
      expectStatus(
          Response.Status.CONFLICT,
          getBuilder(getHttpClient().getCatalogApi()).buildPost(Entity.json(newFolder)));

      // one child at this point
      assertEquals(space.getChildren().size(), 1);
      assertEquals(space.getChildren().get(0).getId(), folder.getId());

      if (deleteFolderFirst) {
        // delete the folder
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(folder.getId())).buildDelete());
        space =
            expectSuccess(
                getBuilder(getHttpClient().getCatalogApi().path(space.getId())).buildGet(),
                new GenericType<Space>() {});
        assertEquals(space.getChildren().size(), 0);

        getNamespaceService().deleteSpace(new NamespaceKey(space.getName()), space.getTag());
      } else {
        getNamespaceService().deleteSpace(new NamespaceKey(space.getName()), space.getTag());

        // delete the folder
        expectError(
            FamilyExpectation.CLIENT_ERROR,
            getBuilder(getHttpClient().getCatalogApi().path(folder.getId())).buildDelete(),
            ValidationErrorMessage.class);
      }
    }
  }

  @Test
  public void testFunctionsInSpace() throws Exception {
    for (boolean deleteFunctionFirst : new boolean[] {false, true}) {
      // create a new space
      Space newSpace = new Space(null, "mySpace123", null, null, null);
      Space space =
          expectSuccess(
              getBuilder(getHttpClient().getCatalogApi()).buildPost(Entity.json(newSpace)),
              new GenericType<Space>() {});

      // no children at this point
      assertEquals(space.getChildren().size(), 0);

      // add a function
      runQuery("CREATE FUNCTION mySpace123.foo()\n" + "RETURNS int\n" + "RETURN 6");

      // make sure function shows up under space
      space =
          expectSuccess(
              getBuilder(getHttpClient().getCatalogApi().path(space.getId())).buildGet(),
              new GenericType<Space>() {});
      assertEquals(space.getChildren().size(), 1);

      if (deleteFunctionFirst) {
        runQuery("DROP FUNCTION mySpace123.foo");

        space =
            expectSuccess(
                getBuilder(getHttpClient().getCatalogApi().path(space.getId())).buildGet(),
                new GenericType<Space>() {});
        assertEquals(space.getChildren().size(), 0);

        getNamespaceService().deleteSpace(new NamespaceKey(space.getName()), space.getTag());
      } else {
        getNamespaceService().deleteSpace(new NamespaceKey(space.getName()), space.getTag());
        try {
          runQuery("DROP FUNCTION mySpace123.foo");
          Assertions.fail(
              "Should not be able to drop a function when we already dropped the space.");
        } catch (UserException ue) {
          // We expect a user exception, since we deleted the space.
        }
      }
    }
  }

  @Test
  public void testDatasetCount() throws Exception {
    Space space = createSpace("dataset count test");

    createVDS(Arrays.asList(space.getName(), "vds1"), "select * from sys.version");
    createVDS(Arrays.asList(space.getName(), "vds2"), "select * from sys.version");

    ResponseList<CatalogItem> items = getRootEntities(null);

    for (CatalogItem item : items.getData()) {
      assertNull(
          item.getStats(),
          "CatalogItemStats should be empty if datasetCount parameter is not provided");
    }

    checkSpaceDatasetCount(space.getId(), 2);

    Folder folder = createFolder(Arrays.asList(space.getName(), "test folder"));
    List<String> vdsPath = new ArrayList<>(folder.getPath());
    vdsPath.add("vds1");

    createVDS(vdsPath, "select * from sys.version");

    checkSpaceDatasetCount(space.getId(), 3);

    getNamespaceService().deleteSpace(new NamespaceKey(space.getName()), space.getTag());
  }

  private void checkSpaceDatasetCount(String spaceId, int expectedDatasetCount) {
    ResponseList<CatalogItem> items =
        getRootEntities(Arrays.asList(CatalogServiceHelper.DetailType.datasetCount));

    Optional<CatalogItem> space =
        items.getData().stream().filter(item -> item.getId().equals(spaceId)).findFirst();

    assertTrue(space.isPresent(), "created space must be returned");
    CatalogItemStats stats = space.get().getStats();
    assertNotNull(stats);
    assertEquals(expectedDatasetCount, stats.getDatasetCount());
  }

  private ResponseList<CatalogItem> getRootEntities(
      final List<CatalogServiceHelper.DetailType> detailsToInclude) {
    WebTarget api = getHttpClient().getCatalogApi();

    if (detailsToInclude != null) {
      for (CatalogServiceHelper.DetailType detail : detailsToInclude) {
        api = api.queryParam("include", detail.name());
      }
    }

    return expectSuccess(
        getBuilder(api).buildGet(), new GenericType<ResponseList<CatalogItem>>() {});
  }

  private Space createSpace(final String spaceName) {
    Space newSpace = new Space(null, spaceName, null, null, null);
    return expectSuccess(
        getBuilder(getHttpClient().getCatalogApi()).buildPost(Entity.json(newSpace)),
        new GenericType<Space>() {});
  }

  @Test
  public void testVDSInSpace() throws Exception {
    // create a new space
    Space space = createSpace("final frontier");

    // add a folder
    Folder newFolder = new Folder(null, Arrays.asList(space.getName(), "myFolder"), null, null);
    Folder folder =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi()).buildPost(Entity.json(newFolder)),
            new GenericType<Folder>() {});

    // create a VDS in the space
    Dataset newVDS =
        getVDSConfig(
            Arrays.asList(space.getName(), "myFolder", "myVDS"), "select * from sys.version");
    Dataset vds =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi()).buildPost(Entity.json(newVDS)),
            new GenericType<Dataset>() {});

    // make sure that trying to create the vds again fails
    expectStatus(
        Response.Status.CONFLICT,
        getBuilder(getHttpClient().getCatalogApi()).buildPost(Entity.json(newVDS)));

    // folder should now have children
    folder =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(folder.getId())).buildGet(),
            new GenericType<Folder>() {});
    assertEquals(folder.getChildren().size(), 1);
    assertEquals(folder.getChildren().get(0).getId(), vds.getId());

    // test rename of a vds
    Dataset renamedVDS =
        new Dataset(
            vds.getId(),
            vds.getType(),
            Arrays.asList(space.getName(), "myFolder", "myVDSRenamed"),
            null,
            null,
            vds.getTag(),
            null,
            vds.getSql(),
            null,
            null,
            null);
    vds =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(renamedVDS.getId()))
                .buildPut(Entity.json(renamedVDS)),
            new GenericType<Dataset>() {});

    folder =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(folder.getId())).buildGet(),
            new GenericType<Folder>() {});
    assertEquals(1, folder.getChildren().size());
    assertEquals(vds.getId(), folder.getChildren().get(0).getId());
    assertEquals("myVDSRenamed", folder.getChildren().get(0).getPath().get(2));

    // test changing sql
    Dataset modifiedVDS =
        new Dataset(
            vds.getId(),
            vds.getType(),
            vds.getPath(),
            null,
            null,
            vds.getTag(),
            null,
            "select version from sys.version",
            null,
            null,
            null);
    vds =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(modifiedVDS.getId()))
                .buildPut(Entity.json(modifiedVDS)),
            new GenericType<Dataset>() {});
    assertEquals(modifiedVDS.getSql(), vds.getSql());
    assertNotEquals(modifiedVDS.getTag(), vds.getTag()); // make sure it stores a new version

    // we currently doesn't allow deserializing of fields so manually check them
    List<Field> fieldsFromDatasetConfig =
        DatasetsUtil.getFieldsFromDatasetConfig(
            getNamespaceService().getDatasetById(new EntityId(vds.getId())).get());
    assertEquals(1, fieldsFromDatasetConfig.size());
    assertEquals("version", fieldsFromDatasetConfig.get(0).getName());

    // delete the vds
    expectSuccess(getBuilder(getHttpClient().getCatalogApi().path(vds.getId())).buildDelete());
    folder =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(folder.getId())).buildGet(),
            new GenericType<Folder>() {});
    assertEquals(0, folder.getChildren().size());

    getNamespaceService().deleteSpace(new NamespaceKey(space.getName()), space.getTag());
  }

  @Test
  public void testVDSInSpaceWithSameName() throws Exception {
    final String sourceName = "src_" + System.currentTimeMillis();

    SourceUI source = new SourceUI();
    source.setName(sourceName);
    source.setCtime(1000L);

    TemporaryFolder folder = new TemporaryFolder();
    folder.create();

    final NASConf config = new NASConf();
    config.path = folder.getRoot().getAbsolutePath();
    source.setConfig(config);

    java.io.File srcFolder = folder.getRoot();

    PrintStream file =
        new PrintStream(new java.io.File(srcFolder.getAbsolutePath(), "myFile.json"));
    for (int i = 0; i < 10; i++) {
      file.println("{a:{b:[1,2]}}");
    }
    file.close();

    getSourceService().registerSourceWithRuntime(source);

    final DatasetPath path1 = new DatasetPath(ImmutableList.of(sourceName, "myFile.json"));
    final DatasetConfig dataset1 =
        new DatasetConfig()
            .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER)
            .setFullPathList(path1.toPathList())
            .setName(path1.getLeaf().getName())
            .setCreatedAt(System.currentTimeMillis())
            .setTag(null)
            .setOwner(DEFAULT_USERNAME)
            .setPhysicalDataset(
                new PhysicalDataset().setFormatSettings(new FileConfig().setType(FileType.JSON)));
    l(NamespaceService.class).addOrUpdateDataset(path1.toNamespaceKey(), dataset1);

    DatasetPath vdsPath = new DatasetPath(ImmutableList.of("@dremio", "myFile.json"));
    getHttpClient()
        .getDatasetApi()
        .createDatasetFromSQLAndSave(vdsPath, "SELECT * FROM \"myFile.json\"", asList(sourceName));

    final String query = "select * from \"myFile.json\"";
    submitJobAndWaitUntilCompletion(
        JobRequest.newBuilder()
            .setSqlQuery(new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
            .setQueryType(QueryType.UI_INTERNAL_RUN)
            .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
            .build());
  }

  @Test
  public void testCrossSourceSelectVDSDefault() throws Exception {

    final String sourceName1 = "src_" + System.currentTimeMillis();
    final String vdsName1 = sourceName1 + "VDS";
    Source newSource1 = createDatasetFromSource(sourceName1, "myFile.json", SRC_EXTERNAL, vdsName1);
    final String sourceName2 = "src_" + System.currentTimeMillis();
    final String vdsName2 = sourceName2 + "VDS";
    Source newSource2 =
        createDatasetFromSource(sourceName2, "myFile2.json", SRC_EXTERNAL, vdsName2);

    getOptionManager()
        .setOption(
            OptionValue.createBoolean(
                OptionType.SYSTEM, "planner.cross_source_select.disable", false));

    final String query =
        String.format(
            "select * from \"@dremio\".\"%s\" as d join \"@dremio\".\"%s\" as e on d.name = e.name ",
            vdsName1, vdsName2);

    submitJobAndWaitUntilCompletion(
        JobRequest.newBuilder()
            .setSqlQuery(new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
            .setQueryType(QueryType.UI_INTERNAL_RUN)
            .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
            .build());
  }

  @Test
  public void testCrossSourceSelectVDSOptionEnabled() throws Exception {

    final String sourceName1 = "src_" + System.currentTimeMillis();
    final String vdsName1 = sourceName1 + "VDS";
    Source newSource1 = createDatasetFromSource(sourceName1, "myFile.json", SRC_EXTERNAL, vdsName1);
    final String sourceName2 = "src_" + System.currentTimeMillis();
    final String vdsName2 = sourceName2 + "VDS";
    Source newSource2 =
        createDatasetFromSource(sourceName2, "myFile2.json", SRC_EXTERNAL, vdsName2);

    getOptionManager()
        .setOption(
            OptionValue.createBoolean(
                OptionType.SYSTEM, "planner.cross_source_select.disable", true));

    final String query =
        String.format(
            "select * from \"@dremio\".\"%s\" as d join \"@dremio\".\"%s\" as e on d.name = e.name ",
            vdsName1, vdsName2);

    final String msg =
        String.format(
            "Cross select is disabled between sources '%s', '%s'.", sourceName1, sourceName2);

    UserExceptionAssert.assertThatThrownBy(
            () ->
                submitJobAndWaitUntilCompletion(
                    JobRequest.newBuilder()
                        .setSqlQuery(
                            new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
                        .setQueryType(QueryType.UI_INTERNAL_RUN)
                        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
                        .build()))
        .hasErrorType(UserBitShared.DremioPBError.ErrorType.VALIDATION)
        .hasMessageContaining(msg);
  }

  @Test
  public void testCrossSourceSelectVDSAllowSource() throws Exception {

    final String sourceName1 = "src_" + System.currentTimeMillis();
    final String vdsName1 = sourceName1 + "VDS";
    Source newSource1 = createDatasetFromSource(sourceName1, "myFile.json", SRC_EXTERNAL, vdsName1);
    final String sourceName2 = "src_" + System.currentTimeMillis();
    final String vdsName2 = sourceName2 + "VDS";
    Source newSource2 =
        createDatasetFromSource(sourceName2, "myFile2.json", SRC_EXTERNAL, vdsName2);

    newSource2.setAllowCrossSourceSelection(true);
    newSource2 =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(newSource2.getId()))
                .buildPut(Entity.json(newSource2)),
            new GenericType<Source>() {});

    getOptionManager()
        .setOption(
            OptionValue.createBoolean(
                OptionType.SYSTEM, "planner.cross_source_select.disable", true));

    final String query =
        String.format(
            "select * from \"@dremio\".\"%s\" as d join \"@dremio\".\"%s\" as e on d.name = e.name ",
            vdsName1, vdsName2);

    submitJobAndWaitUntilCompletion(
        JobRequest.newBuilder()
            .setSqlQuery(new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
            .setQueryType(QueryType.UI_INTERNAL_RUN)
            .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
            .build());
  }

  @Test
  public void testCrossSourceSelectMixVDS() throws Exception {

    final String sourceName1 = "src_" + System.currentTimeMillis();
    final String vdsName1 = sourceName1 + "VDS";
    Source newSource1 = createDatasetFromSource(sourceName1, "myFile.json", SRC_EXTERNAL, vdsName1);
    final String sourceName2 = "src_" + System.currentTimeMillis();
    final String vdsName2 = sourceName2 + "VDS";
    Source newSource2 =
        createDatasetFromSource(sourceName2, "myFile2.json", SRC_EXTERNAL, vdsName2);
    final String sourceName3 = "src_" + System.currentTimeMillis();
    final String vdsName3 = sourceName3 + "VDS";
    Source newSource3 =
        createDatasetFromSource(sourceName3, "myFile3.json", SRC_EXTERNAL, vdsName3);

    newSource2.setAllowCrossSourceSelection(true);
    newSource2 =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(newSource2.getId()))
                .buildPut(Entity.json(newSource2)),
            new GenericType<Source>() {});

    getOptionManager()
        .setOption(
            OptionValue.createBoolean(
                OptionType.SYSTEM, "planner.cross_source_select.disable", true));

    final String query =
        String.format(
            "select * from \"@dremio\".\"%s\" as d join \"@dremio\".\"%s\" as e on d.name = e.name ",
            vdsName1, vdsName2);
    final String query2 =
        query + String.format("join \"@dremio\".\"%s\" as f on d.name = f.name", vdsName3);

    final String msg =
        String.format(
            "Cross select is disabled between sources '%s', '%s'.", sourceName1, sourceName3);

    UserExceptionAssert.assertThatThrownBy(
            () ->
                submitJobAndWaitUntilCompletion(
                    JobRequest.newBuilder()
                        .setSqlQuery(
                            new SqlQuery(query2, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
                        .setQueryType(QueryType.UI_INTERNAL_RUN)
                        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
                        .build()))
        .hasErrorType(UserBitShared.DremioPBError.ErrorType.VALIDATION)
        .hasMessageContaining(msg);
  }

  @Test
  public void testCrossSourceSelectDefault() throws Exception {

    final String sourceName1 = "src_" + System.currentTimeMillis();
    Source newSource1 = createDatasetFromSource(sourceName1, "myFile.json", SRC_EXTERNAL, null);
    final String sourceName2 = "src_" + System.currentTimeMillis();
    Source newSource2 = createDatasetFromSource(sourceName2, "myFile2.json", SRC_EXTERNAL, null);

    getOptionManager()
        .setOption(
            OptionValue.createBoolean(
                OptionType.SYSTEM, "planner.cross_source_select.disable", false));

    final String query =
        String.format(
            "select * from %s.\"myFile.json\" as d join %s.\"myFile2.json\" as e on d.name = e.name",
            sourceName1, sourceName2);

    submitJobAndWaitUntilCompletion(
        JobRequest.newBuilder()
            .setSqlQuery(new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
            .setQueryType(QueryType.UI_INTERNAL_RUN)
            .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
            .build());
  }

  @Test
  public void testCrossSourceSelectOptionEnabled() throws Exception {

    final String sourceName1 = "src_" + System.currentTimeMillis();
    Source newSource1 = createDatasetFromSource(sourceName1, "myFile.json", SRC_EXTERNAL, null);
    final String sourceName2 = "src_" + System.currentTimeMillis();
    Source newSource2 = createDatasetFromSource(sourceName2, "myFile2.json", SRC_EXTERNAL, null);

    getOptionManager()
        .setOption(
            OptionValue.createBoolean(
                OptionType.SYSTEM, "planner.cross_source_select.disable", true));

    final String query =
        String.format(
            "select * from %s.\"myFile.json\" as d join %s.\"myFile2.json\" as e on d.name = e.name",
            sourceName1, sourceName2);

    final String msg =
        String.format(
            "Cross select is disabled between sources '%s', '%s'.", sourceName1, sourceName2);

    UserExceptionAssert.assertThatThrownBy(
            () ->
                submitJobAndWaitUntilCompletion(
                    JobRequest.newBuilder()
                        .setSqlQuery(
                            new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
                        .setQueryType(QueryType.UI_INTERNAL_RUN)
                        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
                        .build()))
        .hasErrorType(UserBitShared.DremioPBError.ErrorType.VALIDATION)
        .hasMessageContaining(msg);
  }

  @Test
  public void testCrossSourceSelectAllowSource() throws Exception {

    final String sourceName1 = "src_" + System.currentTimeMillis();
    Source newSource1 = createDatasetFromSource(sourceName1, "myFile.json", SRC_EXTERNAL, null);
    final String sourceName2 = "src_" + System.currentTimeMillis();
    Source newSource2 = createDatasetFromSource(sourceName2, "myFile2.json", SRC_EXTERNAL, null);

    newSource2.setAllowCrossSourceSelection(true);
    newSource2 =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(newSource2.getId()))
                .buildPut(Entity.json(newSource2)),
            new GenericType<Source>() {});

    getOptionManager()
        .setOption(
            OptionValue.createBoolean(
                OptionType.SYSTEM, "planner.cross_source_select.disable", true));

    final String query =
        String.format(
            "select * from %s.\"myFile.json\" as d join %s.\"myFile2.json\" as e on d.name = e.name",
            sourceName1, sourceName2);

    submitJobAndWaitUntilCompletion(
        JobRequest.newBuilder()
            .setSqlQuery(new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
            .setQueryType(QueryType.UI_INTERNAL_RUN)
            .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
            .build());
  }

  @Test
  public void testCrossSourceSelectMixed() throws Exception {

    final String sourceName1 = "src_" + System.currentTimeMillis();
    Source newSource1 = createDatasetFromSource(sourceName1, "myFile.json", SRC_EXTERNAL, null);
    final String sourceName2 = "src_" + System.currentTimeMillis();
    Source newSource2 = createDatasetFromSource(sourceName2, "myFile2.json", SRC_EXTERNAL, null);
    final String sourceName3 = "src_" + System.currentTimeMillis();
    Source newSource3 = createDatasetFromSource(sourceName3, "myFile3.json", SRC_EXTERNAL, null);

    newSource2.setAllowCrossSourceSelection(true);
    newSource2 =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(newSource2.getId()))
                .buildPut(Entity.json(newSource2)),
            new GenericType<Source>() {});

    getOptionManager()
        .setOption(
            OptionValue.createBoolean(
                OptionType.SYSTEM, "planner.cross_source_select.disable", true));

    final String query =
        String.format(
            "select * from %s.\"myFile.json\" as d join %s.\"myFile2.json\" as e on d.name = e.name ",
            sourceName1, sourceName2);
    final String query2 =
        query + String.format("join %s.\"myFile3.json\" as f on d.name = f.name", sourceName3);

    final String msg =
        String.format(
            "Cross select is disabled between sources '%s', '%s'.", sourceName1, sourceName3);

    UserExceptionAssert.assertThatThrownBy(
            () ->
                submitJobAndWaitUntilCompletion(
                    JobRequest.newBuilder()
                        .setSqlQuery(
                            new SqlQuery(query2, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
                        .setQueryType(QueryType.UI_INTERNAL_RUN)
                        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
                        .build()))
        .hasErrorType(UserBitShared.DremioPBError.ErrorType.VALIDATION)
        .hasMessageContaining(msg);
  }

  @Test
  public void testCrossSourceSelectInformationSchema() throws Exception {

    final String sourceName = "src_" + System.currentTimeMillis();
    final Source newSource =
        createDatasetFromSource(sourceName, "myFile.json", SRC_INFORMATION_SCHEMA, null);
    getOptionManager()
        .setOption(
            OptionValue.createBoolean(
                OptionType.SYSTEM, "planner.cross_source_select.disable", true));

    final String query =
        String.format(
            "select * from %s.\"myFile.json\" as d join INFORMATION_SCHEMA.catalogs as e on d.catalog = e.catalog_name",
            sourceName);

    final JobId jobId =
        submitJobAndWaitUntilCompletion(
            JobRequest.newBuilder()
                .setSqlQuery(new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
                .setQueryType(QueryType.UI_INTERNAL_RUN)
                .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
                .build());

    final JobSummary job =
        l(JobsService.class)
            .getJobSummary(
                JobSummaryRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId)).build());
    assertSame(JobsProtoUtil.toStuff(job.getJobState()), JobState.COMPLETED);
  }

  @Test
  public void testCrossSourceSelectSys() throws Exception {

    final String sourceName = "src_" + System.currentTimeMillis();
    final Source newSource = createDatasetFromSource(sourceName, "myFile.json", SRC_SYS, null);
    getOptionManager()
        .setOption(
            OptionValue.createBoolean(
                OptionType.SYSTEM, "planner.cross_source_select.disable", true));

    final String query =
        String.format(
            "select * from %s.\"myFile.json\" as d join sys.options as e on d.type = e.type",
            sourceName);

    final JobId jobId =
        submitJobAndWaitUntilCompletion(
            JobRequest.newBuilder()
                .setSqlQuery(new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
                .setQueryType(QueryType.UI_INTERNAL_RUN)
                .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
                .build());
    final JobSummary job =
        l(JobsService.class)
            .getJobSummary(
                JobSummaryRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId)).build());
    assertSame(JobsProtoUtil.toStuff(job.getJobState()), JobState.COMPLETED);
  }

  private Source createDatasetFromSource(
      String sourceName, String fileName, int sourceType, String vdsName) throws Exception {
    TemporaryFolder folder = new TemporaryFolder();
    folder.create();

    final NASConf config = new NASConf();
    config.path = folder.getRoot().getAbsolutePath();

    java.io.File srcFolder = folder.getRoot();
    try (PrintStream file =
        new PrintStream(new java.io.File(srcFolder.getAbsolutePath(), fileName))) {
      if (sourceType == SRC_INFORMATION_SCHEMA) {
        file.println(
            "{\"catalog\":\"DREMIO\",\"name\":\"fred ovid\",\"age\":76,\"gpa\":1.55,\"studentnum\":692315658449}");
      } else if (sourceType == SRC_SYS) {
        file.println(
            "{\"type\":\"SYSTEM\",\"name\":\"fred ovid\",\"age\":76,\"gpa\":1.55,\"studentnum\":692315658449}");
      } else {
        file.println(
            "{\"rownum\":1,\"name\":\"fred ovid\",\"age\":76,\"gpa\":1.55,\"studentnum\":692315658449,\"create_time\":\"2014-05-27 00:26:07\", \"interests\": [ \"Reading\", \"Mountain Biking\", \"Hacking\" ], \"favorites\": {\"color\": \"Blue\", \"sport\": \"Soccer\", \"food\": \"Spaghetti\"}}");
      }
    }

    Source newSource = new Source();
    newSource.setName(sourceName);
    newSource.setType("NAS");
    newSource.setConfig(config);
    newSource.setCreatedAt(1000L);
    newSource.setAllowCrossSourceSelection(false);

    Source source =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi()).buildPost(Entity.json(newSource)),
            new GenericType<Source>() {});
    newSource =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(source.getId())).buildGet(),
            new GenericType<Source>() {});

    final DatasetPath path = new DatasetPath(ImmutableList.of(sourceName, fileName));
    final DatasetConfig dataset =
        new DatasetConfig()
            .setId(new EntityId().setId(UUID.randomUUID().toString()))
            .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER)
            .setFullPathList(path.toPathList())
            .setName(path.getLeaf().getName())
            .setCreatedAt(System.currentTimeMillis())
            .setTag(null)
            .setOwner(DEFAULT_USERNAME)
            .setPhysicalDataset(
                new PhysicalDataset().setFormatSettings(new FileConfig().setType(FileType.JSON)));
    l(NamespaceService.class).addOrUpdateDataset(path.toNamespaceKey(), dataset);

    if (vdsName != null) {
      DatasetPath vdsPath = new DatasetPath(ImmutableList.of("@dremio", vdsName));
      final String query = String.format("SELECT * FROM %s.\"%s\"", sourceName, fileName);
      getHttpClient()
          .getDatasetApi()
          .createDatasetFromSQLAndSave(vdsPath, query, asList(sourceName));
    }

    return newSource;
  }

  @Test
  public void testMetadataTooLarge() throws Exception {

    final String sourceName1 = "src_" + System.currentTimeMillis();

    TemporaryFolder folder = new TemporaryFolder();
    folder.create();

    final NASConf config = new NASConf();
    config.path = folder.getRoot().getAbsolutePath();

    java.io.File srcFolder = folder.getRoot();
    try (PrintStream file =
        new PrintStream(new java.io.File(srcFolder.getAbsolutePath(), "zmyFile1.json"))) {
      file.println(
          "{\"rownum\":1,\"name\":\"fred ovid\",\"age\":76,\"gpa\":1.55,\"studentnum\":692315658449,\"create_time\":\"2014-05-27 00:26:07\", \"interests\": [ \"Reading\", \"Mountain Biking\", \"Hacking\" ], \"favorites\": {\"color\": \"Blue\", \"sport\": \"Soccer\", \"food\": \"Spaghetti\"}}");
    }

    String nestedDir = srcFolder + java.io.File.separator + "nestedDir";
    java.io.File dir = new java.io.File(nestedDir);
    dir.mkdirs();

    try (PrintStream file =
        new PrintStream(new java.io.File(dir.getAbsolutePath(), "nestedFile1.json"))) {
      file.println(
          "{\"rownum\":1,\"name\":\"fred ovid\",\"age\":76,\"gpa\":1.55,\"studentnum\":692315658449,\"create_time\":\"2014-05-27 00:26:07\", \"interests\": [ \"Reading\", \"Mountain Biking\", \"Hacking\" ], \"favorites\": {\"color\": \"Blue\", \"sport\": \"Soccer\", \"food\": \"Spaghetti\"}}");
    }
    try (PrintStream file =
        new PrintStream(new java.io.File(dir.getAbsolutePath(), "nestedFile2.json"))) {
      file.println(
          "{\"rownum\":1,\"name\":\"fred ovid\",\"age\":76,\"gpa\":1.55,\"studentnum\":692315658449,\"create_time\":\"2014-05-27 00:26:07\", \"interests\": [ \"Reading\", \"Mountain Biking\", \"Hacking\" ], \"favorites\": {\"color\": \"Blue\", \"sport\": \"Soccer\", \"food\": \"Spaghetti\"}}");
    }

    Source newSource = new Source();
    newSource.setName(sourceName1);
    newSource.setType("NAS");
    newSource.setConfig(config);
    newSource.setCreatedAt(1000L);

    Source source =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi()).buildPost(Entity.json(newSource)),
            new GenericType<Source>() {});
    newSource =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(source.getId())).buildGet(),
            new GenericType<Source>() {});

    DatasetPath path = new DatasetPath(ImmutableList.of(sourceName1, "nestedDir"));
    DatasetConfig dataset =
        new DatasetConfig()
            .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER)
            .setFullPathList(path.toPathList())
            .setName(path.getLeaf().getName())
            .setCreatedAt(System.currentTimeMillis())
            .setTag(null)
            .setOwner(DEFAULT_USERNAME)
            .setPhysicalDataset(
                new PhysicalDataset().setFormatSettings(new FileConfig().setType(FileType.JSON)));
    l(NamespaceService.class).addOrUpdateDataset(path.toNamespaceKey(), dataset);

    path = new DatasetPath(ImmutableList.of(sourceName1, "zmyFile1.json"));
    dataset =
        new DatasetConfig()
            .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER)
            .setFullPathList(path.toPathList())
            .setName(path.getLeaf().getName())
            .setCreatedAt(System.currentTimeMillis())
            .setTag(null)
            .setOwner(DEFAULT_USERNAME)
            .setPhysicalDataset(
                new PhysicalDataset().setFormatSettings(new FileConfig().setType(FileType.JSON)));
    l(NamespaceService.class).addOrUpdateDataset(path.toNamespaceKey(), dataset);

    NamespaceKey datasetKey =
        new DatasetPath(ImmutableList.of(sourceName1, "zmyFile1.json")).toNamespaceKey();
    DatasetConfig dataset1 = l(NamespaceService.class).getDataset(datasetKey);

    getOptionManager()
        .setOption(OptionValue.createLong(OptionType.SYSTEM, "dremio.store.dfs.max_files", 1));

    java.io.File deleted = new java.io.File(srcFolder.getAbsolutePath(), "zmyFile1.json");
    boolean bool = deleted.delete();

    // After delete "zmyFile1.json" from the source, if the refresh succeeds, it will delete the
    // dataset from the kv store.
    // If we query "nestedDir", it will be still in the kv store. If we query "zmyFile1.json", it
    // will throw namespace exception.
    ((CatalogServiceImpl) getCatalogService())
        .refreshSource(
            new NamespaceKey(sourceName1),
            CatalogService.REFRESH_EVERYTHING_NOW,
            CatalogServiceImpl.UpdateType.FULL);
    dataset1 =
        getNamespaceService()
            .getDataset(
                new DatasetPath(ImmutableList.of(sourceName1, "nestedDir")).toNamespaceKey());
    assertEquals("nestedDir", dataset1.getName());
    assertThatThrownBy(() -> l(NamespaceService.class).getDataset(datasetKey))
        .isInstanceOf(NamespaceException.class);
  }

  private boolean isComplete(DatasetConfig config) {
    return config != null
        && DatasetHelper.getSchemaBytes(config) != null
        && config.getReadDefinition() != null;
  }

  @Test
  public void testForgetTableDefault() throws Exception {

    final String sourceName = "src_" + System.currentTimeMillis();
    final Source newSource = createDatasetFromSource(sourceName, "myFile.json", SRC_EXTERNAL, null);

    // Do an inline refresh to build complete DatasetConfig before forget metadata
    final String queryForInlineRefresh =
        String.format("select * from %s.\"myFile.json\"", sourceName);
    JobId jobId =
        submitJobAndWaitUntilCompletion(
            JobRequest.newBuilder()
                .setSqlQuery(
                    new SqlQuery(
                        queryForInlineRefresh, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
                .setQueryType(QueryType.UI_INTERNAL_RUN)
                .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
                .build());
    JobSummary job =
        l(JobsService.class)
            .getJobSummary(
                JobSummaryRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId)).build());
    assertSame(JobsProtoUtil.toStuff(job.getJobState()), JobState.COMPLETED);

    DatasetConfig dataset1 =
        l(NamespaceService.class)
            .getDataset(
                new DatasetPath(ImmutableList.of(sourceName, "myFile.json")).toNamespaceKey());
    assertEquals("myFile.json", dataset1.getName());
    assertTrue(isComplete(dataset1));

    // Forget metadata
    final String query =
        String.format("alter table %s.\"myFile.json\" forget metadata", sourceName);
    jobId =
        submitJobAndWaitUntilCompletion(
            JobRequest.newBuilder()
                .setSqlQuery(new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
                .setQueryType(QueryType.UI_INTERNAL_RUN)
                .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
                .build());

    // Expect to delete the metadata record in kv store successfully
    job =
        l(JobsService.class)
            .getJobSummary(
                JobSummaryRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId)).build());
    assertSame(JobsProtoUtil.toStuff(job.getJobState()), JobState.COMPLETED);

    // Expect to receive an NamespaceException if try to find the deleted metadata in kv store
    assertThatThrownBy(
            () ->
                getNamespaceService()
                    .getDataset(
                        new DatasetPath(ImmutableList.of(sourceName, "myFile.json"))
                            .toNamespaceKey()))
        .isInstanceOf(NamespaceException.class);
  }

  @Test
  public void testForgetTableMultiple() throws Exception {

    final String sourceName = "src_" + System.currentTimeMillis();
    final Source newSource = createDatasetFromSource(sourceName, "myFile.json", SRC_EXTERNAL, null);

    // Do a full refresh to build complete DatasetConfig before forget metadata
    ((CatalogServiceImpl) getCatalogService())
        .refreshSource(
            new NamespaceKey(sourceName),
            CatalogService.REFRESH_EVERYTHING_NOW,
            CatalogServiceImpl.UpdateType.FULL);
    DatasetConfig dataset1 =
        l(NamespaceService.class)
            .getDataset(
                new DatasetPath(ImmutableList.of(sourceName, "myFile.json")).toNamespaceKey());
    assertEquals("myFile.json", dataset1.getName());
    assertTrue(isComplete(dataset1));

    // Forget metadata
    final String query =
        String.format("alter table %s.\"myFile.json\" forget metadata", sourceName);
    JobId jobId =
        submitJobAndWaitUntilCompletion(
            JobRequest.newBuilder()
                .setSqlQuery(new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
                .setQueryType(QueryType.UI_INTERNAL_RUN)
                .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
                .build());

    // Expect to delete the metadata record in kv store successfully
    final JobSummary job =
        l(JobsService.class)
            .getJobSummary(
                JobSummaryRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId)).build());
    assertSame(JobsProtoUtil.toStuff(job.getJobState()), JobState.COMPLETED);

    // Expect to receive an UserException if try to forget metadata on the deleted metadata in kv
    // store
    final String msg =
        String.format(
            "VALIDATION ERROR: Dataset %s.\"myFile.json\" does not exist or is not a table.",
            sourceName);

    // Forget metadata again
    UserExceptionAssert.assertThatThrownBy(
            () ->
                submitJobAndWaitUntilCompletion(
                    JobRequest.newBuilder()
                        .setSqlQuery(
                            new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
                        .setQueryType(QueryType.UI_INTERNAL_RUN)
                        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
                        .build()))
        .hasErrorType(ErrorType.VALIDATION)
        .hasMessageContaining(msg);
  }

  @Test
  public void testForgetTableWithNameRefresh() throws Exception {

    final String sourceName = "src_" + System.currentTimeMillis();
    final Source newSource = createDatasetFromSource(sourceName, "myFile.json", SRC_EXTERNAL, null);

    // Do an inline refresh to build complete DatasetConfig before forget metadata
    final String queryForInlineRefresh =
        String.format("select * from %s.\"myFile.json\"", sourceName);
    JobId jobId =
        submitJobAndWaitUntilCompletion(
            JobRequest.newBuilder()
                .setSqlQuery(
                    new SqlQuery(
                        queryForInlineRefresh, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
                .setQueryType(QueryType.UI_INTERNAL_RUN)
                .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
                .build());
    JobSummary job =
        l(JobsService.class)
            .getJobSummary(
                JobSummaryRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId)).build());
    assertSame(JobsProtoUtil.toStuff(job.getJobState()), JobState.COMPLETED);

    DatasetConfig dataset1 =
        l(NamespaceService.class)
            .getDataset(
                new DatasetPath(ImmutableList.of(sourceName, "myFile.json")).toNamespaceKey());
    assertEquals("myFile.json", dataset1.getName());
    assertTrue(isComplete(dataset1));

    // Forget metadata
    String query = String.format("alter table %s.\"myFile.json\" forget metadata", sourceName);
    jobId =
        submitJobAndWaitUntilCompletion(
            JobRequest.newBuilder()
                .setSqlQuery(new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
                .setQueryType(QueryType.UI_INTERNAL_RUN)
                .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
                .build());

    // Expect to delete the metadata record in kv store successfully
    job =
        l(JobsService.class)
            .getJobSummary(
                JobSummaryRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId)).build());
    assertSame(JobsProtoUtil.toStuff(job.getJobState()), JobState.COMPLETED);

    // Emulate NameRefresh to put a shallow metadata into kv store
    DatasetPath path = new DatasetPath(ImmutableList.of(sourceName, "myFile.json"));
    DatasetConfig dataset =
        new DatasetConfig()
            .setId(new EntityId().setId(UUID.randomUUID().toString()))
            .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER)
            .setFullPathList(path.toPathList())
            .setName(path.getLeaf().getName())
            .setCreatedAt(System.currentTimeMillis())
            .setTag(null);
    l(NamespaceService.class).addOrUpdateDataset(path.toNamespaceKey(), dataset);

    // Expect to find the metadata record when query the kv store
    dataset1 =
        getNamespaceService()
            .getDataset(
                new DatasetPath(ImmutableList.of(sourceName, "myFile.json")).toNamespaceKey());
    assertEquals("myFile.json", dataset1.getName());
    // Assert shallow dataset config is incomplete
    assertFalse(isComplete(dataset1));

    // Forget metadata again
    jobId =
        submitJobAndWaitUntilCompletion(
            JobRequest.newBuilder()
                .setSqlQuery(new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
                .setQueryType(QueryType.UI_INTERNAL_RUN)
                .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
                .build());

    // Expect to delete the metadata record in kv store successfully
    job =
        l(JobsService.class)
            .getJobSummary(
                JobSummaryRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId)).build());
    assertSame(JobsProtoUtil.toStuff(job.getJobState()), JobState.COMPLETED);

    // Expect to receive an NamespaceException if try to find the deleted metadata in kv store
    assertThatThrownBy(
            () ->
                p(NamespaceService.class)
                    .get()
                    .getDataset(
                        new DatasetPath(ImmutableList.of(sourceName, "myFile.json"))
                            .toNamespaceKey()))
        .isInstanceOf(NamespaceException.class);
  }

  @Test
  public void testVDSConcurrency() throws Exception {
    Space space = createSpace("concurrency");

    int max = 5;

    List<Thread> threads = new ArrayList<>();
    for (int i = 1; i <= max; i++) {
      threads.add(createVDSInSpace("vds" + i, space.getName(), "select " + i));
    }

    threads.forEach(Thread::start);

    for (Thread thread : threads) {
      thread.join();
    }

    // verify that the VDS were created with the correct SQL
    for (int i = 1; i <= max; i++) {
      DatasetConfig dataset =
          getNamespaceService()
              .getDataset(new NamespaceKey(Arrays.asList(space.getName(), "vds" + i)));
      assertEquals(dataset.getVirtualDataset().getSql(), "select " + i);
    }

    getNamespaceService().deleteSpace(new NamespaceKey(space.getName()), space.getTag());
  }

  private Thread createVDSInSpace(String name, String spaceName, String sql) {
    return new Thread(
        () -> {
          createVDS(Arrays.asList(spaceName, name), sql);
        });
  }

  @Test
  public void testSource() throws Exception {
    Source source = createSource();

    // make sure we can fetch the source by id
    source =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(source.getId())).buildGet(),
            new GenericType<Source>() {});

    // make sure that trying to create the source again fails
    NASConf nasConf = new NASConf();
    nasConf.path = TestTools.getWorkingPath() + "/src/test/resources";

    Source newSource = new Source();
    newSource.setName("catalog-test");
    newSource.setType("NAS");
    newSource.setConfig(nasConf);
    expectStatus(
        Response.Status.CONFLICT,
        getBuilder(getHttpClient().getCatalogApi()).buildPost(Entity.json(newSource)));

    // edit source
    source.setAccelerationRefreshPeriodMs(0);
    source =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(source.getId()))
                .buildPut(Entity.json(source)),
            new GenericType<Source>() {});

    assertNotNull(source.getTag());
    assertEquals((long) source.getAccelerationRefreshPeriodMs(), 0);

    // adding a folder to a source should fail
    Folder newFolder = new Folder(null, Arrays.asList(source.getName(), "myFolder"), null, null);
    expectStatus(
        Response.Status.BAD_REQUEST,
        getBuilder(getHttpClient().getCatalogApi()).buildPost(Entity.json(newFolder)));

    // delete source
    expectSuccess(getBuilder(getHttpClient().getCatalogApi().path(source.getId())).buildDelete());

    Source finalSource = source;
    assertThatThrownBy(
            () -> getNamespaceService().getSource(new NamespaceKey(finalSource.getName())))
        .isInstanceOf(NamespaceException.class);
  }

  @Test
  public void testCreateSourceWithInternalConnectionConfTypes() {
    String location =
        Path.of("file:///" + tempDir + "/" + "testCreateSourceWithInternalConnectionConfTypes/")
            .toString();
    Set<ConnectionConf<?, ?>> internalConnectionConfs =
        new HashSet<ConnectionConf<?, ?>>() {
          {
            add(new PDFSConf(location));
            add(new HomeFileConf(location, "localhost"));
          }
        };

    for (ConnectionConf<?, ?> conf : internalConnectionConfs) {
      final String sourceName = UUID.randomUUID().toString();
      Source source = new Source();
      source.setName(sourceName);
      source.setType(conf.getType());
      source.setConfig(conf);

      expectStatus(
          BAD_REQUEST, getBuilder(getHttpClient().getCatalogApi()).buildPost(Entity.json(source)));
    }
  }

  @Test
  public void testUpdateSourceWithInternalConnectionConfTypes() {
    String location =
        Path.of("file:///" + tempDir + "/" + "testUpdateSourceWithInternalConnectionConfTypes/")
            .toString();
    Set<ConnectionConf<?, ?>> internalConnectionConfs =
        new HashSet<ConnectionConf<?, ?>>() {
          {
            add(new PDFSConf(location));
            add(new HomeFileConf(location, "localhost"));
          }
        };

    Source source = createSource();

    for (ConnectionConf<?, ?> conf : internalConnectionConfs) {
      source.setType(conf.getType());
      source.setConfig(conf);

      expectStatus(
          BAD_REQUEST,
          getBuilder(
                  getHttpClient()
                      .getCatalogApi()
                      .path(PathUtils.encodeURIComponent(source.getId())))
              .buildPut(Entity.json(source)));
    }
  }

  @Test
  public void testSourceBrowsing() throws Exception {
    Source source = createSource();

    // browse to the json directory
    String id = getFolderIdByName(source.getChildren(), "json");
    assertNotNull("Failed to find json directory", id);

    // deleting a folder on a source should fail
    expectStatus(
        Response.Status.BAD_REQUEST,
        getBuilder(
                getHttpClient()
                    .getCatalogApi()
                    .path(com.dremio.common.utils.PathUtils.encodeURIComponent(id)))
            .buildDelete());
  }

  @Test
  public void testSourcePromoting() throws Exception {
    Source source = createSource();

    // browse to the json directory
    String id = getFolderIdByName(source.getChildren(), "json");
    assertNotNull("Failed to find json directory", id);

    // load the json dir
    Folder folder =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getCatalogApi()
                        .path(com.dremio.common.utils.PathUtils.encodeURIComponent(id)))
                .buildGet(),
            new GenericType<Folder>() {});
    assertEquals(folder.getChildren().size(), 19);

    String fileId = null;

    for (CatalogItem item : folder.getChildren()) {
      List<String> path = item.getPath();
      // get the numbers.json file
      if (item.getType() == CatalogItem.CatalogItemType.FILE
          && path.get(path.size() - 1).equals("numbers.json")) {
        fileId = item.getId();
        break;
      }
    }

    assertNotNull("Failed to find numbers.json file", fileId);

    // load the file
    File file =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getCatalogApi()
                        .path(com.dremio.common.utils.PathUtils.encodeURIComponent(fileId)))
                .buildGet(),
            new GenericType<File>() {});

    // promote the file (dac/backend/src/test/resources/json/numbers.json)
    Dataset dataset =
        createPDS(CatalogServiceHelper.getPathFromInternalId(file.getId()), new JsonFileConfig());

    dataset =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getCatalogApi()
                        .path(com.dremio.common.utils.PathUtils.encodeURIComponent(fileId)))
                .buildPost(Entity.json(dataset)),
            new GenericType<Dataset>() {});

    // load the dataset
    dataset =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(dataset.getId())).buildGet(),
            new GenericType<Dataset>() {});

    // verify listing
    folder =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getCatalogApi()
                        .path(com.dremio.common.utils.PathUtils.encodeURIComponent(id)))
                .buildGet(),
            new GenericType<Folder>() {});
    assertEquals(folder.getChildren().size(), 19);

    // unpromote file
    expectSuccess(getBuilder(getHttpClient().getCatalogApi().path(dataset.getId())).buildDelete());

    // dataset should no longer exist
    expectStatus(
        Response.Status.NOT_FOUND,
        getBuilder(getHttpClient().getCatalogApi().path(dataset.getId())).buildGet());

    // verify listing
    folder =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getCatalogApi()
                        .path(com.dremio.common.utils.PathUtils.encodeURIComponent(id)))
                .buildGet(),
            new GenericType<Folder>() {});
    assertEquals(folder.getChildren().size(), 19);

    // promote a folder that contains several csv files
    // (dac/backend/src/test/resources/datasets/folderdataset)
    String folderId = getFolderIdByName(source.getChildren(), "datasets");
    assertNotNull("Failed to find datasets directory", folderId);

    Folder dsFolder =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getCatalogApi()
                        .path(com.dremio.common.utils.PathUtils.encodeURIComponent(folderId)))
                .buildGet(),
            new GenericType<Folder>() {});

    String folderDatasetId = getFolderIdByName(dsFolder.getChildren(), "folderdataset");
    assertNotNull("Failed to find folderdataset directory", folderDatasetId);

    // we want to use the path that the backend gives us so fetch the full folder
    folder =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getCatalogApi()
                        .path(
                            com.dremio.common.utils.PathUtils.encodeURIComponent(folderDatasetId)))
                .buildGet(),
            new GenericType<Folder>() {});

    ParquetFileConfig parquetFileConfig = new ParquetFileConfig();
    Dataset.RefreshSettings refreshSettings =
        new Dataset.RefreshSettings(
            RefreshPolicyType.PERIOD,
            null,
            3600000L,
            null,
            10800000L,
            RefreshMethod.INCREMENTAL,
            false,
            false,
            false);
    dataset = createPDS(folder.getPath(), parquetFileConfig, refreshSettings);

    Dataset datasetPromoted =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getCatalogApi()
                        .path(
                            com.dremio.common.utils.PathUtils.encodeURIComponent(folderDatasetId)))
                .buildPost(Entity.json(dataset)),
            new GenericType<Dataset>() {});

    // load the promoted dataset
    dataset =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(datasetPromoted.getId())).buildGet(),
            new GenericType<Dataset>() {});

    // Verify the response from POST matches the response from GET
    assertEquals(dataset.getId(), datasetPromoted.getId());
    assertEquals(RefreshMethod.INCREMENTAL, dataset.getAccelerationRefreshPolicy().getMethod());
    assertEquals(
        RefreshMethod.INCREMENTAL, datasetPromoted.getAccelerationRefreshPolicy().getMethod());
    assertEquals(
        dataset.getAccelerationRefreshPolicy().getRefreshPeriodMs(),
        datasetPromoted.getAccelerationRefreshPolicy().getRefreshPeriodMs());
    assertEquals(
        dataset.getAccelerationRefreshPolicy().getGracePeriodMs(),
        datasetPromoted.getAccelerationRefreshPolicy().getGracePeriodMs());

    // unpromote the folder
    expectSuccess(getBuilder(getHttpClient().getCatalogApi().path(dataset.getId())).buildDelete());

    // dataset should no longer exist
    expectStatus(
        Response.Status.NOT_FOUND,
        getBuilder(getHttpClient().getCatalogApi().path(dataset.getId())).buildGet());
  }

  @Test
  public void testSourceEditWithoutSecret() throws Exception {
    // fakesource only works if password is the same as the name, else with fail to create
    final FakeSource fakeConf = new FakeSource();
    fakeConf.password = "fake";
    fakeConf.isAwesome = true;

    final Source newSource = new Source();
    newSource.setName("fake");
    newSource.setType("FAKESOURCE");
    newSource.setConfig(fakeConf);

    // create the source
    Source source =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi()).buildPost(Entity.json(newSource)),
            new GenericType<Source>() {});

    FakeSource config = (FakeSource) source.getConfig();

    // we should get back the use existing secret const
    assertTrue(config.isAwesome);
    assertEquals(config.password, ConnectionConf.USE_EXISTING_SECRET_VALUE);

    // verify that saving with the const works
    config.isAwesome = false;
    source.setConfig(config);

    source =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(source.getId()))
                .buildPut(Entity.json(source)),
            new GenericType<Source>() {});
    config = (FakeSource) source.getConfig();

    assertFalse(config.isAwesome);
    assertNotNull(source.getTag());
  }

  private Source createSource() {
    NASConf nasConf = new NASConf();
    nasConf.path = TestTools.getWorkingPath() + "/src/test/resources";

    Source newSource = new Source();
    newSource.setName("catalog-test");
    newSource.setType("NAS");
    newSource.setConfig(nasConf);

    // create the source
    return expectSuccess(
        getBuilder(getHttpClient().getCatalogApi()).buildPost(Entity.json(newSource)),
        new GenericType<Source>() {});
  }

  @Test
  public void testHome() throws Exception {
    ResponseList<CatalogItem> items = getRootEntities(null);

    String homeId = null;

    for (CatalogItem item : items.getData()) {
      if (item.getType() == CatalogItem.CatalogItemType.CONTAINER
          && item.getContainerType() == CatalogItem.ContainerSubType.HOME) {
        homeId = item.getId();
        break;
      }
    }

    // load home space
    Home home =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(homeId)).buildGet(),
            new GenericType<Home>() {});

    int size = home.getChildren().size();

    // add a folder
    Folder newFolder = new Folder(null, Arrays.asList(home.getName(), "myFolder"), null, null);
    Folder folder =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi()).buildPost(Entity.json(newFolder)),
            new GenericType<Folder>() {});
    assertEquals(newFolder.getPath(), folder.getPath());

    // make sure folder shows up under space
    home =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(homeId)).buildGet(),
            new GenericType<Home>() {});
    assertEquals(home.getChildren().size(), size + 1);

    // make sure that trying to create the folder again fails
    expectStatus(
        Response.Status.CONFLICT,
        getBuilder(getHttpClient().getCatalogApi()).buildPost(Entity.json(newFolder)));

    // load folder
    folder =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(folder.getId())).buildGet(),
            new GenericType<Folder>() {});

    // store a VDS in the folder
    Dataset vds =
        getVDSConfig(
            Arrays.asList(home.getName(), "myFolder", "myVDS"), "select * from sys.version");

    Dataset newVDS =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi()).buildPost(Entity.json(vds)),
            new GenericType<Dataset>() {});

    // folder should have children now
    folder =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(folder.getId())).buildGet(),
            new GenericType<Folder>() {});
    assertEquals(folder.getChildren().size(), 1);

    // delete vds
    expectSuccess(getBuilder(getHttpClient().getCatalogApi().path(newVDS.getId())).buildDelete());

    // folder should have no children now
    folder =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(folder.getId())).buildGet(),
            new GenericType<Folder>() {});
    assertEquals(folder.getChildren().size(), 0);

    // delete folder
    expectSuccess(getBuilder(getHttpClient().getCatalogApi().path(folder.getId())).buildDelete());

    home =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(homeId)).buildGet(),
            new GenericType<Home>() {});
    assertEquals(home.getChildren().size(), size);
  }

  @Test
  public void testByPath() throws Exception {
    Source createdSource = createSource();

    // test getting a source by name
    Source source =
        expectSuccess(
            getBuilder(
                    getHttpClient().getCatalogApi().path("by-path").path(createdSource.getName()))
                .buildGet(),
            new GenericType<Source>() {});
    assertEquals(source.getId(), createdSource.getId());

    // test getting a folder by path
    expectSuccess(
        getBuilder(
                getHttpClient()
                    .getCatalogApi()
                    .path("by-path")
                    .path(createdSource.getName())
                    .path("json"))
            .buildGet(),
        new GenericType<Folder>() {});

    // test getting a file with a url character in name (?)
    expectSuccess(
        getBuilder(
                getHttpClient()
                    .getCatalogApi()
                    .path("by-path")
                    .path(createdSource.getName())
                    .path("testfiles")
                    .path("file_with_?.json"))
            .buildGet(),
        new GenericType<File>() {});
  }

  @Test
  public void testRepromote() throws Exception {
    final Source source = createSource();

    // promote a folder that contains several csv files
    // (dac/backend/src/test/resources/datasets/folderdataset)
    final String folderId = getFolderIdByName(source.getChildren(), "datasets");
    assertNotNull("Failed to find datasets directory", folderId);

    Folder dsFolder =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(PathUtils.encodeURIComponent(folderId)))
                .buildGet(),
            new GenericType<Folder>() {});

    final String folderDatasetId = getFolderIdByName(dsFolder.getChildren(), "folderdataset");
    assertNotNull("Failed to find folderdataset directory", folderDatasetId);

    // we want to use the path that the backend gives us so fetch the full folder
    Folder folder =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getCatalogApi()
                        .path(PathUtils.encodeURIComponent(folderDatasetId)))
                .buildGet(),
            new GenericType<Folder>() {});

    final ParquetFileConfig parquetFileConfig = new ParquetFileConfig();
    Dataset dataset = createPDS(folder.getPath(), parquetFileConfig);

    dataset =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getCatalogApi()
                        .path(PathUtils.encodeURIComponent(folderDatasetId)))
                .buildPost(Entity.json(dataset)),
            new GenericType<Dataset>() {});

    // load the promoted dataset
    dataset =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(dataset.getId())).buildGet(),
            new GenericType<Dataset>() {});

    // unpromote the folder
    expectSuccess(getBuilder(getHttpClient().getCatalogApi().path(dataset.getId())).buildDelete());

    // dataset should no longer exist
    expectStatus(
        Response.Status.NOT_FOUND,
        getBuilder(getHttpClient().getCatalogApi().path(dataset.getId())).buildGet());

    // re-promote the folder by using by-path
    dataset = createPDS(folder.getPath(), parquetFileConfig);
    dataset =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getCatalogApi()
                        .path(PathUtils.encodeURIComponent(folderDatasetId)))
                .buildPost(Entity.json(dataset)),
            new GenericType<Dataset>() {});

    // unpromote the folder
    expectSuccess(getBuilder(getHttpClient().getCatalogApi().path(dataset.getId())).buildDelete());
  }

  @Test
  public void testErrors() throws Exception {
    // test non-existent id
    expectStatus(
        Response.Status.NOT_FOUND,
        getBuilder(getHttpClient().getCatalogApi().path("bad-id")).buildGet());

    // test non-existent internal id
    expectStatus(
        Response.Status.NOT_FOUND,
        getBuilder(
                getHttpClient()
                    .getCatalogApi()
                    .path(CatalogServiceHelper.generateInternalId(Arrays.asList("bad-id"))))
            .buildGet());

    // test non-existent path
    expectStatus(
        Response.Status.NOT_FOUND,
        getBuilder(getHttpClient().getCatalogApi().path("by-path").path("doesnot").path("exist"))
            .buildGet());
  }

  @Test
  public void testExcludeChildren() throws Exception {
    Space space = createSpace("children test");

    createVDS(Arrays.asList(space.getName(), "vds1"), "select * from sys.version");
    createVDS(Arrays.asList(space.getName(), "vds2"), "select * from sys.version");

    Space result =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path("by-path").path(space.getName()))
                .buildGet(),
            new GenericType<Space>() {});
    assertEquals(2, result.getChildren().size());

    result =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getCatalogApi()
                        .path("by-path")
                        .path(space.getName())
                        .queryParam("exclude", "children"))
                .buildGet(),
            new GenericType<Space>() {});
    assertEquals(0, result.getChildren().size());

    result =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(space.getId())).buildGet(),
            new GenericType<Space>() {});
    assertEquals(2, result.getChildren().size());

    result =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getCatalogApi()
                        .path(space.getId())
                        .queryParam("exclude", "children"))
                .buildGet(),
            new GenericType<Space>() {});
    assertEquals(0, result.getChildren().size());
  }

  @Test
  public void testPromotingReflectionSettings() throws Exception {
    Source source = createSource();

    // browse to the json directory
    String id = getFolderIdByName(source.getChildren(), "json");
    assertNotNull("Failed to find json directory", id);

    // load the json dir
    Folder folder =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getCatalogApi()
                        .path(com.dremio.common.utils.PathUtils.encodeURIComponent(id)))
                .buildGet(),
            new GenericType<Folder>() {});
    assertEquals(folder.getChildren().size(), 19);

    // promote a folder that contains several csv files
    // (dac/backend/src/test/resources/datasets/folderdataset)
    String folderId = getFolderIdByName(source.getChildren(), "datasets");
    assertNotNull("Failed to find datasets directory", folderId);

    Folder dsFolder =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getCatalogApi()
                        .path(com.dremio.common.utils.PathUtils.encodeURIComponent(folderId)))
                .buildGet(),
            new GenericType<Folder>() {});

    String folderDatasetId = getFolderIdByName(dsFolder.getChildren(), "folderdataset");
    assertNotNull("Failed to find folderdataset directory", folderDatasetId);

    // we want to use the path that the backend gives us so fetch the full folder
    folder =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getCatalogApi()
                        .path(
                            com.dremio.common.utils.PathUtils.encodeURIComponent(folderDatasetId)))
                .buildGet(),
            new GenericType<Folder>() {});

    ParquetFileConfig parquetFileConfig = new ParquetFileConfig();
    Dataset.RefreshSettings refreshSettings =
        new Dataset.RefreshSettings(
            RefreshPolicyType.PERIOD,
            null,
            5000L,
            null,
            5000L,
            RefreshMethod.INCREMENTAL,
            false,
            false,
            false);

    Dataset dataset = createPDS(folder.getPath(), parquetFileConfig, refreshSettings);

    dataset =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getCatalogApi()
                        .path(
                            com.dremio.common.utils.PathUtils.encodeURIComponent(folderDatasetId)))
                .buildPost(Entity.json(dataset)),
            new GenericType<Dataset>() {});

    // load the promoted dataset
    dataset =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(dataset.getId())).buildGet(),
            new GenericType<Dataset>() {});
    assertEquals(RefreshMethod.INCREMENTAL, dataset.getAccelerationRefreshPolicy().getMethod());
    assertFalse(dataset.getAccelerationRefreshPolicy().getNeverRefresh());
    assertFalse(dataset.getAccelerationRefreshPolicy().getNeverExpire());

    // update reflection settings
    refreshSettings =
        new Dataset.RefreshSettings(
            RefreshPolicyType.PERIOD,
            null,
            500L,
            null,
            500L,
            RefreshMethod.FULL,
            true,
            true,
            false);
    Dataset newPDS = createPDS(dataset, refreshSettings);
    dataset =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(dataset.getId()))
                .buildPut(Entity.json(newPDS)),
            new GenericType<Dataset>() {});
    assertEquals(RefreshMethod.FULL, dataset.getAccelerationRefreshPolicy().getMethod());
    assertTrue(dataset.getAccelerationRefreshPolicy().getNeverRefresh());
    assertTrue(dataset.getAccelerationRefreshPolicy().getNeverExpire());
    assertEquals(
        refreshSettings.getRefreshPeriodMs(),
        dataset.getAccelerationRefreshPolicy().getRefreshPeriodMs());
    assertEquals(
        refreshSettings.getGracePeriodMs(),
        dataset.getAccelerationRefreshPolicy().getGracePeriodMs());

    // unpromote the folder
    expectSuccess(getBuilder(getHttpClient().getCatalogApi().path(dataset.getId())).buildDelete());

    // dataset should no longer exist
    expectStatus(
        Response.Status.NOT_FOUND,
        getBuilder(getHttpClient().getCatalogApi().path(dataset.getId())).buildGet());
  }

  @ParameterizedTest
  @ValueSource(strings = {"0,0", "1,0", "0,1", "1,1"})
  public void testPaginateChildren_namespace(String paginateAndIdOrPath) throws Exception {
    boolean paginate = paginateAndIdOrPath.split(",")[0].equals("1");
    boolean byId = paginateAndIdOrPath.split(",")[1].equals("1");
    NamespaceService namespaceService = getNamespaceService();

    // Create space.
    NamespaceKey spaceKey = new NamespaceKey("pagination" + paginateAndIdOrPath.replace(',', '_'));
    SpaceConfig spaceConfig = new SpaceConfig();
    spaceConfig.setName(spaceKey.getName());
    namespaceService.addOrUpdateSpace(spaceKey, spaceConfig);
    String spaceId = namespaceService.getEntityByPath(spaceKey).getSpace().getId().getId();

    // Create folders in the space.
    List<String> allExpectedChildrenPaths = new ArrayList<>();
    int numFolders = 20;
    for (int i = 0; i < numFolders; i++) {
      String folderName = String.format("folder%03d", i);
      NamespaceKey folderKey = new NamespaceKey(asList(spaceKey.getName(), folderName));
      FolderConfig folderConfig =
          new FolderConfig().setName(folderName).setFullPathList(folderKey.getPathComponents());
      namespaceService.addOrUpdateFolder(folderKey, folderConfig);
      allExpectedChildrenPaths.add(String.format("%s.%s", spaceKey.getName(), folderName));
    }

    // Verify that children were created in KV store.
    List<NameSpaceContainer> spaceNamespaceChildren =
        namespaceService.list(spaceKey, null, Integer.MAX_VALUE);
    assertThat(spaceNamespaceChildren).hasSize(numFolders);

    List<String> allChildrenPaths = new ArrayList<>();
    if (paginate) {
      // Paginate over the space.
      int macChildren = 7;
      String pageToken = null;
      do {
        WebTarget target = getHttpClient().getCatalogApi();
        if (byId) {
          target = target.path(spaceId);
        } else {
          target = target.path("by-path").path(spaceKey.getName());
        }
        target = target.queryParam("maxChildren", macChildren);
        if (pageToken != null) {
          target = target.queryParam("pageToken", pageToken);
        }
        Space space = expectSuccess(getBuilder(target).buildGet(), Space.class);
        assertThat(space.getChildren().size()).isLessThanOrEqualTo(macChildren);

        pageToken = space.getNextPageToken();

        space.getChildren().forEach(c -> allChildrenPaths.add(String.join(".", c.getPath())));
      } while (pageToken != null);
    } else {
      WebTarget target = getHttpClient().getCatalogApi();
      if (byId) {
        target = target.path(spaceId);
      } else {
        target = target.path("by-path").path(spaceKey.getName());
      }
      Space space = expectSuccess(getBuilder(target).buildGet(), Space.class);
      space.getChildren().forEach(c -> allChildrenPaths.add(String.join(".", c.getPath())));
    }

    assertEquals(allChildrenPaths, allExpectedChildrenPaths);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testPaginateChildren_nessieInvalidToken(boolean useInvalidTokenOrPath)
      throws Exception {
    NamespaceService namespaceService = getNamespaceService();

    String sourceId =
        namespaceService
            .getEntityByPath(new NamespaceKey(NESSIE_SOURCE_NAME))
            .getSource()
            .getId()
            .getId();

    // Create folders in the source.
    int numFolders = 20;
    for (int i = 0; i < numFolders; i++) {
      expectSuccess(
          getBuilder(getHttpClient().getCatalogApi())
              .buildPost(
                  Entity.json(
                      new Folder(
                          null,
                          asList(NESSIE_SOURCE_NAME, String.format("folder%03d", i)),
                          null,
                          null))));
    }

    // Get page token.
    Source source =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path(sourceId).queryParam("maxChildren", 3))
                .buildGet(),
            Source.class);
    CatalogPageToken catalogPageToken = CatalogPageToken.fromApiToken(source.getNextPageToken());

    // Get an error with invalid token.
    WebTarget target = getHttpClient().getCatalogApi().path(sourceId);
    String pageToken;
    if (useInvalidTokenOrPath) {
      pageToken =
          catalogPageToken.toBuilder()
              .setVersionContext(VersionContext.ofBranch("main"))
              .build()
              .toApiToken();
    } else {
      pageToken = catalogPageToken.toBuilder().setPath("abc").build().toApiToken();
    }
    String error =
        expectError(
            FamilyExpectation.CLIENT_ERROR,
            getBuilder(target.queryParam("maxChildren", 3).queryParam("pageToken", pageToken))
                .buildGet(),
            String.class);
    assertThat(error)
        .contains(
            useInvalidTokenOrPath
                ? "Passed version [Unspecified version context] does not match previous version [branch main]"
                : "Passed path [[nessie-source]] does not match previous path [[abc]]");
  }

  @ParameterizedTest
  @ValueSource(strings = {"0,0", "1,0", "0,1", "1,1"})
  public void testPaginateChildren_nessie(String paginateAndIdOrPath) throws Exception {
    boolean paginate = paginateAndIdOrPath.split(",")[0].equals("1");
    boolean byId = paginateAndIdOrPath.split(",")[1].equals("1");
    NamespaceService namespaceService = getNamespaceService();

    String sourceId =
        namespaceService
            .getEntityByPath(new NamespaceKey(NESSIE_SOURCE_NAME))
            .getSource()
            .getId()
            .getId();

    // Create folders in the source.
    List<String> allExpectedChildrenPaths = new ArrayList<>();
    int numFolders = 20;
    for (int i = 0; i < numFolders; i++) {
      String folderName = String.format("folder%03d", i);
      expectSuccess(
          getBuilder(getHttpClient().getCatalogApi())
              .buildPost(
                  Entity.json(
                      new Folder(null, asList(NESSIE_SOURCE_NAME, folderName), null, null))));
      allExpectedChildrenPaths.add(String.format("%s.%s", NESSIE_SOURCE_NAME, folderName));
    }

    // Verify that no children were created in KV store.
    List<NameSpaceContainer> sourceNamespaceChildren =
        namespaceService.list(
            new NamespaceKey(ImmutableList.of(NESSIE_SOURCE_NAME)), null, Integer.MAX_VALUE);
    assertThat(sourceNamespaceChildren).isEmpty();

    List<String> allChildrenPaths = new ArrayList<>();
    if (paginate) {
      // Paginate over the source.
      int maxChildren = 7;
      String pageToken = null;
      do {
        WebTarget target = getHttpClient().getCatalogApi();
        if (byId) {
          target = target.path(sourceId);
        } else {
          target = target.path("by-path").path(NESSIE_SOURCE_NAME);
        }
        target = target.queryParam("maxChildren", maxChildren);
        if (pageToken != null) {
          target = target.queryParam("pageToken", pageToken);
        }
        Source source = expectSuccess(getBuilder(target).buildGet(), Source.class);
        assertThat(source.getChildren().size()).isLessThanOrEqualTo(maxChildren);

        pageToken = source.getNextPageToken();

        source.getChildren().forEach(c -> allChildrenPaths.add(String.join(".", c.getPath())));
      } while (pageToken != null);
    } else {
      // Don't paginate.
      WebTarget target = getHttpClient().getCatalogApi();
      if (byId) {
        target = target.path(sourceId);
      } else {
        target = target.path("by-path").path(NESSIE_SOURCE_NAME);
      }
      Source source = expectSuccess(getBuilder(target).buildGet(), Source.class);
      source.getChildren().forEach(c -> allChildrenPaths.add(String.join(".", c.getPath())));
    }

    assertEquals(allChildrenPaths, allExpectedChildrenPaths);
  }

  /** Tests "by-ids" and "by-paths" endpoints. */
  @Test
  public void test_getList() throws Exception {
    NamespaceService namespaceService = getNamespaceService();

    List<List<String>> entityPaths = new ArrayList<>();
    List<String> entityIds = new ArrayList<>();

    // Create folders in Nessie.
    String sourceId =
        namespaceService
            .getEntityByPath(new NamespaceKey(NESSIE_SOURCE_NAME))
            .getSource()
            .getId()
            .getId();
    entityPaths.add(asList(NESSIE_SOURCE_NAME));
    entityIds.add(sourceId);
    int numFolders = 5;
    for (int i = 0; i < numFolders; i++) {
      String folderName = String.format("folder%03d", i);
      Folder folder =
          expectSuccess(
              getBuilder(getHttpClient().getCatalogApi())
                  .buildPost(
                      Entity.json(
                          new Folder(null, asList(NESSIE_SOURCE_NAME, folderName), null, null))),
              new GenericType<>() {});
      entityPaths.add(asList(NESSIE_SOURCE_NAME, folderName));
      entityIds.add(folder.getId());
    }

    // Create space.
    Space space = createSpace("space");
    entityPaths.add(asList(space.getName()));
    entityIds.add(space.getId());

    // Add invalid entity id.
    String invalidId = UUID.randomUUID().toString();
    entityIds.add(invalidId);
    List<String> invalidPath = ImmutableList.of("non-existent");
    entityPaths.add(invalidPath);

    // Get entities by id.
    int maxChildren = 2;
    ResponseList<CatalogEntity> responseListByIds =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getCatalogApi()
                        .path("by-ids")
                        .queryParam("maxChildren", Integer.toString(maxChildren)))
                .buildPost(Entity.json(entityIds)),
            new GenericType<>() {});

    // Get entities by path.
    ResponseList<CatalogEntity> responseListByPaths =
        expectSuccess(
            getBuilder(
                    getHttpClient()
                        .getCatalogApi()
                        .path("by-paths")
                        .queryParam("maxChildren", Integer.toString(maxChildren)))
                .buildPost(Entity.json(entityPaths)),
            new GenericType<>() {});

    // Verify result.
    assertThat(
            responseListByIds.getData().stream()
                .map(CatalogEntity::getId)
                .collect(Collectors.toUnmodifiableList()))
        .isEqualTo(entityIds.subList(0, entityIds.size() - 1));
    assertThat(responseListByIds.getErrors()).hasSize(1);
    assertThat(responseListByIds.getErrors().get(0).getErrorMessage())
        .startsWith(String.format("'%s'", invalidId));
    assertInstanceOf(Source.class, responseListByIds.getData().get(0));
    Source source = (Source) responseListByIds.getData().get(0);
    assertThat(source.getChildren()).hasSize(maxChildren);
    assertThat(
            responseListByPaths.getData().stream()
                .map(CatalogEntity::getId)
                .collect(Collectors.toUnmodifiableList()))
        .isEqualTo(entityIds.subList(0, entityIds.size() - 1));
    assertThat(responseListByPaths.getErrors().get(0).getErrorMessage())
        .startsWith(String.format("'%s'", invalidPath));
  }

  @Test
  public void test_deleteById() {
    Space space = createSpace("deleteById");
    String viewName = "view";
    Dataset dataset =
        createVDS(Arrays.asList(space.getName(), viewName), "select * from sys.version");

    expectSuccess(getBuilder(getHttpClient().getCatalogApi().path(dataset.getId())).buildDelete());

    assertTrue(getNamespaceService().getEntityById(new EntityId(dataset.getId())).isEmpty());
  }

  public static String getFolderIdByName(List<CatalogItem> items, String nameToFind) {
    for (CatalogItem item : items) {
      List<String> path = item.getPath();
      if (item.getContainerType() == CatalogItem.ContainerSubType.FOLDER
          && path.get(path.size() - 1).equals(nameToFind)) {
        return item.getId();
      }
    }

    return null;
  }

  private Dataset createPDS(List<String> path, FileFormat format) {
    return createPDS(path, format, null);
  }

  private Dataset createPDS(
      List<String> path, FileFormat format, Dataset.RefreshSettings refreshSettings) {
    return new Dataset(
        null,
        Dataset.DatasetType.PHYSICAL_DATASET,
        path,
        null,
        null,
        null,
        refreshSettings,
        null,
        null,
        format,
        null);
  }

  private Dataset createPDS(Dataset dataset, Dataset.RefreshSettings refreshSettings) {
    return new Dataset(
        dataset.getId(),
        dataset.getType(),
        dataset.getPath(),
        ImmutableList.of(),
        dataset.getCreatedAt(),
        dataset.getTag(),
        refreshSettings,
        dataset.getSql(),
        dataset.getSqlContext(),
        dataset.getFormat(),
        dataset.getApproximateStatisticsAllowed());
  }

  public static Dataset getVDSConfig(List<String> path, String sql) {
    return new Dataset(
        null,
        Dataset.DatasetType.VIRTUAL_DATASET,
        path,
        null,
        null,
        null,
        null,
        sql,
        null,
        null,
        null);
  }

  private Dataset createVDS(List<String> path, String sql) {
    Dataset vds = getVDSConfig(path, sql);
    return expectSuccess(
        getBuilder(getHttpClient().getCatalogApi()).buildPost(Entity.json(vds)),
        new GenericType<Dataset>() {});
  }

  private Folder getFolderConfig(List<String> path) {
    return new Folder(null, path, null, null);
  }

  private Folder createFolder(List<String> path) {
    return createFolder(getFolderConfig(path));
  }

  private Folder createFolder(Folder folder) {
    return expectSuccess(
        getBuilder(getHttpClient().getCatalogApi()).buildPost(Entity.json(folder)),
        new GenericType<Folder>() {});
  }
}
