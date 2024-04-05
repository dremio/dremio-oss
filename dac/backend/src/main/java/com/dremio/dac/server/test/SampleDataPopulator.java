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
package com.dremio.dac.server.test;

import static com.dremio.dac.model.spaces.HomeName.HOME_PREFIX;
import static com.dremio.dac.server.test.DataPopulatorUtils.addDefaultDremioUser;
import static com.dremio.dac.server.test.DataPopulatorUtils.createUserIfNotExists;
import static com.dremio.service.namespace.dataset.DatasetVersion.newVersion;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.perf.Timer;
import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.dac.explore.DatasetTool;
import com.dremio.dac.explore.QueryParser;
import com.dremio.dac.explore.QuerySemantics;
import com.dremio.dac.explore.model.DatasetName;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.folder.FolderName;
import com.dremio.dac.model.sources.PhysicalDatasetPath;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.model.sources.UIMetadataPolicy;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.model.spaces.SpaceName;
import com.dremio.dac.model.spaces.SpacePath;
import com.dremio.dac.proto.model.dataset.From;
import com.dremio.dac.proto.model.dataset.FromSQL;
import com.dremio.dac.proto.model.dataset.FromTable;
import com.dremio.dac.proto.model.dataset.Transform;
import com.dremio.dac.proto.model.dataset.TransformCreateFromParent;
import com.dremio.dac.proto.model.dataset.TransformType;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.server.ServerErrorException;
import com.dremio.dac.service.collaboration.CollaborationHelper;
import com.dremio.dac.service.collaboration.Tags;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.dac.service.errors.DatasetVersionNotFoundException;
import com.dremio.dac.service.source.SourceService;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.Views;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.exec.util.TestUtilities;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.jobs.metadata.QueryMetadata;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.namespace.file.proto.JsonFileConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.service.users.SystemUser;
import com.dremio.service.users.UserService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.io.Resources;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import org.apache.commons.io.FileUtils;

/** Populates Dremio spaces with sample data */
public class SampleDataPopulator implements AutoCloseable {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(SampleDataPopulator.class);

  public static final String PASSWORD = "dremio123";
  public static final String DEFAULT_USER_NAME =
      System.getProperty("dremio.test.default-username", "dremio");
  public static final String DEFAULT_USER_FIRSTNAME = "Dre";
  public static final String DEFAULT_USER_LASTNAME = "Mio";

  public static final String POPULATOR_USER_NAME = "data_populator_user";
  public static final String TEST_USER_NAME = "test_user";

  private final NamespaceService namespaceService;
  private final SourceService sourceService;
  private final DatasetVersionMutator datasetService;
  private final UserService userService;
  private final SabotContext context;
  private final String username;
  private final Path path;
  private CollaborationHelper collaborationService;

  @Inject
  public SampleDataPopulator(
      SabotContext context,
      SourceService sourceService,
      DatasetVersionMutator datasetService,
      final UserService userService,
      final NamespaceService namespaceService,
      final String username,
      CollaborationHelper collaborationService)
      throws Exception {
    this.namespaceService = namespaceService;
    this.datasetService = datasetService;
    this.sourceService = sourceService;
    this.userService = userService;
    this.username = username;
    this.context = context;
    this.path = Files.createTempDirectory("sample-data-");
    this.collaborationService = collaborationService;
  }

  /**
   * Utility method that creates default first user and home folder for the default user. First user
   * is an ADMIN user.
   *
   * @param userService {@link UserService} instance
   * @param namespaceService {@link NamespaceService} instance without the user context.
   */
  public static void addDefaultFirstUser(
      final UserService userService, final NamespaceService namespaceService) throws Exception {
    if (!userService.hasAnyUser()) {
      addDefaultDremioUser(userService, namespaceService);
    }
  }

  public void populateTestUsers() throws IOException, NamespaceException {
    createUserIfNotExists(
        userService,
        namespaceService,
        DEFAULT_USER_NAME,
        PASSWORD,
        DEFAULT_USER_FIRSTNAME,
        DEFAULT_USER_LASTNAME);
    createUserIfNotExists(
        userService, namespaceService, TEST_USER_NAME, PASSWORD, TEST_USER_NAME, TEST_USER_NAME);
    createUserIfNotExists(
        userService,
        namespaceService,
        POPULATOR_USER_NAME,
        PASSWORD,
        POPULATOR_USER_NAME,
        POPULATOR_USER_NAME);
  }

  public Path getPath() {
    return path;
  }

  @VisibleForTesting
  public VirtualDatasetUI putDS(String spaceName, String name, From from)
      throws NamespaceException, DatasetNotFoundException {
    return putDS(spaceName, Collections.<FolderName>emptyList(), name, from);
  }

  @VisibleForTesting
  public VirtualDatasetUI putDS(
      String spaceName, List<FolderName> folderPath, String name, From from)
      throws NamespaceException, DatasetNotFoundException {
    Stopwatch sw = Stopwatch.createStarted();

    DatasetPath datasetPath =
        new DatasetPath(
            spaceName.startsWith(HOME_PREFIX) ? new HomeName(spaceName) : new SpaceName(spaceName),
            folderPath,
            new DatasetName(name));

    VirtualDatasetUI ds =
        newDataset(datasetPath, newVersion(), from, datasetPath.toParentPathList());
    datasetService.put(ds);
    long t = sw.elapsed(MILLISECONDS);
    if (t > 100) {
      logger.warn(String.format("creating dataset %s took %dms", datasetPath, t));
    }
    return ds;
  }

  @VisibleForTesting
  public VirtualDatasetUI updateDS(DatasetPath datasetPath, String savedTag, From from)
      throws NamespaceException, DatasetNotFoundException {
    Stopwatch sw = Stopwatch.createStarted();
    VirtualDatasetUI ds =
        newDataset(datasetPath, newVersion(), from, datasetPath.toParentPathList());
    ds.setSavedTag(savedTag);
    datasetService.putVersion(ds);
    long t = sw.elapsed(MILLISECONDS);
    if (t > 100) {
      logger.warn(String.format("creating dataset %s took %dms", datasetPath, t));
    }
    return ds;
  }

  public void populateInitialData()
      throws NamespaceException,
          IOException,
          DatasetNotFoundException,
          ExecutionSetupException,
          DatasetVersionNotFoundException {
    try (TimedBlock b = Timer.time("populateInitialData")) {
      populateInitialData0();
    }
  }

  public void populateSources() throws ExecutionSetupException, NamespaceException {
    {
      SourceUI source = new SourceUI();
      source.setName("LocalFS1");
      source.setCtime(System.currentTimeMillis());
      final NASConf nas = new NASConf();
      nas.path = path.toFile().getPath();
      source.setConfig(nas);
      source.setMetadataPolicy(
          UIMetadataPolicy.of(CatalogService.DEFAULT_METADATA_POLICY_WITH_AUTO_PROMOTE));
      sourceService.registerSourceWithRuntime(source.asSourceConfig(), SystemUser.SYSTEM_USERNAME);
    }

    {
      SourceUI source = new SourceUI();
      source.setName("LocalFS2");
      source.setCtime(System.currentTimeMillis());
      final NASConf nas = new NASConf();
      nas.path = path.toFile().getPath();
      source.setConfig(nas);
      source.setMetadataPolicy(
          UIMetadataPolicy.of(CatalogService.DEFAULT_METADATA_POLICY_WITH_AUTO_PROMOTE));
      sourceService.registerSourceWithRuntime(source.asSourceConfig(), SystemUser.SYSTEM_USERNAME);
    }

    {
      SourceUI source = new SourceUI();
      source.setName("LocalRootRO");
      source.setCtime(System.currentTimeMillis());
      final NASConf nas = new NASConf();
      nas.path = "/";
      source.setConfig(nas);
      source.setMetadataPolicy(
          UIMetadataPolicy.of(CatalogService.DEFAULT_METADATA_POLICY_WITH_AUTO_PROMOTE));
      sourceService.registerSourceWithRuntime(source.asSourceConfig(), SystemUser.SYSTEM_USERNAME);
    }
  }

  private void populateInitialData0()
      throws NamespaceException,
          IOException,
          DatasetNotFoundException,
          ExecutionSetupException,
          DatasetVersionNotFoundException {
    Stopwatch sw = Stopwatch.createStarted();
    populateTestUsers();
    SpaceConfig spaceConfig = new SpaceConfig();
    spaceConfig.setName("Prod-Sample");
    spaceConfig.setDescription("production");
    spaceConfig.setCtime(System.currentTimeMillis());

    // Check if this space already exists, if it does, skip sample dataset population as it will
    // fail
    // to re-create due to OCC conflicts
    try {
      namespaceService.getSpace(
          new SpacePath(new SpaceName(spaceConfig.getName())).toNamespaceKey());

      // If this space already exists (the above line did not throw an exception
      // about space not found), skip sample dataset population entirely, assuming
      // it was successful previously. Partial sample dataset population will need
      // to be fixed by clearing the key value store and starting fresh.
      logger.info(
          "Found "
              + spaceConfig.getName()
              + " space already created. Skipping sample dataset population. If"
              + " the sample datasets have been changed or if they were only partly constructed, clean the KV store and"
              + " restart the server.");
      return;
    } catch (Exception ex) {
      // do nothing and continue with the sample dataset population
    }

    namespaceService.addOrUpdateSpace(
        new SpacePath(new SpaceName(spaceConfig.getName())).toNamespaceKey(), spaceConfig);
    spaceConfig = new SpaceConfig();
    spaceConfig.setName("Sales-Sample");
    spaceConfig.setDescription("sales");
    spaceConfig.setCtime(System.currentTimeMillis());
    namespaceService.addOrUpdateSpace(
        new SpacePath(new SpaceName(spaceConfig.getName())).toNamespaceKey(), spaceConfig);
    spaceConfig = new SpaceConfig();
    spaceConfig.setName("Tpch-Sample");
    spaceConfig.setDescription("tpch queries");
    spaceConfig.setCtime(System.currentTimeMillis());
    namespaceService.addOrUpdateSpace(
        new SpacePath(new SpaceName(spaceConfig.getName())).toNamespaceKey(), spaceConfig);
    spaceConfig = new SpaceConfig();
    spaceConfig.setName("DG");
    spaceConfig.setDescription("dataset graph");
    spaceConfig.setCtime(System.currentTimeMillis());
    namespaceService.addOrUpdateSpace(
        new SpacePath(new SpaceName(spaceConfig.getName())).toNamespaceKey(), spaceConfig);

    populateSources();

    Path folder = new File(path.toFile(), "folder1").toPath();
    if (!Files.exists(folder)) {
      Files.createDirectory(folder);
    }
    File sample1 = new File(path.toFile(), "dac-sample1.json");
    if (!sample1.exists()) {
      try (BufferedWriter writer = new BufferedWriter(new FileWriter(sample1))) {
        for (int i = 0; i < 100; ++i) {
          writer.write(
              String.format(
                  "{ \"user\" : \"user%d\", \"age\": %d, \"address\": \"address%d\"}",
                  i, (i % 25), i));
        }
      }
    }
    File sample2 = new File(path.toFile(), "dac-sample2.json");
    if (!sample2.exists()) {
      try (BufferedWriter writer = new BufferedWriter(new FileWriter(sample2))) {
        for (int i = 500; i < 1000; ++i) {
          writer.write(String.format("{ \"A\" : %d , \"B\": %d }", i, i));
        }
      }
    }

    File sample3 = new File(path.toFile(), "dac-sample3.json");
    if (!sample3.exists()) {
      try (BufferedWriter writer = new BufferedWriter(new FileWriter(sample3))) {
        for (int i = 500; i < 1000; ++i) {
          writer.write(String.format("{ \"A\" : %d , \"B\": %d, \"C\": %d }", i, i, i));
        }
      }
    }

    final String allTypesJson = "all_types_dremio.json";
    File sample4 = new File(path.toFile(), allTypesJson);
    if (!sample4.exists()) {
      try (BufferedWriter writer = new BufferedWriter(new FileWriter(sample4))) {
        writer.write(getFileContentsFromClassPath(allTypesJson));
      }
    }

    // add physical datasets
    {
      PhysicalDatasetPath dacSampleAllTypes =
          new PhysicalDatasetPath(asList("LocalFS1", allTypesJson));
      DatasetConfig dacAllTypes = new DatasetConfig();
      dacAllTypes.setOwner(username);
      dacAllTypes.setCreatedAt(System.currentTimeMillis());
      dacAllTypes.setFullPathList(dacSampleAllTypes.toPathList());
      dacAllTypes.setType(DatasetType.PHYSICAL_DATASET_SOURCE_FILE);
      dacAllTypes.setName(allTypesJson);
      dacAllTypes.setPhysicalDataset(
          new PhysicalDataset().setFormatSettings(new JsonFileConfig().asFileConfig()));
      namespaceService.addOrUpdateDataset(dacSampleAllTypes.toNamespaceKey(), dacAllTypes);
    }

    {
      PhysicalDatasetPath dacSample1Path =
          new PhysicalDatasetPath(asList("LocalFS1", "dac-sample1.json"));
      DatasetConfig dacSample1 = new DatasetConfig();
      dacSample1.setOwner(username);
      dacSample1.setCreatedAt(System.currentTimeMillis());
      dacSample1.setFullPathList(dacSample1Path.toPathList());
      dacSample1.setType(DatasetType.PHYSICAL_DATASET_SOURCE_FILE);
      dacSample1.setName("dac-sample1.json");
      dacSample1.setPhysicalDataset(
          new PhysicalDataset().setFormatSettings(new JsonFileConfig().asFileConfig()));
      namespaceService.addOrUpdateDataset(dacSample1Path.toNamespaceKey(), dacSample1);
      // adding tags for table (physical dataset)
      List<String> tagList = Arrays.asList("tag3", "tag4");
      Tags newTags = new Tags(tagList, null);
      collaborationService.setTags(dacSample1.getId().getId(), newTags);
    }

    {
      PhysicalDatasetPath dacSample2Path =
          new PhysicalDatasetPath(asList("LocalFS2", "dac-sample2.json"));
      DatasetConfig dacSample2 = new DatasetConfig();
      dacSample2.setOwner(username);
      dacSample2.setCreatedAt(System.currentTimeMillis());
      dacSample2.setFullPathList(dacSample2Path.toPathList());
      dacSample2.setType(DatasetType.PHYSICAL_DATASET_SOURCE_FILE);
      dacSample2.setName("dac-sample2.json");
      dacSample2.setPhysicalDataset(
          new PhysicalDataset().setFormatSettings(new JsonFileConfig().asFileConfig()));
      namespaceService.addOrUpdateDataset(dacSample2Path.toNamespaceKey(), dacSample2);
      // adding empty tags for table (physical dataset)
      Tags newTags = new Tags(new ArrayList<>(), null);
      collaborationService.setTags(dacSample2.getId().getId(), newTags);
    }

    putDS("Prod-Sample", "ds1", new FromTable("LocalFS1.\"dac-sample1.json\"").wrap());
    putDS(
        "Prod-Sample",
        "ds2",
        new FromSQL("select * from LocalFS1.\"dac-sample1.json\" where age <= 20").wrap());

    putDS("Sales-Sample", "ds3", new FromTable("LocalFS2.\"dac-sample2.json\"").wrap());
    putDS(
        "Sales-Sample",
        "ds4",
        new FromSQL("select * from LocalFS2.\"dac-sample2.json\" where A >= 900").wrap());

    TestUtilities.addClasspathSourceIf(context.getCatalogService());

    putDS(
        "Tpch-Sample",
        "tpch20",
        new FromSQL(
                getFileContentsFromClassPath("queries/tpch/20.sql")
                    .replaceAll("\\-\\-.*", "")
                    .replace('`', '"')
                    .replace(';', ' '))
            .wrap());
    putDS(
        "Tpch-Sample",
        "tpch03",
        new FromSQL(
                getFileContentsFromClassPath("queries/tpch/03.sql")
                    .replaceAll("\\-\\-.*", "")
                    .replace('`', '"')
                    .replace(';', ' '))
            .wrap());

    // dataset dependency graph
    putDS("DG", "dsg1", new FromSQL("select * from LocalFS1.\"dac-sample1.json\"").wrap());
    putDS("DG", "dsg2", new FromSQL("select * from LocalFS2.\"dac-sample2.json\"").wrap());

    putDS("DG", "dsg3", new FromTable("DG.dsg1").wrap());
    putDS("DG", "dsg4", new FromTable("DG.dsg2").wrap());

    putDS("DG", "dsg5", new FromSQL("select * from DG.dsg3 where age >=900").wrap());
    putDS("DG", "dsg6", new FromSQL("select * from DG.dsg3 where age < 900").wrap());
    putDS(
        "DG",
        "dsg7",
        new FromSQL(
                "select age, dsg3.\"user\" as user1 from DG.dsg3 union select age, dsg5.\"user\" as user2 from DG.dsg5")
            .wrap());
    putDS(
        "DG",
        "dsg8",
        new FromSQL("select t1.age, t2.A from DG.dsg3 t1 inner join DG.dsg4 t2 on t1.age=t2.A")
            .wrap());
    putDS(
        "DG",
        "dsg9",
        new FromSQL("select age A, age B from DG.dsg3 union select A, B from DG.dsg2").wrap());
    putDS(
        "DG",
        "dsg10",
        new FromSQL("select * from DG.dsg9 t1 left join DG.dsg8 t2 on t1.A=t2.age").wrap());

    // adding tags for view (virtual dataset)
    DatasetConfig dsg3 = namespaceService.getDataset(new NamespaceKey(Arrays.asList("DG", "dsg3")));
    List<String> tagList = Arrays.asList("tag1", "tag2");
    Tags newTags = new Tags(tagList, null);
    collaborationService.setTags(dsg3.getId().getId(), newTags);

    DatasetConfig dsg4 = namespaceService.getDataset(new NamespaceKey(Arrays.asList("DG", "dsg4")));
    Tags emptyTags = new Tags(new ArrayList<>(), null);
    collaborationService.setTags(dsg4.getId().getId(), emptyTags);

    long t = sw.elapsed(MILLISECONDS);
    if (t > 100) {
      logger.warn(
          String.format(
              "com.dremio.dac.daemon.SampleDataPopulator.populateInitialData() took %dms", t));
    }
  }

  private static String getFileContentsFromClassPath(String resource) throws IOException {
    final URL url = Resources.getResource(resource);
    if (url == null) {
      throw new IOException(String.format("Unable to find path %s.", resource));
    }
    return Resources.toString(url, UTF_8);
  }

  private VirtualDatasetUI newDataset(
      DatasetPath datasetPath, DatasetVersion version, From from, List<String> sqlContext) {

    final VirtualDatasetUI ds =
        com.dremio.dac.explore.DatasetTool.newDatasetBeforeQueryMetadata(
            datasetPath, version, from, sqlContext, username, null, null);
    final SqlQuery query = new SqlQuery(ds.getSql(), ds.getState().getContextList(), username);
    ds.setLastTransform(
        new Transform(TransformType.createFromParent)
            .setTransformCreateFromParent(new TransformCreateFromParent(from)));
    final QueryMetadata metadata;

    try {
      Stopwatch sw = Stopwatch.createStarted();
      metadata = QueryParser.extract(query, context);
      long t = sw.elapsed(MILLISECONDS);
      if (t > 100) {
        logger.warn(
            String.format("parsing sql took %dms for %s:\n%s", t, ds.getName(), ds.getSql()));
      }
    } catch (RuntimeException e) {
      Throwables.propagateIfInstanceOf(e, UserException.class);
      throw new ServerErrorException(
          "Produced invalid SQL:\n" + ds.getSql() + "\n" + e.getMessage(), e);
    }

    final List<ViewFieldType> viewFieldTypes =
        Views.viewToFieldTypes(Views.relDataTypeToFieldType(metadata.getRowType()));
    QuerySemantics.populateSemanticFields(viewFieldTypes, ds.getState());
    DatasetTool.applyQueryMetadata(
        ds,
        metadata.getParents(),
        metadata.getBatchSchema(),
        metadata.getFieldOrigins(),
        metadata.getGrandParents(),
        JobsProtoUtil.toBuf(metadata));

    return ds;
  }

  @Override
  public void close() throws IOException {
    FileUtils.deleteDirectory(path.toFile());
  }
}
