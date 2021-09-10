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
package com.dremio.dac.daemon;

import static com.dremio.dac.service.datasets.DatasetDownloadManager.DATASET_DOWNLOAD_STORAGE_PLUGIN;
import static com.dremio.dac.support.SupportService.*;
import static com.dremio.exec.ExecConstants.METADATA_CLOUD_CACHING_ENABLED;
import static com.dremio.service.reflection.ReflectionOptions.CLOUD_CACHING_ENABLED;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ConcurrentModificationException;

import com.dremio.common.DeferredException;
import com.dremio.common.exceptions.UserException;
import com.dremio.config.DremioConfig;
import com.dremio.dac.homefiles.HomeFileConf;
import com.dremio.dac.homefiles.HomeFileSystemStoragePlugin;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemConf;
import com.dremio.exec.store.dfs.InternalFileConf;
import com.dremio.exec.store.dfs.MetadataStoragePluginConfig;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.options.TypeValidators;
import com.dremio.service.BindingProvider;
import com.dremio.service.DirectProvider;
import com.dremio.service.Initializer;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.ProjectConfig;
import com.dremio.service.coordinator.TaskLeaderElection;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.reflection.materialization.AccelerationStoragePluginConfig;
import com.google.common.annotations.VisibleForTesting;

/**
 * Create all the system storage plugins, such as results, accelerator, etc.
 * Also creates the backing directories for each of these plugins
 */
@SuppressWarnings("unused") // found through reflection search and executed by InitializerRegistry
public class SystemStoragePluginInitializer implements Initializer<Void> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SystemStoragePluginInitializer.class);

  private static final String LOCAL_TASK_LEADER_NAME = "plugininit";
  private static final int MAX_CACHE_SPACE_PERCENT = 100;

  @Override
  public Void initialize(BindingProvider provider) throws Exception {
    final SabotContext sabotContext = provider.lookup(SabotContext.class);
    boolean isDistributedCoordinator = sabotContext.getDremioConfig().isMasterlessEnabled()
      && sabotContext.getRoles().contains(ClusterCoordinator.Role.COORDINATOR);
    boolean isMaster = sabotContext.getRoles().contains(ClusterCoordinator.Role.MASTER);
    if (!(isMaster || isDistributedCoordinator)) {
      logger.debug("System storage plugins will be created only on master coordinator");
      return null;
    }

    if (!isMaster) {
      // masterless mode
      TaskLeaderElection taskLeaderElection = new TaskLeaderElection(
        LOCAL_TASK_LEADER_NAME,
        DirectProvider.wrap(sabotContext.getClusterCoordinator()),
        DirectProvider.wrap(sabotContext.getEndpoint()));

      taskLeaderElection.start();
      // waiting for the leader to show
      taskLeaderElection.getTaskLeader();

      if (taskLeaderElection.isTaskLeader()) {
        try {
          pluginsCreation(provider, sabotContext);
        } catch (Exception e) {
          logger.warn("Exception while trying to init system plugins. Let other node (if available) handle it");
          // close leader elections for this service
          // let others take over leadership - if they initialize later
          taskLeaderElection.close();
          throw e;
        }
      } else {
        logger.debug("System storage plugins will be created only on task leader coordinator");
      }
      return null;
    }

    pluginsCreation(provider, sabotContext);
    return null;
  }

  /**
   * To wrap plugins creation
   * @param provider
   * @param sabotContext
   * @throws Exception
   */
  private void pluginsCreation(final BindingProvider provider, final SabotContext sabotContext) throws Exception {
    final DremioConfig config = provider.lookup(DremioConfig.class);
    final CatalogService catalogService = provider.lookup(CatalogService.class);
    final NamespaceService ns = provider.lookup(SabotContext.class).getNamespaceService(SYSTEM_USERNAME);
    final DeferredException deferred = new DeferredException();
    final ProjectConfig projectConfig = provider.lookup(ProjectConfig.class);

    final Path supportPath = Paths.get(sabotContext.getOptionManager().getOption(TEMPORARY_SUPPORT_PATH));
    final Path logPath = Paths.get(System.getProperty(DREMIO_LOG_PATH_PROPERTY, "/var/log/dremio"));

    final ProjectConfig.DistPathConfig uploadsPathConfig = projectConfig.getUploadsConfig();
    final ProjectConfig.DistPathConfig accelerationPathConfig = projectConfig.getAcceleratorConfig();
    final ProjectConfig.DistPathConfig scratchPathConfig = projectConfig.getScratchConfig();
    final ProjectConfig.DistPathConfig metadataPathConfig = projectConfig.getMetadataConfig();
    final URI downloadPath = config.getURI(DremioConfig.DOWNLOADS_PATH_STRING);
    final URI resultsPath = config.getURI(DremioConfig.RESULTS_PATH_STRING);
    // Do not construct URI simply by concatenating, as it might not be encoded properly
    final URI logsPath = new URI("pdfs", "///" + logPath.toUri().getPath(), null);
    final URI supportURI = supportPath.toUri();

    final boolean enableAsyncForUploads = enable(config, DremioConfig.DEBUG_UPLOADS_ASYNC_ENABLED);
    createSafe(catalogService, ns,
      HomeFileConf.create(HomeFileSystemStoragePlugin.HOME_PLUGIN_NAME, uploadsPathConfig.getUri(), config.getThisNode(),
        SchemaMutability.USER_TABLE, CatalogService.NEVER_REFRESH_POLICY,
        enableAsyncForUploads, scratchPathConfig.getDataCredentials()), deferred);


    final int maxCacheSpacePercent = config.hasPath(DremioConfig.DEBUG_DIST_MAX_CACHE_SPACE_PERCENT)?
      config.getInt(DremioConfig.DEBUG_DIST_MAX_CACHE_SPACE_PERCENT) : MAX_CACHE_SPACE_PERCENT;

    final boolean enableAsyncForAcceleration = enable(config, DremioConfig.DEBUG_DIST_ASYNC_ENABLED);

    final boolean enableS3FileStatusCheck = isEnableS3FileStatusCheck(config, accelerationPathConfig);
    boolean enableCachingForAcceleration = isEnableCaching(sabotContext, config, accelerationPathConfig, CLOUD_CACHING_ENABLED);

    createSafe(catalogService, ns,
        AccelerationStoragePluginConfig.create(accelerationPathConfig.getUri(), enableAsyncForAcceleration,
            enableCachingForAcceleration, maxCacheSpacePercent, enableS3FileStatusCheck,
          accelerationPathConfig.getDataCredentials()), deferred);

    final boolean enableAsyncForJobs = enable(config, DremioConfig.DEBUG_JOBS_ASYNC_ENABLED);
    createSafe(catalogService, ns,
      InternalFileConf.create(DACDaemonModule.JOBS_STORAGEPLUGIN_NAME, resultsPath, SchemaMutability.SYSTEM_TABLE,
        CatalogService.DEFAULT_METADATA_POLICY_WITH_AUTO_PROMOTE, enableAsyncForJobs, null), deferred);

    final boolean enableAsyncForScratch = enable(config, DremioConfig.DEBUG_SCRATCH_ASYNC_ENABLED);
    createSafe(catalogService, ns,
      InternalFileConf.create(DACDaemonModule.SCRATCH_STORAGEPLUGIN_NAME, scratchPathConfig.getUri(), SchemaMutability.USER_TABLE,
        CatalogService.NEVER_REFRESH_POLICY_WITH_AUTO_PROMOTE, enableAsyncForScratch, scratchPathConfig.getDataCredentials()), deferred);

    final boolean enableAsyncForDownload = enable(config, DremioConfig.DEBUG_DOWNLOAD_ASYNC_ENABLED);
    createSafe(catalogService, ns,
      InternalFileConf.create(DATASET_DOWNLOAD_STORAGE_PLUGIN, downloadPath, SchemaMutability.USER_TABLE,
        CatalogService.NEVER_REFRESH_POLICY, enableAsyncForDownload, null), deferred);

    final boolean enableAsyncForMetadata = enable(config, DremioConfig.DEBUG_METADATA_ASYNC_ENABLED);

    final boolean enableS3FileStatusCheckForMetadata = isEnableS3FileStatusCheck(config, metadataPathConfig);
    boolean enableCachingForMetadata = isEnableCaching(sabotContext, config, metadataPathConfig, METADATA_CLOUD_CACHING_ENABLED);

    createSafe(catalogService, ns,
            MetadataStoragePluginConfig.create(metadataPathConfig.getUri(), enableAsyncForMetadata,
                    enableCachingForMetadata, maxCacheSpacePercent, enableS3FileStatusCheckForMetadata,
                    metadataPathConfig.getDataCredentials()), deferred);


    final boolean enableAsyncForLogs = enable(config, DremioConfig.DEBUG_LOGS_ASYNC_ENABLED);
    createSafe(catalogService, ns,
      InternalFileConf.create(LOGS_STORAGE_PLUGIN, logsPath, SchemaMutability.NONE,
        CatalogService.NEVER_REFRESH_POLICY, enableAsyncForLogs, null), deferred);

    final boolean enableAsyncForSupport = enable(config, DremioConfig.DEBUG_SUPPORT_ASYNC_ENABLED);
    createSafe(catalogService, ns,
      InternalFileConf.create(LOCAL_STORAGE_PLUGIN, supportURI, SchemaMutability.SYSTEM_TABLE,
        CatalogService.NEVER_REFRESH_POLICY, enableAsyncForSupport, null), deferred);

    deferred.throwAndClear();
  }

  private boolean isEnableCaching(SabotContext sabotContext, DremioConfig config,
                                  ProjectConfig.DistPathConfig pathConfig,
                                  TypeValidators.BooleanValidator optionValidator) {
    boolean enableCachingForAcceleration = enable(config, DremioConfig.DEBUG_DIST_CACHING_ENABLED);
    if (FileSystemConf.isCloudFileSystemScheme(pathConfig.getUri().getScheme())) {
      enableCachingForAcceleration = sabotContext.getOptionManager().getOption(optionValidator) ;
    }
    return enableCachingForAcceleration;
  }

  private boolean isEnableS3FileStatusCheck(DremioConfig config, ProjectConfig.DistPathConfig pathConfig) {
    return !FileSystemConf.CloudFileSystemScheme.S3_FILE_SYSTEM_SCHEME.getScheme().equals(pathConfig.getUri().getScheme())
            || enable(config, DremioConfig.DEBUG_DIST_S3_FILE_STATUS_CHECK);
  }

  private static boolean enable(DremioConfig config, String path) {
    return !config.hasPath(path) || config.getBoolean(path);
  }

  private void createSafe(final CatalogService catalogService, final NamespaceService ns, final
      SourceConfig config, DeferredException deferred) {
    try {
      createOrUpdateSystemSource(catalogService, ns, config);
    } catch (Exception ex) {
      deferred.addException(ex);
    }
  }

  /**
   * Create provided source if does not exist
   * or update if does exist
   * used for Such internal sources that can change based on the external configuration
   * such as hdfs to pdfs, directory structures
   * @param catalogService
   * @param ns
   * @param config
   */
  @VisibleForTesting
  void createOrUpdateSystemSource(final CatalogService catalogService, final NamespaceService ns, final
  SourceConfig config) throws Exception {
    try {
      config.setAllowCrossSourceSelection(true);
      final boolean isCreated = catalogService.createSourceIfMissingWithThrow(config);
      if (isCreated) {
        return;
      }
    } catch (ConcurrentModificationException ex) {
      // someone else got there first, ignore this failure.
      logger.info(ex.getMessage(), ex);
      // proceed with update
    } catch (UserException ex) {
      if (ex.getErrorType() != UserBitShared.DremioPBError.ErrorType.CONCURRENT_MODIFICATION) {
        throw ex;
      }
      // someone else got there first, ignore this failure.
      logger.info(ex.getMessage(), ex);
    }
    final NamespaceKey nsKey = new NamespaceKey(config.getName());
    final SourceConfig oldConfig = ns.getSource(nsKey);
    final SourceConfig updatedConfig = config;
    // make incoming config match existing config to be used in comparison
    updatedConfig
      .setId(oldConfig.getId())
      .setCtime(oldConfig.getCtime())
      .setTag(oldConfig.getTag())
      .setConfigOrdinal(oldConfig.getConfigOrdinal());
    // if old and new configs match don't update
    if (oldConfig.equals(updatedConfig)) {
      return;
    }
    ((CatalogServiceImpl) catalogService).getSystemUserCatalog().updateSource(updatedConfig);
  }
}
