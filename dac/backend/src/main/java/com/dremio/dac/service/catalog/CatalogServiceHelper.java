/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.dac.service.catalog;

import static com.dremio.dac.util.DatasetsUtil.toDatasetConfig;
import static com.dremio.service.namespace.dataset.proto.DatasetType.VIRTUAL_DATASET;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Objects;

import javax.inject.Inject;
import javax.ws.rs.core.SecurityContext;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.dac.api.CatalogEntity;
import com.dremio.dac.api.CatalogItem;
import com.dremio.dac.api.Dataset;
import com.dremio.dac.api.File;
import com.dremio.dac.api.Folder;
import com.dremio.dac.api.Home;
import com.dremio.dac.api.Source;
import com.dremio.dac.api.Space;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.homefiles.HomeFileSystemStoragePlugin;
import com.dremio.dac.homefiles.HomeFileTool;
import com.dremio.dac.model.common.NamespacePath;
import com.dremio.dac.model.common.NamespacePathUtils;
import com.dremio.dac.model.sources.PhysicalDatasetPath;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.model.spaces.HomePath;
import com.dremio.dac.model.spaces.SpaceName;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.dac.service.source.SourceService;
import com.dremio.dac.util.DatasetsUtil;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaEntity;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.physicaldataset.proto.PhysicalDatasetConfig;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Catalog Service Helper
 *
 * A helper that allows interacting with the Dremio catalog.  Allows browsing and created/editing/deleting of sources,
 * spaces, datasets, files and folders where allowed.
 *
 */
public class CatalogServiceHelper {
  private static final Logger logger = LoggerFactory.getLogger(CatalogServiceHelper.class);

  private final Catalog catalog;
  private final SecurityContext context;
  private final SourceService sourceService;
  private final NamespaceService namespaceService;
  private final SabotContext sabotContext;
  private final ReflectionServiceHelper reflectionServiceHelper;
  private final HomeFileTool homeFileTool;
  private final DatasetVersionMutator datasetVersionMutator;

  @Inject
  public CatalogServiceHelper(Catalog catalog, SecurityContext context, SourceService sourceService, NamespaceService namespaceService, SabotContext sabotContext, ReflectionServiceHelper reflectionServiceHelper, HomeFileTool homeFileTool, DatasetVersionMutator datasetVersionMutator) {
    this.catalog = catalog;
    this.context = context;
    this.sourceService = sourceService;
    this.namespaceService = namespaceService;
    this.sabotContext = sabotContext;
    this.reflectionServiceHelper = reflectionServiceHelper;
    this.homeFileTool = homeFileTool;
    this.datasetVersionMutator = datasetVersionMutator;
  }

  public Optional<DatasetConfig> getDatasetById(String datasetId) {
    DremioTable table = catalog.getTable(datasetId);

    if (table == null) {
      return Optional.absent();
    }

    return Optional.fromNullable(table.getDatasetConfig());
  }

  private HomeConfig getHomeForCurrentUser() throws NamespaceException {
    HomePath homePath = new HomePath(HomeName.getUserHomePath(context.getUserPrincipal().getName()));

    return namespaceService.getHome(homePath.toNamespaceKey());
  }

  public List<CatalogItem> getTopLevelCatalogItems() {
    List<CatalogItem> topLevelItems = new ArrayList<>();

    try {
      HomeConfig homeForCurrentUser = getHomeForCurrentUser();
      topLevelItems.add(CatalogItem.fromHomeConfig(homeForCurrentUser));
    } catch (NamespaceException e) {
      // if for some reason we can't find a home space, log it but keep going
      logger.warn("Failed to find home space for user [{}]", context.getUserPrincipal().getName());
    }

    for (SpaceConfig spaceConfig : namespaceService.getSpaces()) {
      topLevelItems.add(CatalogItem.fromSpaceConfig(spaceConfig));
    }

    for (SourceConfig sourceConfig : sourceService.getSources()) {
      topLevelItems.add(CatalogItem.fromSourceConfig(sourceConfig));
    }

    return topLevelItems;
  }

  private NameSpaceContainer getNamespaceEntity(NamespaceKey namespaceKey) throws NamespaceException {
    return namespaceService.getEntities(Collections.singletonList(namespaceKey)).get(0);
  }

  public Optional<CatalogEntity> getCatalogEntityById(String id) throws NamespaceException {
    Optional<?> entity = getById(id);

    if (!entity.isPresent()) {
      return Optional.absent();
    }

    Object object = entity.get();

    if (object instanceof SourceConfig) {
      SourceConfig config = (SourceConfig) object;

      Source source = sourceService.fromSourceConfig(config, getChildrenForPath(new NamespaceKey(config.getName())));
      return Optional.of((CatalogEntity) source);
    } else if (object instanceof SpaceConfig) {
      SpaceConfig config = (SpaceConfig) object;

      Space space = getSpaceFromConfig(config, getChildrenForPath(new NamespaceKey(config.getName())));
      return Optional.of((CatalogEntity) space);
    } else if (object instanceof DatasetConfig) {
      DatasetConfig config = (DatasetConfig) object;

      Dataset dataset;

      // only set acceleration settings if one exists in the store - we don't want inherited settings
      Optional<AccelerationSettings> settings = getStoredReflectionSettingsForDataset(config);
      if (settings.isPresent()) {
        dataset = getDatasetFromConfig(config, new Dataset.RefreshSettings(settings.get()));
      } else {
        dataset = getDatasetFromConfig(config, null);
      }

      return Optional.of((CatalogEntity) dataset);
    } else if (object instanceof HomeConfig) {
      HomeConfig config = (HomeConfig) object;

      Home home = getHomeFromConfig(config, getChildrenForPath(new NamespaceKey(HomeName.getUserHomePath(config.getOwner()).getName())));
      return Optional.of((CatalogEntity) home);
    } else if (object instanceof FolderConfig) {
      FolderConfig config = (FolderConfig) object;

      Folder folder = getFolderFromConfig(config, getChildrenForPath(new NamespaceKey(config.getFullPathList())));
      return Optional.of((CatalogEntity) folder);
    } else if (object instanceof CatalogEntity) {
      // this is something not in the namespace, a file/folder from a filesystem source
      CatalogEntity catalogEntity = (CatalogEntity) object;
      return Optional.of(catalogEntity);
    } else {
      throw new IllegalArgumentException(String.format("Unexpected catalog type found [%s] with id [%s].", object.getClass().getName(), id));
    }
  }

  /**
   * Given an id, retrieves the entity from the namespace.  Also handles fake ids (using generateInternalId) that we
   * generate for folders/files that exist in file-based sources that are not in the namespace.
   *
   * Note: this returns the namespace object (DatasetConfig, etc) for entites found in the namespace.  For non-namespace
   * items it returns the appropriate CatalogEntity item (Folder/File only).
   */
  private Optional<?> getById(String id) {
    try {
      if (isInternalId(id)) {
        CatalogItem catalogItem = getInternalItemByPath(getPathFromInternalId(id));

        // can either be a folder or a file
        if (catalogItem.getContainerType() == CatalogItem.ContainerSubType.FOLDER) {
          Folder folder = new Folder(catalogItem.getId(), catalogItem.getPath(), null, getListingForInternalItem(getPathFromInternalId(id)));
          return Optional.of((Object) folder);
        } else if (catalogItem.getType() == CatalogItem.CatalogItemType.FILE) {
          File file = new File(catalogItem.getId(), catalogItem.getPath());
          return Optional.of((Object) file);
        }

        throw new RuntimeException(String.format("Could not retrieve internal item [%s]", catalogItem.toString()));
      } else {
        NameSpaceContainer entity = namespaceService.getEntityById(id);

        if (entity == null) {
          // if we can't find it by id, maybe its not in the namespace
          logger.warn("Could not find entity with id [{}]", id);
          return Optional.absent();
        }

        Optional result = Optional.absent();

        switch (entity.getType()) {
          case SOURCE: {
            result = Optional.of(entity.getSource());
            break;
          }

          case SPACE: {
            result = Optional.of(entity.getSpace());
            break;
          }

          case DATASET: {
            // for datasets go to the catalog to ensure we have schema.
            result = Optional.of(catalog.getTable(id).getDatasetConfig());
            break;
          }

          case HOME: {
            result = Optional.of(entity.getHome());
            break;
          }

          case FOLDER: {
            result = Optional.of(entity.getFolder());
            break;
          }

          default: {
            throw new RuntimeException(String.format("Unsupported namespace entity with id [%s] of type [%s]", id, entity.getType()));
          }
        }

        return result;
      }
    } catch (NamespaceException e) {
      logger.warn("Failed to get entity ", e);
      return Optional.absent();
    }
  }

  private List<CatalogItem> getListingForInternalItem(List<String> path) throws NamespaceException {
    NameSpaceContainer rootEntity = getNamespaceEntity(new NamespaceKey(path.get(0)));

    if (rootEntity.getType() == NameSpaceContainer.Type.SOURCE) {
      return getChildrenForPath(new NamespaceKey(path));
    }

    throw new IllegalArgumentException(String.format("Can only get listing for sources, but [%s] is of type [%s]", path, rootEntity.getType()));
  }

  private CatalogItem getInternalItemByPath(List<String> path) throws NamespaceException {
    NameSpaceContainer rootEntity = getNamespaceEntity(new NamespaceKey(path.get(0)));

    if (rootEntity.getType() == NameSpaceContainer.Type.SOURCE) {
      return getInternalItemFromSource(rootEntity.getSource(), path);
    } else {
      throw new IllegalArgumentException(String.format("Can not get internal item from a non-source [%s] container.", rootEntity.getType()));
    }
  }

  private CatalogItem getInternalItemFromSource(SourceConfig sourceConfig, List<String> path) {
    final StoragePlugin plugin = checkNotNull(catalog.getSource(sourceConfig.getName()), "storage plugin %s not found", sourceConfig.getName());

    if (!(plugin instanceof FileSystemPlugin)) {
      throw new IllegalArgumentException(String.format("Can not get internal item from non-filesystem source [%s] of type [%s]", sourceConfig.getName(), plugin.getClass().getName()));
    }

    SchemaEntity entity = ((FileSystemPlugin) plugin).get(path, context.getUserPrincipal().getName());

    return convertSchemaEntityToCatalogItem(entity, path.subList(0, path.size() - 1));
  }

  private CatalogItem convertSchemaEntityToCatalogItem(SchemaEntity entity, List<String> parentPath) {
    List<String> entityPath = Lists.newArrayList(parentPath);
    entityPath.add(entity.getPath());

    CatalogItem catalogItem = null;

    switch(entity.getType()) {
      case FILE: {
        catalogItem = new CatalogItem(generateInternalId(entityPath), entityPath, null, CatalogItem.CatalogItemType.FILE, null, null);
        break;
      }

      case FOLDER: {
        catalogItem = new CatalogItem(generateInternalId(entityPath), entityPath, null, CatalogItem.CatalogItemType.CONTAINER, null, CatalogItem.ContainerSubType.FOLDER);
        break;
      }

      case FILE_TABLE:
      case FOLDER_TABLE: {
        // We skip promoted schema entities as they will be retrieved separately from the catalog
        break;
      }

      default: {
        throw new RuntimeException(String.format("Trying to convert unexpected schema entity [%s] of type [%s].", entity.getPath(), entity.getType()));
      }
    }

    return catalogItem;
  }

  @VisibleForTesting
  public List<CatalogItem> getChildrenForPath(NamespaceKey path) throws NamespaceException {
    List<CatalogItem> catalogItems = new ArrayList<>();

    // get parent info
    NameSpaceContainer rootEntity = getNamespaceEntity(new NamespaceKey(path.getPathComponents().get(0)));

    if (rootEntity.getType() == NameSpaceContainer.Type.SOURCE) {
      // get non-namespace entities directly from the source
      catalogItems.addAll(getNonNamespaceChildrenForSourcePath(rootEntity.getSource().getName(), path.getPathComponents()));
    }

    try {
      List<NameSpaceContainer> list = namespaceService.list(path);

      for (NameSpaceContainer container : list) {
        Optional<CatalogItem> item = CatalogItem.fromNamespaceContainer(container);
        if (item.isPresent()) {
          catalogItems.add(item.get());
        }
      }
    } catch (NamespaceException e) {
      logger.warn(e.getMessage());
    }

    return catalogItems;
  }

  /**
   *  Returns all children of the listingPath that are not in the namespace - so all items that are in filesystem based
   *  and are not datasets.
   */
  private List<CatalogItem> getNonNamespaceChildrenForSourcePath(String sourceName, List<String> listingPath) {
    List<CatalogItem> catalogItems = new ArrayList<>();

    final StoragePlugin plugin = checkNotNull(catalog.getSource(sourceName), "storage plugin %s not found", sourceName);
    if (plugin instanceof FileSystemPlugin) {
      List<SchemaEntity> list = ((FileSystemPlugin) plugin).list(listingPath, context.getUserPrincipal().getName());

      for (SchemaEntity entity : list) {
        CatalogItem catalogItem = convertSchemaEntityToCatalogItem(entity, listingPath);

        if (catalogItem != null) {
          catalogItems.add(catalogItem);
        }
      }
    }

    return catalogItems;
  }

  public CatalogEntity createCatalogItem(CatalogEntity entity) throws NamespaceException, UnsupportedOperationException, ExecutionSetupException {
    if (entity instanceof Space) {
      Space space = (Space) entity;
      return createSpace(space);
    } else if (entity instanceof Source) {
      return createSource((Source) entity);
    } else if (entity instanceof Dataset) {
      Dataset dataset = (Dataset) entity;
      return createDataset(dataset);
    } else if (entity instanceof Folder) {
      try {
        return createFolder((Folder) entity);
      } catch (UserException e) {
        throw new ConcurrentModificationException(e);
      }
    } else {
      throw new UnsupportedOperationException(String.format("Catalog item of type [%s] can not be edited", entity.getClass().getName()));
    }
  }

  private CatalogEntity createDataset(Dataset dataset) throws NamespaceException {
    validateDataset(dataset);

    // only handle VDS
    Preconditions.checkArgument(dataset.getType() != Dataset.DatasetType.PHYSICAL_DATASET, "Phyiscal Datasets can only be created by promoting other entities.");

    Preconditions.checkArgument(dataset.getId() == null, "Dataset id is immutable.");

    // verify we can save
    NamespaceKey topLevelKey = new NamespaceKey(dataset.getPath().get(0));
    NamespaceKey parentNamespaceKey = new NamespaceKey(dataset.getPath().subList(0, dataset.getPath().size() - 1));
    NamespaceKey namespaceKey = new NamespaceKey(dataset.getPath());
    Preconditions.checkArgument(namespaceService.exists(parentNamespaceKey), String.format("Dataset parent path [%s] doesn't exist.", parentNamespaceKey.toString()));

    // can only create VDs in a space or home
    NameSpaceContainer entity = getNamespaceEntity(topLevelKey);
    List<NameSpaceContainer.Type> types = Arrays.asList(NameSpaceContainer.Type.SPACE, NameSpaceContainer.Type.HOME);
    Preconditions.checkArgument(types.contains(entity.getType()), "Virtual datasets can only be saved into spaces or home space.");

    sabotContext.getViewCreator(context.getUserPrincipal().getName()).createView(dataset.getPath(), dataset.getSql(), dataset.getSqlContext());

    DatasetConfig created = namespaceService.getDataset(namespaceKey);

    return getDatasetFromConfig(created, null);
  }

  /**
   * Promotes the target to a PDS using the formatting options submitted via dataset.
   */
  public Dataset promoteToDataset(String targetId, Dataset dataset) throws NamespaceException, UnsupportedOperationException {
    Preconditions.checkArgument(dataset.getType() == Dataset.DatasetType.PHYSICAL_DATASET, "Promoting can only create physical datasets.");

    // verify we can promote the target entity
    List<String> path = getPathFromInternalId(targetId);
    Preconditions.checkArgument(CollectionUtils.isEqualCollection(path, dataset.getPath()), "Entity id does not match the path specified in the dataset.");

    // validation
    validateDataset(dataset);
    Preconditions.checkArgument(dataset.getFormat() != null, "To promote a dataset, format settings are required.");

    NamespaceKey namespaceKey = new NamespaceKey(path);
    CatalogItem catalogItem = getInternalItemByPath(path);

    // can only promote a file or folder from a source (which getInternalItemByPath verifies)
    if (catalogItem.getContainerType() == CatalogItem.ContainerSubType.FOLDER || catalogItem.getType() == CatalogItem.CatalogItemType.FILE) {
      PhysicalDatasetConfig physicalDatasetConfig = new PhysicalDatasetConfig();
      physicalDatasetConfig.setName(namespaceKey.getName());
      physicalDatasetConfig.setFormatSettings(dataset.getFormat().asFileConfig());

      if (catalogItem.getContainerType() == CatalogItem.ContainerSubType.FOLDER) {
        physicalDatasetConfig.setType(com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER);
      } else {
        physicalDatasetConfig.setType(com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET_SOURCE_FILE);
      }
      physicalDatasetConfig.setFullPathList(path);

      catalog.createOrUpdateDataset(namespaceService, new NamespaceKey(namespaceKey.getRoot()),
        new PhysicalDatasetPath(path).toNamespaceKey(), toDatasetConfig(physicalDatasetConfig, null));
    } else {
      throw new UnsupportedOperationException(String.format("Can only promote a folder or a file but found [%s]", catalogItem.getType()));
    }

    return getDatasetFromConfig(namespaceService.getDataset(namespaceKey), null);
  }

  private void updateDataset(Dataset dataset) throws NamespaceException {
    Preconditions.checkArgument(dataset.getId() != null, "Dataset Id is missing.");

    DatasetConfig currentDatasetConfig = namespaceService.findDatasetByUUID(dataset.getId());
    if (currentDatasetConfig == null) {
      throw new IllegalArgumentException(String.format("Could not find dataset with id [%s]", dataset.getId()));
    }

    validateDataset(dataset);

    // use the version of the dataset to check for concurrency issues
    currentDatasetConfig.setVersion(Long.valueOf(dataset.getTag()));

    NamespaceKey namespaceKey = new NamespaceKey(dataset.getPath());
    NamespacePath path = NamespacePathUtils.getNamespacePathForDataType(currentDatasetConfig.getType(), currentDatasetConfig.getFullPathList());

    // check type
    if (dataset.getType() == Dataset.DatasetType.PHYSICAL_DATASET) {
      // cannot change the path of a physical dataset
      Preconditions.checkArgument(CollectionUtils.isEqualCollection(dataset.getPath(), currentDatasetConfig.getFullPathList()), "Dataset path can not be modified.");

      Preconditions.checkArgument( currentDatasetConfig.getType() != VIRTUAL_DATASET, "Dataset type can not be modified");

      if (currentDatasetConfig.getType() == com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET_HOME_FILE) {
        DatasetConfig datasetConfig = toDatasetConfig(dataset.getFormat().asFileConfig(), currentDatasetConfig.getType(),
          context.getUserPrincipal().getName(), currentDatasetConfig.getId());

        catalog.createOrUpdateDataset(namespaceService, new NamespaceKey(HomeFileSystemStoragePlugin.HOME_PLUGIN_NAME), namespaceKey, datasetConfig);
      } else if (currentDatasetConfig.getType() == com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET_SOURCE_FILE || currentDatasetConfig.getType() == com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER) {
        Preconditions.checkArgument(dataset.getFormat() != null, "Promoted dataset needs to have a format set.");

        //DatasetConfig datasetConfig = toDatasetConfig(dataset.getFormat().asFileConfig(), currentDatasetConfig.getType(), context.getUserPrincipal().getName(), currentDatasetConfig.getId());
        // only thing that can change is the formatting
        currentDatasetConfig.getPhysicalDataset().setFormatSettings(dataset.getFormat().asFileConfig());

        catalog.createOrUpdateDataset(namespaceService, new NamespaceKey(HomeFileSystemStoragePlugin.HOME_PLUGIN_NAME), namespaceKey, currentDatasetConfig);
      }

      // update refresh settings
      Optional<AccelerationSettings> storedReflectionSettingsForDataset = getStoredReflectionSettingsForDataset(currentDatasetConfig);
      if (dataset.getAccelerationRefreshPolicy() == null && storedReflectionSettingsForDataset.isPresent()) {
        // we are clearing the acceleration settings for the dataset
        reflectionServiceHelper.getReflectionSettings().removeSettings(namespaceKey);
      } else if (dataset.getAccelerationRefreshPolicy() != null){
        reflectionServiceHelper.getReflectionSettings().setReflectionSettings(namespaceKey, dataset.getAccelerationRefreshPolicy().toAccelerationSettings());
      }
    } else if (dataset.getType() == Dataset.DatasetType.VIRTUAL_DATASET) {
      Preconditions.checkArgument(currentDatasetConfig.getType() == VIRTUAL_DATASET, "Dataset type can not be modified");

      // Check if the dataset is being renamed
      if (!Objects.equals(currentDatasetConfig.getFullPathList(), dataset.getPath())) {
        datasetVersionMutator.renameDataset(new DatasetPath(currentDatasetConfig.getFullPathList()), new DatasetPath(dataset.getPath()));
        currentDatasetConfig = namespaceService.getDataset(namespaceKey);
      }

      VirtualDataset virtualDataset = currentDatasetConfig.getVirtualDataset();
      // Only update if the sql or context change
      if (!Objects.equals(virtualDataset.getSql(), dataset.getSql()) ||
          !Objects.equals(virtualDataset.getContextList(), dataset.getSqlContext())) {
        virtualDataset.setSql(dataset.getSql());
        virtualDataset.setContextList(dataset.getSqlContext());
        currentDatasetConfig.setVirtualDataset(virtualDataset);

        namespaceService.addOrUpdateDataset(path.toNamespaceKey(), currentDatasetConfig);
      }
    }
  }

  private void deleteDataset(DatasetConfig config, String tag) throws NamespaceException, UnsupportedOperationException, IOException {
    // if no tag is passed in, use the latest version
    long version = config.getVersion();

    if (tag != null) {
      version = Long.parseLong(tag);
    }

    switch (config.getType()) {
      case PHYSICAL_DATASET: {
        throw new UnsupportedOperationException("A physical dataset can not be deleted.");
      }

      case PHYSICAL_DATASET_SOURCE_FILE:
      case PHYSICAL_DATASET_SOURCE_FOLDER: {
        // remove the formatting
        removeFormatFromDataset(config, version);
        break;
      }

      case PHYSICAL_DATASET_HOME_FILE:
      case PHYSICAL_DATASET_HOME_FOLDER: {
        deleteHomeDataset(config, version);
        break;
      }

      case VIRTUAL_DATASET: {
        namespaceService.deleteDataset(new NamespaceKey(config.getFullPathList()), version);
        break;
      }

      default: {
        throw new RuntimeException(String.format("Dataset [%s] of unknown type [%s] found.", config.getId().getId(), config.getType()));
      }
    }
  }

  public void deleteHomeDataset(DatasetConfig config, long version) throws IOException, NamespaceException {
    FileConfig formatSettings = config.getPhysicalDataset().getFormatSettings();
    homeFileTool.deleteFile(formatSettings.getLocation());
    namespaceService.deleteDataset(new NamespaceKey(config.getFullPathList()), version);
  }

  public void removeFormatFromDataset(DatasetConfig config, long version) {
    Iterable<ReflectionGoal> reflections = reflectionServiceHelper.getReflectionsForDataset(config.getId().getId());
    for (ReflectionGoal reflection : reflections) {
      reflectionServiceHelper.removeReflection(reflection.getId().getId());
    }

    PhysicalDatasetPath datasetPath = new PhysicalDatasetPath(config.getFullPathList());

    sourceService.deletePhysicalDataset(datasetPath.getSourceName(), datasetPath, version);
  }

  private void validateDataset(Dataset dataset) {
    Preconditions.checkArgument(dataset.getType() != null, "Dataset type is required.");
    Preconditions.checkArgument(dataset.getPath() != null, "Dataset path is required.");

    if (dataset.getType() == Dataset.DatasetType.VIRTUAL_DATASET) {
      // VDS requires sql
      Preconditions.checkArgument(dataset.getSql() != null, "Virtual dataset must have sql defined.");
      Preconditions.checkArgument(dataset.getSql().trim().length() > 0, "Virtual dataset cannot have empty sql defined.");
      Preconditions.checkArgument(dataset.getFormat() == null, "Virtual dataset cannot have a format defined.");
    } else {
      // PDS
      Preconditions.checkArgument(dataset.getSql() == null, "Physical dataset can not have sql defined.");
      Preconditions.checkArgument(dataset.getSqlContext() == null, "Physical dataset can not have sql context defined.");
    }
  }

  private CatalogEntity createSpace(Space space) throws NamespaceException {
    String spaceName = space.getName();

    Preconditions.checkArgument(space.getId() == null, "Space id is immutable.");
    Preconditions.checkArgument(spaceName != null, "Space name is required.");
    Preconditions.checkArgument(spaceName.trim().length() > 0, "Space name cannot be empty.");

    // TODO: move the space name validation somewhere reusable instead of having to create a new SpaceName
    new SpaceName(spaceName);

    NamespaceKey namespaceKey = new NamespaceKey(spaceName);

    // check if space already exists with the given name.
    if (namespaceService.exists(namespaceKey, NameSpaceContainer.Type.SPACE)) {
      throw new ConcurrentModificationException(String.format("A space with the name [%s] already exists.", spaceName));
    }

    namespaceService.addOrUpdateSpace(namespaceKey, getSpaceConfig(space));

    return getSpaceFromConfig(namespaceService.getSpace(namespaceKey), null);
  }

  private CatalogEntity createSource(Source source) throws NamespaceException, ExecutionSetupException {
    SourceConfig sourceConfig = sourceService.createSource(source.toSourceConfig());
    return sourceService.fromSourceConfig(sourceConfig, getChildrenForPath(new NamespaceKey(sourceConfig.getName())));
  }

  public CatalogEntity updateCatalogItem(CatalogEntity entity, String id) throws NamespaceException, UnsupportedOperationException, ExecutionSetupException {
    if (entity instanceof Dataset) {
      Dataset dataset = (Dataset) entity;
      Preconditions.checkArgument(dataset.getId().equals(id), "Ids must match.");

      updateDataset(dataset);
    } else if (entity instanceof Source) {
      Source source = (Source) entity;
      sourceService.updateSource(id, source.toSourceConfig());
    } else {
      throw new UnsupportedOperationException(String.format("Catalog item [%s] of type [%s] can not be edited.", id, entity.getClass().getName()));
    }

    Optional<CatalogEntity> newEntity = getCatalogEntityById(id);

    if (newEntity.isPresent()) {
      return newEntity.get();
    } else {
      throw new RuntimeException(String.format("Catalog item [%s] of type [%s] could not be found", id, entity.getClass().getName()));
    }
  }

  public void deleteCatalogItem(String id, String tag) throws NamespaceException, UnsupportedOperationException {
    Optional<?> entity = getById(id);

    if (!entity.isPresent()) {
      throw new IllegalArgumentException(String.format("Could not find entity with id [%s].", id));
    }

    Object object = entity.get();

    if (object instanceof SourceConfig) {
      SourceConfig config = (SourceConfig) object;

      if (tag != null) {
        config.setVersion(Long.valueOf(tag));
      }

      sourceService.deleteSource(config);
    } else if (object instanceof SpaceConfig) {
      SpaceConfig config = (SpaceConfig) object;

      long version = config.getVersion();

      if (tag != null) {
        version = Long.parseLong(tag);
      }

      namespaceService.deleteSpace(new NamespaceKey(config.getName()), version);
    } else if (object instanceof DatasetConfig) {
      DatasetConfig config = (DatasetConfig) object;

      try {
        deleteDataset(config, tag);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    } else if (object instanceof FolderConfig) {
      FolderConfig config = (FolderConfig) object;

      long version = config.getVersion();

      if (tag != null) {
        version = Long.parseLong(tag);
      }

      namespaceService.deleteFolder(new NamespaceKey(config.getFullPathList()), version);
    } else {
      throw new UnsupportedOperationException(String.format("Catalog item [%s] of type [%s] can not be deleted.", id, object.getClass().getName()));
    }
  }

  private Folder createFolder(Folder folder) throws NamespaceException {
    NamespaceKey parentKey = new NamespaceKey(folder.getPath().subList(0, folder.getPath().size() - 1));
    List<NameSpaceContainer> entities = namespaceService.getEntities(Collections.singletonList(parentKey));

    NameSpaceContainer container = entities.get(0);

    if (container == null) {
      // if we can't find it by id, maybe its not in the namespace
      throw new IllegalArgumentException(String.format("Could not find entity with path [%s].", folder.getPath()));
    }

    NamespaceKey key = new NamespaceKey(folder.getPath());

    switch (container.getType()) {
      case SPACE:
      case HOME:
      case FOLDER: {
        namespaceService.addOrUpdateFolder(key, getFolderConfig(folder));
        break;
      }

      default: {
        throw new UnsupportedOperationException(String.format("Can not create a folder inside a [%s].", container.getType()));
      }
    }

    return getFolderFromConfig(namespaceService.getFolder(key), null);
  }

  /**
   *  Refresh a catalog item.  Only supports datasets currently.
   */
  public void refreshCatalogItem(String id) throws UnsupportedOperationException {
    Optional<?> entity = getById(id);

    if (!entity.isPresent()) {
      throw new IllegalArgumentException(String.format("Could not find entity with id [%s].", id));
    }

    Object object = entity.get();

    if (object instanceof DatasetConfig) {
      reflectionServiceHelper.refreshReflectionsForDataset(id);
    } else {
      throw new UnsupportedOperationException(String.format("Can only refresh datasets but found [%s].", object.getClass().getName()));
    }
  }

  private Optional<AccelerationSettings> getStoredReflectionSettingsForDataset(DatasetConfig datasetConfig) {
    return reflectionServiceHelper.getReflectionSettings().getStoredReflectionSettings(new NamespaceKey(datasetConfig.getFullPathList()));
  }

  public static Dataset getDatasetFromConfig(DatasetConfig config, Dataset.RefreshSettings refreshSettings) {
    if (config.getType() == VIRTUAL_DATASET) {
      String sql = null;
      List<String> sqlContext = null;

      VirtualDataset virtualDataset = config.getVirtualDataset();
      if (virtualDataset != null) {
        sql = virtualDataset.getSql();
        sqlContext = virtualDataset.getContextList();
      }

      return new Dataset(
        config.getId().getId(),
        Dataset.DatasetType.VIRTUAL_DATASET,
        config.getFullPathList(),
        DatasetsUtil.getArrowFieldsFromDatasetConfig(config),
        config.getCreatedAt(),
        String.valueOf(config.getVersion()),
        refreshSettings,
        sql,
        sqlContext,
        null
      );
    } else {
      FileFormat format = null;

      PhysicalDataset physicalDataset = config.getPhysicalDataset();
      if (physicalDataset != null) {
        FileConfig formatSettings = physicalDataset.getFormatSettings();

        if (formatSettings != null) {
          if (config.getType() == com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET_SOURCE_FILE ||
            config.getType() == com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET_HOME_FILE) {
            format = FileFormat.getForFile(formatSettings);
          } else {
            format = FileFormat.getForFolder(formatSettings);
          }
        }
      }
      return new Dataset(
        config.getId().getId(),
        Dataset.DatasetType.PHYSICAL_DATASET,
        config.getFullPathList(),
        DatasetsUtil.getArrowFieldsFromDatasetConfig(config),
        config.getCreatedAt(),
        String.valueOf(config.getVersion()),
        refreshSettings,
        null,
        null,
        format
      );
    }
  }

  private static Home getHomeFromConfig(HomeConfig config, List<CatalogItem> children) {
    return new Home(
      config.getId().getId(),
      HomeName.getUserHomePath(config.getOwner()).toString(),
      String.valueOf(config.getVersion()),
      children
    );
  }

  private static Space getSpaceFromConfig(SpaceConfig config, List<CatalogItem> children) {
    return new Space(
      config.getId().getId(),
      config.getName(),
      String.valueOf(config.getVersion()),
      config.getCtime(),
      children
    );
  }

  public static SpaceConfig getSpaceConfig(Space space) {
    SpaceConfig config = new SpaceConfig();
    config.setName(space.getName());
    config.setId(new EntityId(space.getId()));
    if (space.getTag() != null) {
      config.setVersion(Long.valueOf(space.getTag()));
    }
    config.setCtime(space.getCreatedAt());

    return config;
  }

  private static Folder getFolderFromConfig(FolderConfig config, List<CatalogItem> children) {
    return new Folder(config.getId().getId(), config.getFullPathList(), String.valueOf(config.getVersion()), children);
  }

  public static FolderConfig getFolderConfig(Folder folder) {
    FolderConfig config = new FolderConfig();
    config.setFullPathList(folder.getPath());
    config.setName(Iterables.getLast(folder.getPath()));
    if (folder.getTag() != null) {
      config.setVersion(Long.valueOf(folder.getTag()));
    }

    return config;
  }

  // Catalog items that are not in the namespace (files/folders in file based sources are given a fake id that
  // is dremio:/path/to/entity - the prefix helps us distinguish between fake and real ids.
  private static String INTERNAL_ID_PREFIX = "dremio:";

  public static String generateInternalId(List<String> path) {
    return INTERNAL_ID_PREFIX + com.dremio.common.utils.PathUtils.toFSPathString(path);
  }

  private static boolean isInternalId(String id) {
    return id.startsWith(INTERNAL_ID_PREFIX);
  }

  public static List<String> getPathFromInternalId(String id) {
    return com.dremio.common.utils.PathUtils.toPathComponents(id.substring(INTERNAL_ID_PREFIX.length()));
  }

}
