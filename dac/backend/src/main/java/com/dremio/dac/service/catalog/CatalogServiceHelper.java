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
package com.dremio.dac.service.catalog;

import static com.dremio.dac.util.DatasetsUtil.toDatasetConfig;
import static com.dremio.service.namespace.dataset.proto.DatasetType.VIRTUAL_DATASET;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.ws.rs.core.SecurityContext;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.PathUtils;
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
import com.dremio.dac.model.sources.PhysicalDatasetPath;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.model.spaces.HomePath;
import com.dremio.dac.model.spaces.SpaceName;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.SourceNotFoundException;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.dac.service.search.SearchContainer;
import com.dremio.dac.service.search.SearchService;
import com.dremio.dac.service.source.SourceService;
import com.dremio.dac.util.DatasetsUtil;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DatasetCatalog.UpdateStatus;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.SchemaEntity;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.service.namespace.BoundedDatasetCount;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
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
import com.dremio.service.users.SystemUser;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
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
  public static final NamespaceAttribute[] DEFAULT_NS_ATTRIBUTES = new NamespaceAttribute[]{};

  /**
   * Additional details that could be included in a result
   */
  public enum DetailType {

    datasetCount {
      @Override
      Stream<CatalogItem.Builder> addInfo(Stream<CatalogItem.Builder> items, final CatalogServiceHelper helper) {
        return items.map(builder -> {
          try {
            final BoundedDatasetCount datasetCount = helper.namespaceService.getDatasetCount(new NamespaceKey(builder.getPath()),
              BoundedDatasetCount.SEARCH_TIME_LIMIT_MS, BoundedDatasetCount.COUNT_LIMIT_TO_STOP_SEARCH);

            return builder
              .setDatasetCount(datasetCount.getCount())
              .setDatasetCountBounded(datasetCount.isCountBound() || datasetCount.isTimeBound());

          } catch (NamespaceException e) {
            throw new RuntimeException(e);
          }
        });
      }
    },

    tags,

    jobCount;

    private static final Set<String> availableValues;

    static {
      availableValues = new HashSet<String>(Arrays.stream(DetailType.values())
        .map(Enum::name)
        .collect(Collectors.toList()));
    }

    public static boolean hasValue(final String key) {
      return availableValues.contains(key);
    }

    Stream<CatalogItem.Builder> addInfo(final Stream<CatalogItem.Builder> items,
      final CatalogServiceHelper helper) {
      throw new IllegalStateException("Not implemented");
    }
  }

  private final Catalog catalog;
  private final SecurityContext context;
  private final SourceService sourceService;
  private final NamespaceService namespaceService;
  private final SabotContext sabotContext;
  private final ReflectionServiceHelper reflectionServiceHelper;
  private final HomeFileTool homeFileTool;
  private final DatasetVersionMutator datasetVersionMutator;
  private final SearchService searchService;

  @Inject
  public CatalogServiceHelper(
    Catalog catalog,
    SecurityContext context,
    SourceService sourceService,
    NamespaceService namespaceService,
    SabotContext sabotContext,
    ReflectionServiceHelper reflectionServiceHelper,
    HomeFileTool homeFileTool,
    DatasetVersionMutator datasetVersionMutator,
    SearchService searchService
  ) {
    this.catalog = catalog;
    this.context = context;
    this.sourceService = sourceService;
    this.namespaceService = namespaceService;
    this.sabotContext = sabotContext;
    this.reflectionServiceHelper = reflectionServiceHelper;
    this.homeFileTool = homeFileTool;
    this.datasetVersionMutator = datasetVersionMutator;
    this.searchService = searchService;
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

  public List<? extends CatalogItem> getTopLevelCatalogItems(final List<String> include) {
    Preconditions.checkNotNull(include);

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

    return applyAdditionalInfoToContainers(topLevelItems, include.stream()
        .map(CatalogServiceHelper.DetailType::valueOf)
        .collect(Collectors.toList()));
  }

  protected NameSpaceContainer getNamespaceEntity(NamespaceKey namespaceKey) throws NamespaceException {
    return namespaceService.getEntities(Collections.singletonList(namespaceKey)).get(0);
  }

  protected NameSpaceContainer getRootContainer(List<String> path) throws NamespaceException {
    NamespaceKey parentKey = new NamespaceKey(path.get(0));
    List<NameSpaceContainer> entities = namespaceService.getEntities(Collections.singletonList(parentKey));

    return entities.get(0);
  }

  public Optional<CatalogEntity> getCatalogEntityByPath(List<String> path) throws NamespaceException {
    NameSpaceContainer entity = getNamespaceEntity(new NamespaceKey(path));

    if (entity == null) {
      // if we can't find it in the namespace, check if its a non-promoted file/folder in a filesystem source
      Optional<CatalogItem> internalItem = getInternalItemByPath(path);
      if (!internalItem.isPresent()) {
        return Optional.absent();
      }

      return getCatalogEntityFromCatalogItem(internalItem.get());
    } else {
      return getCatalogEntityFromNamespaceContainer(extractFromNamespaceContainer(entity).get());
    }
  }

  public Optional<CatalogEntity> getCatalogEntityById(String id, final List<String> include) throws NamespaceException {
    Optional<?> entity = getById(id);

    if (!entity.isPresent()) {
      return Optional.absent();
    }

    return getCatalogEntityFromNamespaceContainer(entity.get());
  }

  private Optional<CatalogEntity> getCatalogEntityFromNamespaceContainer(Object object) throws NamespaceException {
    if (object instanceof SourceConfig) {
      SourceConfig config = (SourceConfig) object;

      Source source = fromSourceConfig(config, getChildrenForPath(new NamespaceKey(config.getName())));
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
      throw new IllegalArgumentException(String.format("Unexpected catalog type found [%s].", object.getClass().getName()));
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
        Optional<CatalogItem> catalogItem = getInternalItemByPath(getPathFromInternalId(id));

        if (!catalogItem.isPresent()) {
          return Optional.absent();
        }

        return getCatalogEntityFromCatalogItem(catalogItem.get());
      } else {
        Optional<?> optional = extractFromNamespaceContainer(namespaceService.getEntityById(id));
        if (!optional.isPresent()) {
          logger.debug("Could not find entity with id [{}]", id);
        }

        return optional;
      }
    } catch (NamespaceException e) {
      logger.debug("Failed to get entity ", e);
      return Optional.absent();
    }
  }

  private Optional<CatalogEntity> getCatalogEntityFromCatalogItem(CatalogItem catalogItem) throws NamespaceException {
    // can either be a folder or a file
    if (catalogItem.getContainerType() == CatalogItem.ContainerSubType.FOLDER) {
      Folder folder = new Folder(catalogItem.getId(), catalogItem.getPath(), null, getListingForInternalItem(getPathFromInternalId(catalogItem.getId())));
      return Optional.of(folder);
    } else if (catalogItem.getType() == CatalogItem.CatalogItemType.FILE) {
      File file = new File(catalogItem.getId(), catalogItem.getPath());
      return Optional.of(file);
    }

    throw new RuntimeException(String.format("Could not retrieve internal item [%s]", catalogItem.toString()));
  }

  // TODO: "?" is losing ACLs info, which requires another lookup against ACS
  private Optional<?> extractFromNamespaceContainer(NameSpaceContainer entity) {
    if (entity == null) {
      // if we can't find it by id, maybe its not in the namespace
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
        DatasetConfig dataset = entity.getDataset();
        result = Optional.of(catalog.getTable(dataset.getId().getId()).getDatasetConfig());
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
        throw new RuntimeException(String.format("Unsupported namespace entity type [%s]", entity.getType()));
      }
    }

    return result;
  }

  private List<CatalogItem> getListingForInternalItem(List<String> path) throws NamespaceException {
    NameSpaceContainer rootEntity = getNamespaceEntity(new NamespaceKey(path.get(0)));

    if (rootEntity.getType() == NameSpaceContainer.Type.SOURCE) {
      return getChildrenForPath(new NamespaceKey(path));
    }

    throw new IllegalArgumentException(String.format("Can only get listing for sources, but [%s] is of type [%s]", path, rootEntity.getType()));
  }

  private Optional<CatalogItem> getInternalItemByPath(List<String> path) throws NamespaceException {
    NameSpaceContainer rootEntity = getNamespaceEntity(new NamespaceKey(path.get(0)));

    if (rootEntity != null && rootEntity.getType() == NameSpaceContainer.Type.SOURCE) {
      return Optional.of(getInternalItemFromSource(rootEntity.getSource(), path));
    } else {
      logger.warn("Can not find internal item with path [{}].", path);
      return Optional.absent();
    }
  }

  private CatalogItem getInternalItemFromSource(SourceConfig sourceConfig, List<String> path) {
    final StoragePlugin plugin = getStoragePlugin(sourceConfig.getName());

    if (!(plugin instanceof FileSystemPlugin)) {
      throw new IllegalArgumentException(String.format("Can not get internal item from non-filesystem source [%s] of type [%s]", sourceConfig.getName(), plugin.getClass().getName()));
    }

    SchemaEntity entity = ((FileSystemPlugin) plugin).get(path, context.getUserPrincipal().getName());

    return convertSchemaEntityToCatalogItem(entity, path.subList(0, path.size() - 1));
  }

  private CatalogItem convertSchemaEntityToCatalogItem(SchemaEntity entity, List<String> parentPath) {
    final List<String> entityPath = Lists.newArrayList(parentPath);
    entityPath.add(entity.getPath());

    CatalogItem catalogItem = null;

    switch(entity.getType()) {
      case FILE: {
        catalogItem = new CatalogItem.Builder()
          .setId(generateInternalId(entityPath))
          .setPath(entityPath)
          .setType(CatalogItem.CatalogItemType.FILE)
          .build();
        break;
      }

      case FOLDER: {
        catalogItem = new CatalogItem.Builder()
          .setId(generateInternalId(entityPath))
          .setPath(entityPath)
          .setType(CatalogItem.CatalogItemType.CONTAINER)
          .setContainerType(CatalogItem.ContainerSubType.FOLDER)
          .build();
        break;
      }

      case FILE_TABLE:
      case FOLDER_TABLE: {
        try {
          final NamespaceKey namespaceKey = new NamespaceKey(PathUtils.toPathComponents(PathUtils.toFSPath(entityPath)));
          final DatasetConfig dataset = namespaceService.getDataset(namespaceKey);
          catalogItem = new CatalogItem.Builder()
            .setId(dataset.getId().getId())
            .setPath(entityPath)
            .setType(CatalogItem.CatalogItemType.DATASET)
            .setDatasetType(CatalogItem.DatasetSubType.PROMOTED)
            .build();
        } catch (NamespaceException e) {
          logger.warn("Can not find item with path [%s]", entityPath, e);
        }
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
    final List<CatalogItem> catalogItems = new ArrayList<>();

    // get parent info
    NameSpaceContainer rootEntity = getNamespaceEntity(new NamespaceKey(path.getPathComponents().get(0)));

    if (rootEntity.getType() == NameSpaceContainer.Type.SOURCE) {
      catalogItems.addAll(getChildrenForSourcePath(rootEntity.getSource().getName(), path.getPathComponents()));
    } else {
      // for non-source roots, go straight to the namespace
      catalogItems.addAll(getNamespaceChildrenForPath(path));
    }

    return catalogItems;
  }

  private List<CatalogItem> getNamespaceChildrenForPath(NamespaceKey path) {
    final List<CatalogItem> catalogItems = new ArrayList<>();

    try {
      final List<NameSpaceContainer> list = namespaceService.list(path);

      for (NameSpaceContainer container : list) {
        final Optional<CatalogItem> item = CatalogItem.fromNamespaceContainer(container);
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
   *  Returns all children of the listingPath for a source
   */
  private List<CatalogItem> getChildrenForSourcePath(String sourceName, List<String> listingPath) {
    final List<CatalogItem> catalogItems = new ArrayList<>();

    final StoragePlugin plugin = getStoragePlugin(sourceName);
    if (plugin instanceof FileSystemPlugin) {
      // For file based plugins, use the list method to get the listing.  That code will merge in any promoted datasets
      // that are in the namespace for us.  This is in line with what the UI does.
      final List<SchemaEntity> list = ((FileSystemPlugin) plugin).list(listingPath, context.getUserPrincipal().getName());

      for (SchemaEntity entity : list) {
        final CatalogItem catalogItem = convertSchemaEntityToCatalogItem(entity, listingPath);

        if (catalogItem != null) {
          catalogItems.add(catalogItem);
        }
      }
    } else {
      // for non-file based plugins we can go directly to the namespace
      catalogItems.addAll(getNamespaceChildrenForPath(new NamespaceKey(listingPath)));
    }

    return catalogItems;
  }

  public CatalogEntity createCatalogItem(CatalogEntity entity) throws NamespaceException, UnsupportedOperationException, ExecutionSetupException {
    if (entity instanceof Space) {
      Space space = (Space) entity;
      return createSpace(space, getNamespaceAttributes(entity));
    } else if (entity instanceof Source) {
      return createSource((Source) entity, getNamespaceAttributes(entity));
    } else if (entity instanceof Dataset) {
      Dataset dataset = (Dataset) entity;
      return createDataset(dataset, getNamespaceAttributes(entity));
    } else if (entity instanceof Folder) {
      try {
        return createFolder((Folder) entity, getNamespaceAttributes(entity));
      } catch (UserException e) {
        throw new ConcurrentModificationException(e);
      }
    } else {
      throw new UnsupportedOperationException(String.format("Catalog item of type [%s] can not be edited", entity.getClass().getName()));
    }
  }

  protected CatalogEntity createDataset(Dataset dataset, NamespaceAttribute... attributes) throws NamespaceException {
    validateDataset(dataset);

    // only handle VDS
    Preconditions.checkArgument(dataset.getType() != Dataset.DatasetType.PHYSICAL_DATASET, "Physical Datasets can only be created by promoting other entities.");

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

    sabotContext.getViewCreator(context.getUserPrincipal().getName()).createView(dataset.getPath(), dataset.getSql(), dataset.getSqlContext(), attributes);

    DatasetConfig created = namespaceService.getDataset(namespaceKey);

    return getDatasetFromConfig(created, null);
  }

  /**
   * Promotes the target to a PDS using the formatting options submitted via dataset.
   */
  public Dataset promoteToDataset(String targetId, Dataset dataset) throws NamespaceException, UnsupportedOperationException {
    Preconditions.checkArgument(dataset.getType() == Dataset.DatasetType.PHYSICAL_DATASET, "Promoting can only create physical datasets.");

    // The id can either be a internal id or a namespace id.  It will be a namespace id if the entity had been promoted
    // before and then unpromoted.
    final List<String> path;
    if (isInternalId(targetId)) {
      path = getPathFromInternalId(targetId);
    } else {
      final NameSpaceContainer entityById = namespaceService.getEntityById(targetId);
      if (entityById == null) {
        throw new IllegalArgumentException(String.format("Could not find entity to promote with ud [%s]", targetId));
      }

      path = entityById.getFullPathList();
    }

    // getPathFromInternalId will return a path without quotes so make sure we do the same for the dataset path
    List<String> normalizedPath = dataset.getPath().stream().map(PathUtils::removeQuotes).collect(Collectors.toList());
    Preconditions.checkArgument(CollectionUtils.isEqualCollection(path, normalizedPath), "Entity id does not match the path specified in the dataset.");

    // validation
    validateDataset(dataset);
    Preconditions.checkArgument(dataset.getFormat() != null, "To promote a dataset, format settings are required.");

    NamespaceKey namespaceKey = new NamespaceKey(path);
    Optional<CatalogItem> catalogItem = getInternalItemByPath(path);

    if (!catalogItem.isPresent()) {
      throw new IllegalArgumentException(String.format("Could not find entity to promote with path [%s]", path));
    }

    // can only promote a file or folder from a source (which getInternalItemByPath verifies)
    if (catalogItem.get().getContainerType() == CatalogItem.ContainerSubType.FOLDER || catalogItem.get().getType() == CatalogItem.CatalogItemType.FILE) {
      PhysicalDatasetConfig physicalDatasetConfig = new PhysicalDatasetConfig();
      physicalDatasetConfig.setName(namespaceKey.getName());
      physicalDatasetConfig.setFormatSettings(dataset.getFormat().asFileConfig());

      if (catalogItem.get().getContainerType() == CatalogItem.ContainerSubType.FOLDER) {
        physicalDatasetConfig.setType(com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER);
      } else {
        physicalDatasetConfig.setType(com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET_SOURCE_FILE);
      }
      physicalDatasetConfig.setFullPathList(path);

      catalog.createOrUpdateDataset(namespaceService, new NamespaceKey(namespaceKey.getRoot()),
        new PhysicalDatasetPath(path).toNamespaceKey(), toDatasetConfig(physicalDatasetConfig, null), getNamespaceAttributes(dataset));
    } else {
      throw new UnsupportedOperationException(String.format("Can only promote a folder or a file but found [%s]", catalogItem.get().getType()));
    }

    return getDatasetFromConfig(namespaceService.getDataset(namespaceKey), null);
  }

  private void updateDataset(Dataset dataset, NamespaceAttribute... attributes) throws NamespaceException, IOException {
    Preconditions.checkArgument(dataset.getId() != null, "Dataset Id is missing.");

    DatasetConfig currentDatasetConfig = namespaceService.findDatasetByUUID(dataset.getId());
    if (currentDatasetConfig == null) {
      throw new IllegalArgumentException(String.format("Could not find dataset with id [%s]", dataset.getId()));
    }

    validateDataset(dataset);

    // use the version of the dataset to check for concurrency issues
    currentDatasetConfig.setTag(dataset.getTag());

    NamespaceKey namespaceKey = new NamespaceKey(dataset.getPath());

    // check type
    final DatasetType type = currentDatasetConfig.getType();

    if (dataset.getType() == Dataset.DatasetType.PHYSICAL_DATASET) {
      // cannot change the path of a physical dataset
      Preconditions.checkArgument(CollectionUtils.isEqualCollection(dataset.getPath(), currentDatasetConfig.getFullPathList()), "Dataset path can not be modified.");
      Preconditions.checkArgument( type != VIRTUAL_DATASET, "Dataset type can not be modified");

      // PDS specific config
      currentDatasetConfig.getPhysicalDataset().setAllowApproxStats(dataset.getApproximateStatisticsAllowed());

      if (type == com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET_HOME_FILE) {
        DatasetConfig datasetConfig = toDatasetConfig(dataset.getFormat().asFileConfig(), type,
          context.getUserPrincipal().getName(), currentDatasetConfig.getId());

        catalog.createOrUpdateDataset(namespaceService, new NamespaceKey(HomeFileSystemStoragePlugin.HOME_PLUGIN_NAME), namespaceKey, datasetConfig, attributes);
      } else if (type == com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET_SOURCE_FILE
          || type == com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER) {
        Preconditions.checkArgument(dataset.getFormat() != null, "Promoted dataset needs to have a format set.");

        //DatasetConfig datasetConfig = toDatasetConfig(dataset.getFormat().asFileConfig(), currentDatasetConfig.getType(), context.getUserPrincipal().getName(), currentDatasetConfig.getId());
        // only thing that can change is the formatting
        currentDatasetConfig.getPhysicalDataset().setFormatSettings(dataset.getFormat().asFileConfig());

        catalog.createOrUpdateDataset(namespaceService, new NamespaceKey(namespaceKey.getRoot()), namespaceKey, currentDatasetConfig, attributes);
      } else {
        catalog.createOrUpdateDataset(namespaceService, new NamespaceKey(namespaceKey.getRoot()), namespaceKey, currentDatasetConfig, attributes);
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
      Preconditions.checkArgument(type == VIRTUAL_DATASET, "Dataset type can not be modified");
      VirtualDataset virtualDataset = currentDatasetConfig.getVirtualDataset();
      Dataset currentDataset = getDatasetFromConfig(currentDatasetConfig, null);

      // Check if the dataset is being renamed
      if (!Objects.equals(currentDatasetConfig.getFullPathList(), dataset.getPath())) {
        datasetVersionMutator.renameDataset(new DatasetPath(currentDatasetConfig.getFullPathList()), new DatasetPath(dataset.getPath()));
        currentDatasetConfig = namespaceService.getDataset(namespaceKey);
      }

      virtualDataset.setSql(dataset.getSql());
      virtualDataset.setContextList(dataset.getSqlContext());
      currentDatasetConfig.setVirtualDataset(virtualDataset);

      List<String> path = dataset.getPath();

      View view = new View(path.get(path.size() - 1), dataset.getSql(), Collections.emptyList(), null, virtualDataset.getContextList(), false);
      catalog.updateView(namespaceKey, view, attributes);
    }
  }

  private void deleteDataset(DatasetConfig config, String tag) throws NamespaceException, UnsupportedOperationException, IOException {
    // if no tag is passed in, use the latest version
    String version = config.getTag();

    if (tag != null) {
      version = tag;
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
        deleteHomeDataset(config, version, config.getFullPathList());
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

  public void deleteHomeDataset(DatasetConfig config, String version, List<String> pathComponents) throws IOException, NamespaceException {
    FileConfig formatSettings = config.getPhysicalDataset().getFormatSettings();
    Preconditions.checkArgument(pathComponents != null && !pathComponents.isEmpty(), "Cannot find path to dataset");
    if (homeFileTool.fileExists(formatSettings.getLocation())) {
      homeFileTool.deleteFile(formatSettings.getLocation());
    }
    namespaceService.deleteDataset(new NamespaceKey(pathComponents), version);
  }

  public void removeFormatFromDataset(DatasetConfig config, String version) {
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

  protected CatalogEntity createSpace(Space space, NamespaceAttribute... attributes) throws NamespaceException {
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

    namespaceService.addOrUpdateSpace(namespaceKey, getSpaceConfig(space).setCtime(System.currentTimeMillis()), attributes);

    return getSpaceFromConfig(namespaceService.getSpace(namespaceKey), null);
  }

  protected void updateSpace(Space space, NamespaceAttribute... attributes) throws NamespaceException {
    NamespaceKey namespaceKey = new NamespaceKey(space.getName());
    SpaceConfig spaceConfig = namespaceService.getSpace(namespaceKey);

    Preconditions.checkArgument(space.getName().equals(spaceConfig.getName()), "Space name is immutable.");

    namespaceService.addOrUpdateSpace(namespaceKey, getSpaceConfig(space).setCtime(spaceConfig.getCtime()), attributes);
  }

  protected void deleteSpace(SpaceConfig spaceConfig, String version) throws NamespaceException {
    namespaceService.deleteSpace(new NamespaceKey(spaceConfig.getName()), version);
  }

  protected CatalogEntity createSource(Source source, NamespaceAttribute... attributes) throws NamespaceException, ExecutionSetupException {
    SourceConfig sourceConfig = sourceService.createSource(source.toSourceConfig(), attributes);
    return fromSourceConfig(sourceConfig, getChildrenForPath(new NamespaceKey(sourceConfig.getName())));
  }

  public CatalogEntity updateCatalogItem(CatalogEntity entity, String id) throws NamespaceException, UnsupportedOperationException, ExecutionSetupException, IOException {
    Preconditions.checkArgument(entity.getId().equals(id), "Ids must match.");

    if (entity instanceof Dataset) {
      Dataset dataset = (Dataset) entity;
      updateDataset(dataset, getNamespaceAttributes(entity));
    } else if (entity instanceof Source) {
      Source source = (Source) entity;
      sourceService.updateSource(id, source.toSourceConfig(), getNamespaceAttributes(entity));
    } else if (entity instanceof Space) {
      Space space = (Space) entity;
      updateSpace(space, getNamespaceAttributes(space));
    } else if (entity instanceof Folder) {
      Folder folder = (Folder) entity;
      updateFolder(folder, getNamespaceAttributes(entity));
    } else {
      throw new UnsupportedOperationException(String.format("Catalog item [%s] of type [%s] can not be edited.", id, entity.getClass().getName()));
    }

    // TODO(DX-18416) What to do?
    Optional<CatalogEntity> newEntity = getCatalogEntityById(id, ImmutableList.of());

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
        config.setTag(tag);
      }

      sourceService.deleteSource(config);
    } else if (object instanceof SpaceConfig) {
      SpaceConfig config = (SpaceConfig) object;

      String version = config.getTag();

      if (tag != null) {
        version = tag;
      }
      deleteSpace(config, version);
    } else if (object instanceof DatasetConfig) {
      DatasetConfig config = (DatasetConfig) object;

      try {
        deleteDataset(config, tag);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    } else if (object instanceof FolderConfig) {
      FolderConfig config = (FolderConfig) object;

      String version = config.getTag();

      if (tag != null) {
        version = tag;
      }

      namespaceService.deleteFolder(new NamespaceKey(config.getFullPathList()), version);
    } else {
      throw new UnsupportedOperationException(String.format("Catalog item [%s] of type [%s] can not be deleted.", id, object.getClass().getName()));
    }
  }

  protected Folder createFolder(Folder folder, NamespaceAttribute... attributes) throws NamespaceException {
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
        namespaceService.addOrUpdateFolder(key, getFolderConfig(folder), attributes);
        break;
      }

      default: {
        throw new UnsupportedOperationException(String.format("Can not create a folder inside a [%s].", container.getType()));
      }
    }

    return getFolderFromConfig(namespaceService.getFolder(key), null);
  }

  protected void updateFolder(Folder folder, NamespaceAttribute... attributes) throws NamespaceException {
    NamespaceKey namespaceKey = new NamespaceKey(folder.getPath());
    FolderConfig folderConfig = namespaceService.getFolder(namespaceKey);

    Preconditions.checkArgument(CollectionUtils.isEqualCollection(folder.getPath(), folderConfig.getFullPathList()), "Folder path is immutable.");

    NameSpaceContainer rootContainer = getRootContainer(folder.getPath());
    if (rootContainer.getType() == NameSpaceContainer.Type.SOURCE) {
      throw new UnsupportedOperationException("Can not update a folder inside a source");
    }

    namespaceService.addOrUpdateFolder(namespaceKey, getFolderConfig(folder), attributes);
  }

  public Source fromSourceConfig(SourceConfig config, List<CatalogItem> children) {
    // TODO: clean up source config creation, move it all into this class
    return sourceService.fromSourceConfig(config, children);
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

  /**
   *  Refresh a catalog item's metadata.  Only supports datasets currently.
   */
  public UpdateStatus refreshCatalogItemMetadata(String id,
                                                               Boolean delete,
                                                               Boolean force,
                                                               Boolean promotion)
  throws UnsupportedOperationException {
    Optional<?> entity = getById(id);

    if (!entity.isPresent()) {
      throw new IllegalArgumentException(String.format("Could not find entity with id [%s].", id));
    }

    Object object = entity.get();

    if (object instanceof DatasetConfig) {
      final NamespaceKey namespaceKey = catalog.resolveSingle(new NamespaceKey(((DatasetConfig)object).getFullPathList()));
      final DatasetRetrievalOptions.Builder retrievalOptionsBuilder = DatasetRetrievalOptions.newBuilder();

      if (delete != null) {
        retrievalOptionsBuilder.setDeleteUnavailableDatasets(delete.booleanValue());
      }
      if (force != null) {
        retrievalOptionsBuilder.setForceUpdate(force.booleanValue());
      }
      if (promotion != null) {
        retrievalOptionsBuilder.setAutoPromote(promotion.booleanValue());
      }

      return catalog.refreshDataset(namespaceKey, retrievalOptionsBuilder.build());

    } else {
      throw new UnsupportedOperationException(
        String.format("Cannot refresh metadata on %s type.  Metadata refresh can only operate on physical datasets.",
          object.getClass().getName()));
    }
  }

  private Optional<AccelerationSettings> getStoredReflectionSettingsForDataset(DatasetConfig datasetConfig) {
    return reflectionServiceHelper.getReflectionSettings().getStoredReflectionSettings(new NamespaceKey(datasetConfig.getFullPathList()));
  }

  public Dataset getDatasetFromConfig(DatasetConfig config, Dataset.RefreshSettings refreshSettings) {
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
        config.getTag(),
        refreshSettings,
        sql,
        sqlContext,
        null,
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
        String.valueOf(config.getTag()),
        refreshSettings,
        null,
        null,
        format,
        physicalDataset.getAllowApproxStats()
      );
    }
  }

  private static Home getHomeFromConfig(HomeConfig config, List<CatalogItem> children) {
    return new Home(
      config.getId().getId(),
      HomeName.getUserHomePath(config.getOwner()).toString(),
      String.valueOf(config.getTag()),
      children
    );
  }

  protected Space getSpaceFromConfig(SpaceConfig config, List<CatalogItem> children) {
    return new Space(
      config.getId().getId(),
      config.getName(),
      String.valueOf(config.getTag()),
      config.getCtime(),
      children
    );
  }

  public static SpaceConfig getSpaceConfig(Space space) {
    SpaceConfig config = new SpaceConfig();
    config.setName(space.getName());
    config.setId(new EntityId(space.getId()));
    if (space.getTag() != null) {
      config.setTag(space.getTag());
    }
    config.setCtime(space.getCreatedAt());

    return config;
  }

  protected Folder getFolderFromConfig(FolderConfig config, List<CatalogItem> children) {
    return new Folder(config.getId().getId(), config.getFullPathList(), String.valueOf(config.getTag()), children);
  }

  public static FolderConfig getFolderConfig(Folder folder) {
    FolderConfig config = new FolderConfig();
    config.setId(new EntityId(folder.getId()));
    config.setFullPathList(folder.getPath());
    config.setName(Iterables.getLast(folder.getPath()));
    if (folder.getTag() != null) {
      config.setTag(folder.getTag());
    }

    return config;
  }

  protected NamespaceAttribute[] getNamespaceAttributes(CatalogEntity entity) {
    return DEFAULT_NS_ATTRIBUTES;
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

  private StoragePlugin getStoragePlugin(String sourceName) {
    final StoragePlugin plugin = catalog.getSource(sourceName);

    if (plugin == null) {
      throw new SourceNotFoundException(sourceName);
    }

    return plugin;
  }

  public List<SearchContainer> searchByQuery(String query) throws NamespaceException {
    return searchService.search(query, null);
  }

  public List<CatalogItem> search(String query) throws NamespaceException {
    List<SearchContainer> searchResults = searchByQuery(query);

    return searchResults.stream().map(searchResult -> {
      return CatalogItem.fromNamespaceContainer(searchResult.getNamespaceContainer());
    })
      .filter(Optional::isPresent)
      .map(Optional::get)
      .collect(Collectors.toList());
  }

  public List<CatalogItem> applyAdditionalInfoToContainers(
    final List<CatalogItem> items, final List<DetailType> include) {
    Stream<CatalogItem.Builder> resultList = items.stream().map(CatalogItem.Builder::new);

    for (DetailType detail : include) {
      resultList = detail.addInfo(resultList, this);
    }

    return resultList
      .map(CatalogItem.Builder::build)
      .collect(Collectors.toList());
  }

  public void createHomeSpace(String userName) {
    try {
      CatalogServiceHelper.ensureUserHasHomespace(sabotContext.getNamespaceService(SystemUser.SYSTEM_USERNAME), userName);
    } catch (NamespaceException ignored) {
    }
  }

  public static void ensureUserHasHomespace(NamespaceService namespaceService, String userName) throws NamespaceException {
    final NamespaceKey homeKey = new HomePath(HomeName.getUserHomePath(userName)).toNamespaceKey();
    try {
      namespaceService.getHome(homeKey);
    } catch (NamespaceNotFoundException ignored) {
      // create home
      namespaceService.addOrUpdateHome(homeKey,
        new HomeConfig().setCtime(System.currentTimeMillis()).setOwner(userName)
      );
    }
  }
}
