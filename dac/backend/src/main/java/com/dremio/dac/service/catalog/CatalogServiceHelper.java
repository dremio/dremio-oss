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
import static com.dremio.exec.ExecConstants.VERSIONED_VIEW_ENABLED;
import static com.dremio.service.namespace.dataset.proto.DatasetType.VIRTUAL_DATASET;

import com.dremio.catalog.exception.SourceDoesNotExistException;
import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
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
import com.dremio.dac.explore.QueryParser;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.VersionContextUtils;
import com.dremio.dac.homefiles.HomeFileSystemStoragePlugin;
import com.dremio.dac.homefiles.HomeFileTool;
import com.dremio.dac.model.folder.SourceFolderPath;
import com.dremio.dac.model.namespace.NamespaceTree;
import com.dremio.dac.model.sources.PhysicalDatasetPath;
import com.dremio.dac.model.sources.SourceName;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.model.spaces.HomePath;
import com.dremio.dac.model.spaces.SpaceName;
import com.dremio.dac.service.autocomplete.model.SuggestionsType;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.ClientErrorException;
import com.dremio.dac.service.errors.SourceNotFoundException;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.dac.service.search.SearchContainer;
import com.dremio.dac.service.search.SearchService;
import com.dremio.dac.service.source.SourceService;
import com.dremio.dac.util.DatasetsUtil;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DatasetMetadataState;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.catalog.TableMutationOptions;
import com.dremio.exec.catalog.VersionedDatasetId;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.physical.base.ViewOptions;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.sql.parser.ParserUtil;
import com.dremio.exec.planner.sql.parser.SqlGrant;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceInfo;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.ReferenceTypeConflictException;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.SchemaEntity;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.options.OptionManager;
import com.dremio.plugins.ExternalNamespaceEntry;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.jobs.metadata.QueryMetadata;
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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.core.SecurityContext;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Catalog Service Helper
 *
 * <p>A helper that allows interacting with the Dremio catalog. Allows browsing and
 * created/editing/deleting of sources, spaces, datasets, files and folders where allowed.
 */
public class CatalogServiceHelper {
  private static final Logger logger = LoggerFactory.getLogger(CatalogServiceHelper.class);
  public static final NamespaceAttribute[] DEFAULT_NS_ATTRIBUTES = new NamespaceAttribute[] {};

  /** Additional details that could be included in a result */
  public enum DetailType {
    datasetCount {
      @Override
      Stream<CatalogItem.Builder> addInfo(
          Stream<CatalogItem.Builder> items, final CatalogServiceHelper helper) {
        return items.map(
            builder -> {
              try {
                final BoundedDatasetCount datasetCount =
                    helper.namespaceService.getDatasetCount(
                        new NamespaceKey(builder.getPath()),
                        BoundedDatasetCount.SEARCH_TIME_LIMIT_MS,
                        BoundedDatasetCount.COUNT_LIMIT_TO_STOP_SEARCH);

                return builder
                    .setDatasetCount(datasetCount.getCount())
                    .setDatasetCountBounded(
                        datasetCount.isCountBound() || datasetCount.isTimeBound());

              } catch (NamespaceException e) {
                throw new RuntimeException(e);
              }
            });
      }
    },

    tags,

    jobCount,

    children;

    private static final Set<String> AVAILABLE_VALUES;

    static {
      AVAILABLE_VALUES =
          Arrays.stream(DetailType.values()).map(Enum::name).collect(Collectors.toSet());
    }

    public static boolean hasValue(final String key) {
      return AVAILABLE_VALUES.contains(key);
    }

    Stream<CatalogItem.Builder> addInfo(
        final Stream<CatalogItem.Builder> items, final CatalogServiceHelper helper) {
      throw new IllegalStateException("Not implemented");
    }
  }

  private final Supplier<Catalog> catalogSupplier;
  private final SecurityContext securityContext;
  private final SourceService sourceService;
  private final NamespaceService namespaceService;
  private final SabotContext sabotContext;
  private final ReflectionServiceHelper reflectionServiceHelper;
  private final HomeFileTool homeFileTool;
  private final DatasetVersionMutator datasetVersionMutator;
  private final SearchService searchService;
  private final OptionManager optionManager;

  @Inject
  public CatalogServiceHelper(
      CatalogService catalogService,
      SecurityContext securityContext,
      SourceService sourceService,
      NamespaceService namespaceService,
      SabotContext sabotContext,
      ReflectionServiceHelper reflectionServiceHelper,
      HomeFileTool homeFileTool,
      DatasetVersionMutator datasetVersionMutator,
      SearchService searchService,
      OptionManager optionManager) {
    // Postpone creation till SecurityContext is populated.
    this.catalogSupplier = Suppliers.memoize(() -> createCatalog(catalogService, securityContext));
    this.securityContext = securityContext;
    this.sourceService = sourceService;
    this.namespaceService = namespaceService;
    this.sabotContext = sabotContext;
    this.reflectionServiceHelper = reflectionServiceHelper;
    this.homeFileTool = homeFileTool;
    this.datasetVersionMutator = datasetVersionMutator;
    this.searchService = searchService;
    this.optionManager = optionManager;
  }

  private static Catalog createCatalog(
      CatalogService catalogService, SecurityContext securityContext) {
    return catalogService.getCatalog(
        MetadataRequestOptions.newBuilder()
            .setSchemaConfig(
                SchemaConfig.newBuilder(
                        CatalogUser.from(securityContext.getUserPrincipal().getName()))
                    .build())
            // Disable inline metadata refresh.
            .setCheckValidity(false)
            .build());
  }

  protected Catalog getCatalog() {
    return catalogSupplier.get();
  }

  @WithSpan
  public Optional<DatasetConfig> getDatasetById(String datasetId) {
    DremioTable table = catalogSupplier.get().getTable(datasetId);

    if (table == null) {
      return Optional.empty();
    }

    return Optional.ofNullable(table.getDatasetConfig());
  }

  private HomeConfig getHomeForCurrentUser() throws NamespaceException {
    HomePath homePath =
        new HomePath(HomeName.getUserHomePath(securityContext.getUserPrincipal().getName()));

    return namespaceService.getHome(homePath.toNamespaceKey());
  }

  @WithSpan
  public List<? extends CatalogItem> getTopLevelCatalogItems(final List<String> include) {
    Preconditions.checkNotNull(include);

    List<CatalogItem> topLevelItems = new ArrayList<>();

    try {
      HomeConfig homeForCurrentUser = getHomeForCurrentUser();
      topLevelItems.add(CatalogItem.fromHomeConfig(homeForCurrentUser));
    } catch (NamespaceException e) {
      // If for some reason we can't find a home space, log it but keep going.
      logger.warn(
          "Failed to find home space for user [{}]", securityContext.getUserPrincipal().getName());
    }

    for (SpaceConfig spaceConfig : namespaceService.getSpaces()) {
      topLevelItems.add(CatalogItem.fromSpaceConfig(spaceConfig));
    }

    for (SourceConfig sourceConfig : sourceService.getSources()) {
      topLevelItems.add(CatalogItem.fromSourceConfig(sourceConfig));
    }

    return applyAdditionalInfoToContainers(
        topLevelItems,
        include.stream()
            .map(CatalogServiceHelper.DetailType::valueOf)
            .collect(Collectors.toList()));
  }

  protected NameSpaceContainer getNamespaceEntity(NamespaceKey namespaceKey)
      throws NamespaceException {
    return namespaceService.getEntities(Collections.singletonList(namespaceKey)).get(0);
  }

  protected NameSpaceContainer getRootContainer(List<String> path) throws NamespaceException {
    NamespaceKey parentKey = new NamespaceKey(path.get(0));
    return namespaceService.getEntities(Collections.singletonList(parentKey)).get(0);
  }

  @WithSpan
  public Optional<CatalogEntity> getCatalogEntityByPath(
      final List<String> path, final List<String> include, final List<String> exclude)
      throws NamespaceException {
    return getCatalogEntityByPath(path, include, exclude, null, null);
  }

  @WithSpan
  public Optional<CatalogEntity> getCatalogEntityByPath(
      final List<String> path,
      final List<String> include,
      final List<String> exclude,
      final String versionType,
      final String versionValue)
      throws NamespaceException {
    final boolean isRoot = path.size() == 1;
    final NamespaceKey namespaceKey = new NamespaceKey(path);
    final boolean includeChildren = !exclude.contains(DetailType.children.name());

    if (!isRoot
        && CatalogUtil.requestedPluginSupportsVersionedTables(
            namespaceKey, catalogSupplier.get())) {
      final Optional<TableVersionContext> tableVersionContext =
          TableVersionContext.tryParse(versionType, versionValue);
      final CatalogEntityKey.Builder builder = CatalogEntityKey.newBuilder().keyComponents(path);

      if (tableVersionContext.isPresent()) {
        builder.tableVersionContext(tableVersionContext.get());
      } else if (!Strings.isNullOrEmpty(versionType) || !Strings.isNullOrEmpty(versionValue)) {
        throw new ClientErrorException(
            "Missing a valid versionType/versionValue pair for versioned dataset");
      }

      VersionContext versionContext =
          tableVersionContext.isPresent()
              ? tableVersionContext.get().asVersionContext()
              : VersionContext.NOT_SPECIFIED;
      VersionedPlugin.EntityType entityType = getVersionedEntityType(path, versionContext);
      switch (entityType) {
        case ICEBERG_TABLE:
        case ICEBERG_VIEW:
          final CatalogEntityKey catalogEntityKey = builder.build();
          final DremioTable table = catalogSupplier.get().getTable(catalogEntityKey);

          if (table == null) {
            return Optional.empty();
          }

          final DatasetConfig datasetConfig = table.getDatasetConfig();
          final Optional<AccelerationSettings> settings =
              getStoredReflectionSettingsForDataset(datasetConfig);
          final Dataset dataset =
              toDatasetAPI(
                  datasetConfig,
                  settings.map(Dataset.RefreshSettings::new).orElse(null),
                  table.getDatasetMetadataState());

          return Optional.of(dataset);
        case FOLDER:
          final String id = catalogSupplier.get().getDatasetId(namespaceKey);
          final Folder folder =
              createCatalogItemForVersionedFolder(
                  path, id, includeChildren, versionType, versionValue);
          return Optional.of(folder);
        case UNKNOWN:
          logger.warn("Could not find versioned entity with path {}", path);
          return Optional.empty();
        default:
          logger.warn("Unrecognized type of versioned entity with path {}", path);
          return Optional.empty();
      }
    }

    final NameSpaceContainer entity = getNamespaceEntity(namespaceKey);

    if (entity == null) {
      // if we can't find it in the namespace, check if it is a non-promoted file/folder in a
      // filesystem source
      Optional<CatalogItem> internalItem = getInternalItemByPath(path);
      if (!internalItem.isPresent()) {
        return Optional.empty();
      }

      return getCatalogEntityFromCatalogItem(internalItem.get(), includeChildren);
    } else {
      return getCatalogEntityFromNamespaceContainer(
          entity, includeChildren, versionType, versionValue);
    }
  }

  @WithSpan
  public Optional<CatalogEntity> getCatalogEntityById(
      String id, final List<String> include, final List<String> exclude) throws NamespaceException {
    boolean includeChildren = !exclude.contains(DetailType.children.name());
    Optional<?> entity = getById(id, includeChildren);

    if (!entity.isPresent()) {
      return Optional.empty();
    }

    String refType = null;
    String refValue = null;
    VersionedDatasetId versionedDatasetId = VersionedDatasetId.tryParse(id);
    if (versionedDatasetId != null) {
      VersionContext versionContext = versionedDatasetId.getVersionContext().asVersionContext();
      refType = versionContext.getType().name();
      refValue = versionContext.getValue();
    }
    return getCatalogEntity(entity.get(), includeChildren, refType, refValue);
  }

  @WithSpan
  private Optional<CatalogEntity> getCatalogEntity(
      Object object, boolean includeChildren, String refType, String refValue)
      throws NamespaceException {
    if (object instanceof NameSpaceContainer) {
      return getCatalogEntityFromNamespaceContainer(
          (NameSpaceContainer) object, includeChildren, refType, refValue);
    } else if (object instanceof CatalogEntity) {
      // this is something not in the namespace, a file/folder from a filesystem source
      CatalogEntity catalogEntity = (CatalogEntity) object;
      return Optional.of(catalogEntity);
    } else {
      throw new IllegalArgumentException(
          String.format("Unexpected catalog type found [%s].", object.getClass().getName()));
    }
  }

  private Optional<CatalogEntity> getCatalogEntityFromNamespaceContainer(
      NameSpaceContainer container, boolean includeChildren, String refType, String refValue)
      throws NamespaceException {

    switch (container.getType()) {
      case SOURCE:
        Source source =
            toSourceAPI(
                container,
                includeChildren
                    ? getChildrenForPath(
                        new NamespaceKey(container.getFullPathList()), refType, refValue)
                    : Collections.emptyList());
        return Optional.of(source);
      case SPACE:
        SpaceConfig config = container.getSpace();

        Space space =
            toSpaceAPI(
                container,
                includeChildren
                    ? getChildrenForPath(new NamespaceKey(config.getName()))
                    : Collections.emptyList());
        return Optional.of(space);
      case DATASET:
        // Update container to use dataset from the catalog to ensure we have recordSchema and
        // update the full dataset
        // config metadata if metadata has not been refreshed within the expiration window.
        // More info: When user added a new source, the nameSpaceContainer only contains a bare
        // minimum metadata info.
        String datasetId = container.getDataset().getId().getId();
        DremioTable table = catalogSupplier.get().getTable(datasetId);
        if (table == null) {
          return Optional.empty();
        }
        DatasetConfig datasetConfig = table.getDatasetConfig();
        container.setDataset(datasetConfig);

        Dataset dataset;

        // only set acceleration settings if one exists in the store - we don't want inherited
        // settings
        Optional<AccelerationSettings> settings =
            getStoredReflectionSettingsForDataset(datasetConfig);
        dataset =
            toDatasetAPI(
                container,
                settings.map(Dataset.RefreshSettings::new).orElse(null),
                table.getDatasetMetadataState());

        return Optional.of(dataset);
      case HOME:
        HomeConfig homeConfig = container.getHome();

        List<CatalogItem> children =
            includeChildren
                ? getChildrenForPath(
                    new NamespaceKey(HomeName.getUserHomePath(homeConfig.getOwner()).getName()))
                : Collections.emptyList();
        Home home = getHomeFromConfig(homeConfig, children);
        return Optional.of(home);
      case FOLDER:
        FolderConfig folderConfig = container.getFolder();

        Folder folder =
            toFolderAPI(
                container,
                includeChildren
                    ? getChildrenForPath(
                        new NamespaceKey(folderConfig.getFullPathList()), refType, refValue)
                    : Collections.emptyList());
        return Optional.of(folder);
      default:
        throw new IllegalArgumentException(
            String.format("Unexpected catalog type found [%s].", container.getType()));
    }
  }

  /**
   * Given an id, retrieves the entity from the namespace. Also handles fake ids (using
   * generateInternalId) that we generate for folders/files that exist in file-based sources that
   * are not in the namespace.
   *
   * <p>Note: this returns the namespace container found in the namespace. For non-namespace items
   * it returns the appropriate CatalogEntity item (Folder/File only).
   */
  @WithSpan
  private Optional<?> getById(String id, boolean includeChildren) {
    try {
      if (isInternalId(id)) {
        Optional<CatalogItem> catalogItem = getInternalItemByPath(getPathFromInternalId(id));

        if (!catalogItem.isPresent()) {
          return Optional.empty();
        }

        final CatalogItem item = catalogItem.get();

        // sometimes we can get back a namespace entity for an internal id (like a folder that gets
        // ACLs)
        if (!isInternalId(item.getId())) {
          return getById(item.getId(), includeChildren);
        }

        return getCatalogEntityFromCatalogItem(item, includeChildren);
      } else if (VersionedDatasetId.tryParse(id) != null) {
        VersionedDatasetId versionedDatasetId = VersionedDatasetId.tryParse(id);
        assert versionedDatasetId != null;
        final VersionedPlugin.EntityType entityType = getVersionedEntityType(versionedDatasetId);
        switch (entityType) {
          case ICEBERG_VIEW:
          case ICEBERG_TABLE:
            DremioTable table = catalogSupplier.get().getTable(id);
            DatasetConfig datasetConfig = table.getDatasetConfig();
            Optional<AccelerationSettings> settings =
                getStoredReflectionSettingsForDataset(datasetConfig);
            Dataset dataset =
                toDatasetAPI(
                    datasetConfig,
                    settings.map(Dataset.RefreshSettings::new).orElse(null),
                    table.getDatasetMetadataState());
            return Optional.of(dataset);
          case FOLDER:
            final List<String> path = versionedDatasetId.getTableKey();
            VersionContext versionContext =
                versionedDatasetId.getVersionContext().asVersionContext();
            String refType = versionContext.getType().name();
            String refValue = versionContext.getValue();
            final Folder folder =
                createCatalogItemForVersionedFolder(path, id, includeChildren, refType, refValue);
            return Optional.of(folder);
          case UNKNOWN:
            logger.debug("Could not find entity with versioned id [{}]", id);
            return Optional.empty();
          default:
            logger.debug("Unrecognized entity type [{}] for versioned id [{}]", entityType, id);
            return Optional.empty();
        }
      } else {
        NameSpaceContainer container = namespaceService.getEntityById(id);
        if (container == null) {
          logger.debug("Could not find entity with id [{}]", id);
        }
        return Optional.ofNullable(container);
      }
    } catch (NamespaceException e) {
      logger.debug("Failed to get entity ", e);
      return Optional.empty();
    }
  }

  private Optional<CatalogEntity> getCatalogEntityFromCatalogItem(
      CatalogItem catalogItem, boolean includeChildren) throws NamespaceException {
    // can either be a folder or a file
    if (catalogItem.getContainerType() == CatalogItem.ContainerSubType.FOLDER) {
      final List<CatalogItem> children =
          includeChildren
              ? getListingForInternalItem(getPathFromInternalId(catalogItem.getId()))
              : Collections.emptyList();
      final Folder folder = new Folder(catalogItem.getId(), catalogItem.getPath(), null, children);
      return Optional.of(folder);
    } else if (catalogItem.getType() == CatalogItem.CatalogItemType.FILE) {
      final File file = new File(catalogItem.getId(), catalogItem.getPath());
      return Optional.of(file);
    }

    throw new RuntimeException(
        String.format("Could not retrieve internal item [%s]", catalogItem.toString()));
  }

  private List<CatalogItem> getListingForInternalItem(List<String> path) throws NamespaceException {
    NameSpaceContainer rootEntity = getNamespaceEntity(new NamespaceKey(path.get(0)));

    if (rootEntity.getType() == NameSpaceContainer.Type.SOURCE) {
      return getChildrenForPath(new NamespaceKey(path));
    }

    throw new IllegalArgumentException(
        String.format(
            "Can only get listing for sources, but [%s] is of type [%s]",
            path, rootEntity.getType()));
  }

  private Optional<CatalogItem> getInternalItemByPath(List<String> path) throws NamespaceException {
    NameSpaceContainer rootEntity = getNamespaceEntity(new NamespaceKey(path.get(0)));

    if (rootEntity != null && rootEntity.getType() == NameSpaceContainer.Type.SOURCE) {
      return Optional.of(getInternalItemFromSource(rootEntity.getSource(), path));
    } else {
      logger.warn("Can not find internal item with path [{}].", path);
      return Optional.empty();
    }
  }

  private CatalogItem getInternalItemFromSource(SourceConfig sourceConfig, List<String> path) {
    final StoragePlugin plugin = getStoragePlugin(sourceConfig.getName());

    if (!(plugin instanceof FileSystemPlugin)) {
      throw new IllegalArgumentException(
          String.format(
              "Can not get internal item from non-filesystem source [%s] of type [%s]",
              sourceConfig.getName(), plugin.getClass().getName()));
    }

    SchemaEntity entity =
        ((FileSystemPlugin) plugin).get(path, securityContext.getUserPrincipal().getName());

    return convertSchemaEntityToCatalogItem(entity, path.subList(0, path.size() - 1), null);
  }

  /**
   * This is only for namespace tree created for versioned plugin as regular fs plugin/non fs
   * creates promoted iceberg tables as entity type folder, whereas versioned creates them as type
   * dataset
   */
  private List<CatalogItem> convertVersionedNamespaceTreeToCatalogItems(NamespaceTree nsTree) {
    List<CatalogItem> items = new ArrayList<>();

    for (com.dremio.dac.model.folder.Folder folder : nsTree.getFolders()) {
      items.add(
          new CatalogItem.Builder()
              .setId(folder.getId())
              .setPath(folder.getFullPathList())
              .setType(CatalogItem.CatalogItemType.CONTAINER)
              .setContainerType(CatalogItem.ContainerSubType.FOLDER)
              .build());
    }

    for (com.dremio.dac.model.sources.PhysicalDataset physicalDataset :
        nsTree.getPhysicalDatasets()) {
      items.add(
          new CatalogItem.Builder()
              .setId(physicalDataset.getDatasetConfig().getId())
              .setPath(physicalDataset.getDatasetConfig().getFullPathList())
              .setType(CatalogItem.CatalogItemType.DATASET)
              .setDatasetType(CatalogItem.DatasetSubType.PROMOTED)
              .build());
    }

    for (com.dremio.dac.explore.model.Dataset dataset : nsTree.getDatasets()) {
      items.add(
          new CatalogItem.Builder()
              .setId(dataset.getDatasetConfig().getId())
              .setPath(dataset.getDatasetConfig().getFullPathList())
              .setType(CatalogItem.CatalogItemType.DATASET)
              .setDatasetType(CatalogItem.DatasetSubType.VIRTUAL)
              .build());
    }

    return items;
  }

  private CatalogItem convertSchemaEntityToCatalogItem(
      SchemaEntity entity,
      List<String> parentPath,
      @Nullable ImmutableSet<NamespaceKey> foldersInNamespace) {
    final List<String> entityPath = Lists.newArrayList(parentPath);

    // SchemaEntity will quote the final element in the path, which we don't want in the full path
    entityPath.add(PathUtils.removeQuotes(entity.getPath()));

    CatalogItem catalogItem = null;

    switch (entity.getType()) {
      case FILE:
        {
          catalogItem =
              new CatalogItem.Builder()
                  .setId(generateInternalId(entityPath))
                  .setPath(entityPath)
                  .setType(CatalogItem.CatalogItemType.FILE)
                  .build();
          break;
        }

      case FOLDER:
        {
          final NamespaceKey namespaceKey = new NamespaceKey(entityPath);

          // Check folder existence in KV store via cached set or direct call.
          boolean existsInNamespace;
          if (foldersInNamespace != null) {
            existsInNamespace = foldersInNamespace.contains(namespaceKey);
          } else {
            existsInNamespace = namespaceService.exists(namespaceKey);
          }

          if (existsInNamespace) {
            try {
              final FolderConfig folder = namespaceService.getFolder(namespaceKey);
              catalogItem =
                  new CatalogItem.Builder()
                      .setId(folder.getId().getId())
                      .setPath(entityPath)
                      .setType(CatalogItem.CatalogItemType.CONTAINER)
                      .setContainerType(CatalogItem.ContainerSubType.FOLDER)
                      .build();
            } catch (NamespaceException e) {
              logger.warn("Can not find item with path [{}]", entityPath, e);
            }
          } else {
            catalogItem =
                new CatalogItem.Builder()
                    .setId(generateInternalId(entityPath))
                    .setPath(entityPath)
                    .setType(CatalogItem.CatalogItemType.CONTAINER)
                    .setContainerType(CatalogItem.ContainerSubType.FOLDER)
                    .build();
          }
          break;
        }

      case FILE_TABLE:
      case FOLDER_TABLE:
        {
          try {
            final NamespaceKey namespaceKey =
                new NamespaceKey(PathUtils.toPathComponents(PathUtils.toFSPath(entityPath)));
            final DatasetConfig dataset = namespaceService.getDataset(namespaceKey);
            catalogItem =
                new CatalogItem.Builder()
                    .setId(dataset.getId().getId())
                    .setPath(entityPath)
                    .setType(CatalogItem.CatalogItemType.DATASET)
                    .setDatasetType(CatalogItem.DatasetSubType.PROMOTED)
                    .build();
          } catch (NamespaceException e) {
            logger.warn("Can not find item with path [{}]", entityPath, e);
          }
          break;
        }

      default:
        {
          throw new RuntimeException(
              String.format(
                  "Trying to convert unexpected schema entity [%s] of type [%s].",
                  entity.getPath(), entity.getType()));
        }
    }

    return catalogItem;
  }

  public List<CatalogItem> getChildrenForPath(NamespaceKey path) throws NamespaceException {
    return getChildrenForPath(path, null, null);
  }

  public List<CatalogItem> getChildrenForPath(NamespaceKey path, String refType, String refValue)
      throws NamespaceException {
    final List<CatalogItem> catalogItems = new ArrayList<>();

    // get parent info
    NameSpaceContainer rootEntity = getRootContainer(path.getPathComponents());

    if (rootEntity.getType() == NameSpaceContainer.Type.SOURCE) {
      catalogItems.addAll(
          getChildrenForSourcePath(rootEntity, path.getPathComponents(), refType, refValue));
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
        item.ifPresent(catalogItems::add);
      }
    } catch (NamespaceException e) {
      logger.warn(e.getMessage());
    }

    return catalogItems;
  }

  /** Returns all children of the listingPath for a source */
  protected List<CatalogItem> getChildrenForSourcePath(
      NameSpaceContainer source, List<String> listingPath, String refType, String refValue)
      throws NamespaceException {
    final List<CatalogItem> catalogItems = new ArrayList<>();
    final String sourceName = source.getSource().getName();
    final NamespaceKey listingKey = new NamespaceKey(listingPath);

    final StoragePlugin plugin = getStoragePlugin(sourceName);
    if (plugin.isWrapperFor(VersionedPlugin.class)) {
      final String userName = securityContext.getUserPrincipal().getName();
      final NamespaceTree namespaceTree =
          listingPath.size() > 1
              ? sourceService.listFolder(
                  new SourceName(source.getSource().getName()),
                  new SourceFolderPath(listingPath),
                  userName,
                  refType,
                  refValue)
              : sourceService.listSource(
                  new SourceName(source.getSource().getName()),
                  source.getSource(),
                  userName,
                  refType,
                  refValue);
      catalogItems.addAll(convertVersionedNamespaceTreeToCatalogItems(namespaceTree));
    } else if (plugin instanceof FileSystemPlugin) {
      // For file based plugins, use the list method to get the listing.  That code will merge in
      // any promoted datasets
      // that are in the namespace for us.  This is in line with what the UI does.
      final List<SchemaEntity> list =
          ((FileSystemPlugin) plugin)
              .list(listingPath, securityContext.getUserPrincipal().getName());

      // Check existence of folders in namespace in bulk.
      ImmutableSet<NamespaceKey> foldersInNamespace = getNamespaceFolderKeys(listingKey);

      for (SchemaEntity entity : list) {
        // Do not check for folder existence via namespace service as FileSystemPlugin converts such
        // folders
        // to FOLDER_TABLEs.
        final CatalogItem catalogItem =
            convertSchemaEntityToCatalogItem(entity, listingPath, foldersInNamespace);

        if (catalogItem != null) {
          catalogItems.add(catalogItem);
        }
      }
    } else {
      // for non-file based plugins we can go directly to the namespace
      catalogItems.addAll(getNamespaceChildrenForPath(listingKey));
    }

    return catalogItems;
  }

  private ImmutableSet<NamespaceKey> getNamespaceFolderKeys(NamespaceKey listingKey)
      throws NamespaceException {
    try {
      return namespaceService.list(listingKey).stream()
          .filter(entity -> entity.getType() == NameSpaceContainer.Type.FOLDER)
          .map(entity -> new NamespaceKey(entity.getFullPathList()))
          .collect(ImmutableSet.toImmutableSet());
    } catch (NamespaceNotFoundException e) {
      // If the root folder is not in the store, its children are not there either.
      return ImmutableSet.of();
    }
  }

  @WithSpan
  public CatalogEntity createCatalogItem(CatalogEntity entity)
      throws NamespaceException, UnsupportedOperationException, ExecutionSetupException {
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
      throw new UnsupportedOperationException(
          String.format(
              "Catalog item of type [%s] can not be edited", entity.getClass().getName()));
    }
  }

  protected CatalogEntity createDataset(Dataset dataset, NamespaceAttribute... attributes)
      throws NamespaceException {
    validateDataset(dataset);

    // only handle VDS
    Preconditions.checkArgument(
        dataset.getType() != Dataset.DatasetType.PHYSICAL_DATASET,
        "Physical Datasets can only be created by promoting other entities.");

    Preconditions.checkArgument(dataset.getId() == null, "Dataset id is immutable.");

    // verify we can save
    NamespaceKey topLevelKey = new NamespaceKey(dataset.getPath().get(0));
    NamespaceKey namespaceKey = new NamespaceKey(dataset.getPath());

    final boolean isVersionedSource =
        CatalogUtil.requestedPluginSupportsVersionedTables(namespaceKey, catalogSupplier.get());
    final boolean isVersionedViewEnabled = optionManager.getOption(VERSIONED_VIEW_ENABLED);
    if (isVersionedSource && !isVersionedViewEnabled) {
      throw UserException.unsupportedError()
          .message("Versioned view is not enabled")
          .buildSilently();
    }

    // Can create VDS in a space, home or versioned source
    NameSpaceContainer rootEntity = getNamespaceEntity(topLevelKey);
    List<NameSpaceContainer.Type> types =
        new ArrayList<>(Arrays.asList(NameSpaceContainer.Type.SPACE, NameSpaceContainer.Type.HOME));
    if (isVersionedSource) {
      types.add(NameSpaceContainer.Type.SOURCE);
    }
    Preconditions.checkArgument(
        types.contains(rootEntity.getType()),
        "Virtual datasets can only be saved into spaces, home space or versioned sources.");

    sabotContext
        .getViewCreator(securityContext.getUserPrincipal().getName())
        .createView(
            dataset.getPath(),
            dataset.getSql(),
            dataset.getSqlContext(),
            isVersionedSource,
            attributes);

    if (isVersionedSource) {
      DatasetConfig datasetConfig =
          CatalogUtil.getDatasetConfig(catalogSupplier.get(), namespaceKey);
      if (datasetConfig == null) {
        throw new RuntimeException(
            String.format("Could not retrieve newly created view [%s]!", namespaceKey));
      }
      Optional<AccelerationSettings> settings =
          getStoredReflectionSettingsForDataset(datasetConfig);
      return toDatasetAPI(
          datasetConfig, settings.map(Dataset.RefreshSettings::new).orElse(null), null);
    } else {
      NameSpaceContainer created = namespaceService.getEntityByPath(namespaceKey);
      return toDatasetAPI(created, null, null);
    }
  }

  /** Promotes the target to a PDS using the formatting options submitted via dataset. */
  @WithSpan
  public Dataset promoteToDataset(String targetId, Dataset dataset)
      throws NamespaceException, UnsupportedOperationException {
    Preconditions.checkArgument(
        dataset.getType() == Dataset.DatasetType.PHYSICAL_DATASET,
        "Promoting can only create physical datasets.");

    // The id can either be an internal id or a namespace id.  It will be a namespace id if the
    // entity had been promoted
    // before and then unpromoted.
    final List<String> path;
    if (isInternalId(targetId)) {
      path = getPathFromInternalId(targetId);
    } else {
      final NameSpaceContainer entityById = namespaceService.getEntityById(targetId);
      if (entityById == null) {
        throw new IllegalArgumentException(
            String.format("Could not find entity to promote with ud [%s]", targetId));
      }

      path = entityById.getFullPathList();
    }

    // getPathFromInternalId will return a path without quotes so make sure we do the same for the
    // dataset path
    List<String> normalizedPath =
        dataset.getPath().stream().map(PathUtils::removeQuotes).collect(Collectors.toList());
    Preconditions.checkArgument(
        normalizedPath.equals(path), "Entity id does not match the path specified in the dataset.");

    // validation
    validateDataset(dataset);
    Preconditions.checkArgument(
        dataset.getFormat() != null, "To promote a dataset, format settings are required.");

    NamespaceKey namespaceKey = new NamespaceKey(path);
    Optional<CatalogItem> catalogItem = getInternalItemByPath(path);

    if (!catalogItem.isPresent()) {
      throw new IllegalArgumentException(
          String.format("Could not find entity to promote with path [%s]", path));
    }

    // can only promote a file or folder from a source (which getInternalItemByPath verifies)
    if (catalogItem.get().getContainerType() == CatalogItem.ContainerSubType.FOLDER
        || catalogItem.get().getType() == CatalogItem.CatalogItemType.FILE) {
      PhysicalDatasetConfig physicalDatasetConfig = new PhysicalDatasetConfig();
      physicalDatasetConfig.setName(namespaceKey.getName());
      physicalDatasetConfig.setFormatSettings(dataset.getFormat().asFileConfig());

      if (catalogItem.get().getContainerType() == CatalogItem.ContainerSubType.FOLDER) {
        physicalDatasetConfig.setType(
            com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER);
      } else {
        physicalDatasetConfig.setType(
            com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET_SOURCE_FILE);
      }
      physicalDatasetConfig.setFullPathList(path);

      catalogSupplier
          .get()
          .createOrUpdateDataset(
              namespaceService,
              new NamespaceKey(namespaceKey.getRoot()),
              new PhysicalDatasetPath(path).toNamespaceKey(),
              toDatasetConfig(physicalDatasetConfig, securityContext.getUserPrincipal().getName()),
              getNamespaceAttributes(dataset));

      if (dataset.getAccelerationRefreshPolicy() != null) {
        reflectionServiceHelper
            .getReflectionSettings()
            .setReflectionSettings(
                namespaceKey, dataset.getAccelerationRefreshPolicy().toAccelerationSettings());
      }
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Can only promote a folder or a file but found [%s]", catalogItem.get().getType()));
    }

    return toDatasetAPI(
        namespaceService.getEntityByPath(namespaceKey),
        dataset.getAccelerationRefreshPolicy(),
        null);
  }

  private void updateDataset(Dataset dataset, NamespaceAttribute... attributes)
      throws NamespaceException, IOException {
    validateDataset(dataset);

    final boolean isVersionedSource =
        CatalogUtil.requestedPluginSupportsVersionedTables(
            new NamespaceKey(dataset.getPath()), catalogSupplier.get());
    if (isVersionedSource) {
      updateVersionedDataset(dataset);
    } else {
      updateNonVersionedDataset(dataset, attributes);
    }
  }

  private void updateVersionedDataset(Dataset dataset) throws IOException {
    validateVersionedDataset(dataset);

    NamespaceKey namespaceKey = new NamespaceKey(dataset.getPath());
    catalogSupplier.get().validatePrivilege(namespaceKey, SqlGrant.Privilege.ALTER);

    DremioTable currentView = catalogSupplier.get().getTable(dataset.getId());
    if (currentView == null) {
      throw new IllegalArgumentException(
          String.format("Could not find dataset with id [%s]", dataset.getId()));
    } else if (!(currentView instanceof ViewTable)) {
      throw UserException.validationError()
          .message(
              "Expecting getting a view but returns a entity of type %s",
              currentView.getDatasetConfig().getType())
          .buildSilently();
    } else if (!namespaceKey.equals(currentView.getPath())) {
      throw UserException.unsupportedError()
          .message("Renaming/moving a versioned view is not supported yet.")
          .buildSilently();
    }

    VersionContext versionContext =
        VersionedDatasetId.fromString(dataset.getId()).getVersionContext().asVersionContext();
    Map<String, VersionContext> contextMap =
        ImmutableMap.of(namespaceKey.getRoot(), versionContext);
    SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
    View view = getViewAndUpdateBatchSchema(dataset, contextMap, schemaBuilder);

    ResolvedVersionContext resolvedVersionContext =
        CatalogUtil.resolveVersionContext(
            catalogSupplier.get(), namespaceKey.getRoot(), versionContext);
    CatalogUtil.validateResolvedVersionIsBranch(resolvedVersionContext);
    final ViewOptions viewOptions =
        new ViewOptions.ViewOptionsBuilder()
            .version(resolvedVersionContext)
            .batchSchema(schemaBuilder.build())
            .actionType(ViewOptions.ActionType.UPDATE_VIEW)
            .build();

    catalogSupplier.get().updateView(namespaceKey, view, viewOptions);
    catalogSupplier.get().clearDatasetCache(namespaceKey, currentView.getVersionContext());
  }

  private void validateVersionedDataset(Dataset dataset) {
    Preconditions.checkArgument(
        VersionedDatasetId.isVersionedDatasetId(dataset.getId()),
        "Versioned Dataset Id must be provided for updating versioned dataset.");
    Preconditions.checkArgument(
        dataset.getType() == Dataset.DatasetType.VIRTUAL_DATASET,
        "Updating versioned table is not supported yet.");
    Preconditions.checkArgument(
        dataset.getPath().size() > 1,
        "View path " + PathUtils.constructFullPath(dataset.getPath()) + " is not valid.");

    if (!optionManager.getOption(VERSIONED_VIEW_ENABLED)) {
      throw UserException.unsupportedError()
          .message("Versioned view is not enabled")
          .buildSilently();
    }

    if (ParserUtil.checkTimeTravelOnView(dataset.getSql())) {
      throw UserException.unsupportedError()
          .message(
              "Versioned views not supported for time travel queries. Please use AT TAG or AT COMMIT instead")
          .buildSilently();
    }
  }

  private View getViewAndUpdateBatchSchema(
      Dataset dataset, Map<String, VersionContext> contextMap, SchemaBuilder schemaBuilder) {
    final SqlQuery query =
        new SqlQuery(
            dataset.getSql(),
            dataset.getSqlContext(),
            securityContext.getUserPrincipal().getName());
    QueryMetadata queryMetadata = QueryParser.extract(query, sabotContext);

    validateParsedViewQuery(queryMetadata.getSqlNode());
    validateVersions(query, contextMap);

    for (RelDataTypeField f : queryMetadata.getRowType().getFieldList()) {
      CalciteArrowHelper.fieldFromCalciteRowType(f.getKey(), f.getValue())
          .ifPresent(schemaBuilder::addField);
    }

    return new View(
        PathUtils.constructFullPath(dataset.getPath()),
        query.getSql(),
        queryMetadata.getRowType(),
        null,
        dataset.getSqlContext());
  }

  private void validateParsedViewQuery(Optional<SqlNode> viewQuery) {
    if (!viewQuery.isPresent()) {
      throw UserException.unsupportedError().message("Invalid view query.").buildSilently();
    }
    ParserUtil.validateParsedViewQuery(viewQuery.get());
  }

  private void validateVersions(SqlQuery query, Map<String, VersionContext> sourceVersionMapping) {
    try {
      QueryParser.validateVersions(query, sabotContext, sourceVersionMapping);
    } catch (ValidationException | RelConversionException e) {
      // Calcite exception could wrap exceptions in layers.  Find the root cause to get the original
      // error message.
      Throwable rootCause = e;
      while (rootCause.getCause() != null && rootCause.getCause() != rootCause) {
        rootCause = rootCause.getCause();
      }
      throw UserException.validationError().message(rootCause.getMessage()).buildSilently();
    } catch (Exception e) {
      throw UserException.validationError()
          .message("Validation of view sql failed. %s ", e.getMessage())
          .buildSilently();
    }
  }

  private void updateNonVersionedDataset(Dataset dataset, NamespaceAttribute... attributes)
      throws NamespaceException, IOException {
    Preconditions.checkArgument(dataset.getId() != null, "Dataset Id is missing.");

    NameSpaceContainer container = namespaceService.getEntityById(dataset.getId());
    DatasetConfig currentDatasetConfig = container.getDataset();
    if (currentDatasetConfig == null) {
      throw new IllegalArgumentException(
          String.format("Could not find dataset with id [%s]", dataset.getId()));
    }

    // use the version of the dataset to check for concurrency issues
    currentDatasetConfig.setTag(dataset.getTag());

    NamespaceKey namespaceKey = new NamespaceKey(dataset.getPath());

    // check type
    final DatasetType type = currentDatasetConfig.getType();

    if (dataset.getType() == Dataset.DatasetType.PHYSICAL_DATASET) {
      // cannot change the path of a physical dataset
      Preconditions.checkArgument(
          dataset.getPath().equals(currentDatasetConfig.getFullPathList()),
          "Dataset path can not be modified.");
      Preconditions.checkArgument(type != VIRTUAL_DATASET, "Dataset type can not be modified");

      // PDS specific config
      currentDatasetConfig
          .getPhysicalDataset()
          .setAllowApproxStats(dataset.getApproximateStatisticsAllowed());

      if (type
          == com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET_HOME_FILE) {
        DatasetConfig datasetConfig =
            toDatasetConfig(
                dataset.getFormat().asFileConfig(),
                type,
                securityContext.getUserPrincipal().getName(),
                currentDatasetConfig.getId());

        catalogSupplier
            .get()
            .createOrUpdateDataset(
                namespaceService,
                new NamespaceKey(HomeFileSystemStoragePlugin.HOME_PLUGIN_NAME),
                namespaceKey,
                datasetConfig,
                attributes);
      } else if (type
              == com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET_SOURCE_FILE
          || type
              == com.dremio.service.namespace.dataset.proto.DatasetType
                  .PHYSICAL_DATASET_SOURCE_FOLDER) {
        Preconditions.checkArgument(
            dataset.getFormat() != null, "Promoted dataset needs to have a format set.");

        // only thing that can change is the formatting
        currentDatasetConfig
            .getPhysicalDataset()
            .setFormatSettings(dataset.getFormat().asFileConfig());

        catalogSupplier
            .get()
            .createOrUpdateDataset(
                namespaceService,
                new NamespaceKey(namespaceKey.getRoot()),
                namespaceKey,
                currentDatasetConfig,
                attributes);
      } else {
        catalogSupplier
            .get()
            .createOrUpdateDataset(
                namespaceService,
                new NamespaceKey(namespaceKey.getRoot()),
                namespaceKey,
                currentDatasetConfig,
                attributes);
      }

      // update refresh settings
      Optional<AccelerationSettings> storedReflectionSettingsForDataset =
          getStoredReflectionSettingsForDataset(currentDatasetConfig);
      if (dataset.getAccelerationRefreshPolicy() == null
          && storedReflectionSettingsForDataset.isPresent()) {
        // we are clearing the acceleration settings for the dataset
        reflectionServiceHelper.getReflectionSettings().removeSettings(namespaceKey);
      } else if (dataset.getAccelerationRefreshPolicy() != null) {
        reflectionServiceHelper
            .getReflectionSettings()
            .setReflectionSettings(
                namespaceKey, dataset.getAccelerationRefreshPolicy().toAccelerationSettings());
      }
    } else if (dataset.getType() == Dataset.DatasetType.VIRTUAL_DATASET) {
      Preconditions.checkArgument(type == VIRTUAL_DATASET, "Dataset type can not be modified");
      VirtualDataset virtualDataset = currentDatasetConfig.getVirtualDataset();

      // Check if the dataset is being renamed
      if (!Objects.equals(currentDatasetConfig.getFullPathList(), dataset.getPath())) {
        datasetVersionMutator.renameDataset(
            new DatasetPath(currentDatasetConfig.getFullPathList()),
            new DatasetPath(dataset.getPath()));
        currentDatasetConfig = namespaceService.getDataset(namespaceKey);
      }

      virtualDataset.setSql(dataset.getSql());
      virtualDataset.setContextList(dataset.getSqlContext());
      currentDatasetConfig.setVirtualDataset(virtualDataset);

      List<String> path = dataset.getPath();

      View view =
          new View(
              path.get(path.size() - 1),
              dataset.getSql(),
              Collections.emptyList(),
              null,
              virtualDataset.getContextList(),
              false);
      catalogSupplier
          .get()
          .updateView(
              namespaceKey,
              view,
              null,
              attributes); // ViewOption will be null because this is unrelated to version context
    }
  }

  private void deleteDataset(DatasetConfig config, String tag)
      throws NamespaceException, UnsupportedOperationException, IOException {
    // if no tag is passed in, use the latest version
    String version = config.getTag();

    if (tag != null) {
      version = tag;
    }

    switch (config.getType()) {
      case PHYSICAL_DATASET:
        {
          throw new UnsupportedOperationException("A physical dataset can not be deleted.");
        }

      case PHYSICAL_DATASET_SOURCE_FILE:
      case PHYSICAL_DATASET_SOURCE_FOLDER:
        {
          // remove the formatting
          removeFormatFromDataset(config, version);
          break;
        }

      case PHYSICAL_DATASET_HOME_FILE:
      case PHYSICAL_DATASET_HOME_FOLDER:
        {
          deleteHomeDataset(config, version, config.getFullPathList());
          break;
        }

      case VIRTUAL_DATASET:
        {
          namespaceService.deleteDataset(new NamespaceKey(config.getFullPathList()), version);
          break;
        }

      default:
        {
          throw new RuntimeException(
              String.format(
                  "Dataset [%s] of unknown type [%s] found.",
                  config.getId().getId(), config.getType()));
        }
    }
  }

  public void deleteHomeDataset(DatasetConfig config, String version, List<String> pathComponents)
      throws IOException, NamespaceException {
    FileConfig formatSettings = config.getPhysicalDataset().getFormatSettings();
    Preconditions.checkArgument(
        pathComponents != null && !pathComponents.isEmpty(), "Cannot find path to dataset");

    // TODO: Probably should be combined into one call for safe home file deletion.
    if (homeFileTool.fileExists(formatSettings.getLocation())) {
      homeFileTool.deleteFile(formatSettings.getLocation());
    }
    namespaceService.deleteDataset(new NamespaceKey(pathComponents), version);
  }

  public void removeFormatFromDataset(DatasetConfig config, String version) {
    PhysicalDatasetPath datasetPath = new PhysicalDatasetPath(config.getFullPathList());
    sourceService.deletePhysicalDataset(
        datasetPath.getSourceName(),
        datasetPath,
        version,
        CatalogUtil.getDeleteCallback(sabotContext.getOrphanageFactory().get()));
  }

  private void validateDataset(Dataset dataset) {
    Preconditions.checkArgument(dataset.getType() != null, "Dataset type is required.");
    Preconditions.checkArgument(dataset.getPath() != null, "Dataset path is required.");
    Preconditions.checkArgument(
        dataset.getPath().size() >= 2, "Dataset path should be fully qualified.");

    if (dataset.getType() == Dataset.DatasetType.VIRTUAL_DATASET) {
      // VDS requires sql
      Preconditions.checkArgument(
          dataset.getSql() != null, "Virtual dataset must have sql defined.");
      Preconditions.checkArgument(
          dataset.getSql().trim().length() > 0, "Virtual dataset cannot have empty sql defined.");
      Preconditions.checkArgument(
          dataset.getFormat() == null, "Virtual dataset cannot have a format defined.");
      Preconditions.checkArgument(
          dataset.getApproximateStatisticsAllowed() == null,
          "Virtual dataset cannot have a approximateStatisticsAllowed defined.");
      ParserUtil.validateViewQuery(dataset.getSql());
    } else {
      // PDS
      Preconditions.checkArgument(
          dataset.getSql() == null, "Physical dataset can not have sql defined.");
      Preconditions.checkArgument(
          dataset.getSqlContext() == null, "Physical dataset can not have sql context defined.");
    }
  }

  protected CatalogEntity createSpace(Space space, NamespaceAttribute... attributes)
      throws NamespaceException {
    String spaceName = space.getName();

    Preconditions.checkArgument(space.getId() == null, "Space id is immutable.");
    Preconditions.checkArgument(spaceName != null, "Space name is required.");
    Preconditions.checkArgument(spaceName.trim().length() > 0, "Space name cannot be empty.");

    // TODO: move the space name validation somewhere reusable instead of having to create a new
    // SpaceName
    new SpaceName(spaceName);

    NamespaceKey namespaceKey = new NamespaceKey(spaceName);

    // check if space already exists with the given name.
    if (namespaceService.exists(namespaceKey, NameSpaceContainer.Type.SPACE)) {
      throw new ConcurrentModificationException(
          String.format("A space with the name [%s] already exists.", spaceName));
    }

    namespaceService.addOrUpdateSpace(
        namespaceKey, getSpaceConfig(space).setCtime(System.currentTimeMillis()), attributes);

    return toSpaceAPI(namespaceService.getEntityByPath(namespaceKey), null);
  }

  protected void updateSpace(Space space, NamespaceAttribute... attributes)
      throws NamespaceException {
    NamespaceKey namespaceKey = new NamespaceKey(space.getName());
    SpaceConfig spaceConfig = namespaceService.getSpace(namespaceKey);

    Preconditions.checkArgument(
        space.getName().equals(spaceConfig.getName()), "Space name is immutable.");

    namespaceService.addOrUpdateSpace(
        namespaceKey, getSpaceConfig(space).setCtime(spaceConfig.getCtime()), attributes);
  }

  protected void deleteSpace(SpaceConfig spaceConfig, String version) throws NamespaceException {
    namespaceService.deleteSpace(new NamespaceKey(spaceConfig.getName()), version);
  }

  protected CatalogEntity createSource(Source source, NamespaceAttribute... attributes)
      throws NamespaceException, ExecutionSetupException {
    SourceConfig sourceConfig = sourceService.createSource(source.toSourceConfig(), attributes);
    return toSourceAPI(
        namespaceService.getEntityById(sourceConfig.getId().getId()),
        getChildrenForPath(new NamespaceKey(sourceConfig.getName())));
  }

  @WithSpan
  public CatalogEntity updateCatalogItem(CatalogEntity entity, String id)
      throws NamespaceException,
          UnsupportedOperationException,
          ExecutionSetupException,
          IOException {
    Preconditions.checkArgument(entity.getId() != null, "Entity id is required.");
    Preconditions.checkArgument(entity.getId().equals(id), "Ids must match.");
    String finalId = id;

    if (entity instanceof Dataset) {
      Span.current().setAttribute("dremio.catalog.entityType", "Dataset");
      Dataset dataset = (Dataset) entity;
      updateDataset(dataset, getNamespaceAttributes(entity));
    } else if (entity instanceof Source) {
      Span.current().setAttribute("dremio.catalog.entityType", "Source");
      Source source = (Source) entity;
      sourceService.updateSource(id, source.toSourceConfig(), getNamespaceAttributes(entity));
    } else if (entity instanceof Space) {
      Span.current().setAttribute("dremio.catalog.entityType", "Space");
      Space space = (Space) entity;
      updateSpace(space, getNamespaceAttributes(space));
    } else if (entity instanceof Folder) {
      Span.current().setAttribute("dremio.catalog.entityType", "Folder");
      Folder folder = (Folder) entity;
      FolderConfig folderConfig = updateFolder(folder, getNamespaceAttributes(entity));
      finalId = folderConfig.getId().getId();
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Catalog item [%s] of type [%s] can not be edited.",
              id, entity.getClass().getName()));
    }

    // TODO(DX-18416) What to do?
    Optional<CatalogEntity> newEntity =
        getCatalogEntityById(finalId, ImmutableList.of(), Collections.emptyList());

    if (newEntity.isPresent()) {
      return newEntity.get();
    } else {
      throw new RuntimeException(
          String.format(
              "Catalog item [%s] of type [%s] could not be found",
              id, entity.getClass().getName()));
    }
  }

  @WithSpan
  public void deleteCatalogItem(String id, String tag)
      throws NamespaceException, UnsupportedOperationException {
    Optional<?> entity = getById(id, false);

    if (!entity.isPresent()) {
      throw new IllegalArgumentException(String.format("Could not find entity with id [%s].", id));
    }

    Object object = entity.get();

    if (object instanceof NameSpaceContainer) {
      deleteCatalogItemFromNamespace((NameSpaceContainer) object, tag);
    } else if (object instanceof CatalogEntity && VersionedDatasetId.isVersionedDatasetId(id)) {
      deleteCatalogEntityFromVersionedPlugin((CatalogEntity) object);
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Catalog item [%s] of type [%s] can not be deleted.",
              id, object.getClass().getName()));
    }
  }

  private void deleteCatalogItemFromNamespace(NameSpaceContainer container, String tag)
      throws NamespaceException, UnsupportedOperationException {
    switch (container.getType()) {
      case SOURCE:
        Span.current().setAttribute("dremio.catalog.entityType", "Source");
        SourceConfig config = container.getSource();

        if (tag != null) {
          config.setTag(tag);
        }

        sourceService.deleteSource(config);
        break;
      case SPACE:
        Span.current().setAttribute("dremio.catalog.entityType", "Space");
        SpaceConfig spaceConfig = container.getSpace();

        String version = spaceConfig.getTag();

        if (tag != null) {
          version = tag;
        }
        deleteSpace(spaceConfig, version);
        break;
      case DATASET:
        Span.current().setAttribute("dremio.catalog.entityType", "Dataset");

        DatasetConfig datasetConfig = container.getDataset();

        try {
          deleteDataset(datasetConfig, tag);
        } catch (IOException e) {
          throw new IllegalArgumentException(e);
        }
        break;
      case FOLDER:
        Span.current().setAttribute("dremio.catalog.entityType", "Folder");

        FolderConfig folderConfig = container.getFolder();

        String folderVersion = folderConfig.getTag();

        if (tag != null) {
          folderVersion = tag;
        }

        namespaceService.deleteFolder(
            new NamespaceKey(folderConfig.getFullPathList()), folderVersion);
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Namespace container of type [%s] can not be deleted.", container.getType()));
    }
  }

  private void deleteCatalogEntityFromVersionedPlugin(CatalogEntity entity) {
    VersionedDatasetId id = VersionedDatasetId.tryParse(entity.getId());
    Preconditions.checkArgument(id != null);
    if (entity instanceof Folder) {
      Folder folder = (Folder) entity;
      SourceFolderPath folderPath = new SourceFolderPath(folder.getPath());
      VersionContext versionContext = id.getVersionContext().asVersionContext();
      sourceService.deleteFolder(
          folderPath, versionContext.getType().name(), versionContext.getValue());
    } else if (entity instanceof Dataset) {
      Dataset dataset = (Dataset) entity;
      NamespaceKey namespaceKey = new NamespaceKey(dataset.getPath());
      ResolvedVersionContext resolvedVersionContext =
          CatalogUtil.resolveVersionContext(
              catalogSupplier.get(),
              id.getTableKey().get(0),
              id.getVersionContext().asVersionContext());
      if (dataset.getType() == Dataset.DatasetType.PHYSICAL_DATASET) {
        catalogSupplier.get().validatePrivilege(namespaceKey, SqlGrant.Privilege.DROP);
        TableMutationOptions tableMutationOptions =
            TableMutationOptions.newBuilder()
                .setResolvedVersionContext(resolvedVersionContext)
                .build();
        catalogSupplier.get().dropTable(namespaceKey, tableMutationOptions);
      } else if (dataset.getType() == Dataset.DatasetType.VIRTUAL_DATASET) {
        catalogSupplier.get().validatePrivilege(namespaceKey, SqlGrant.Privilege.ALTER);
        ViewOptions viewOptions =
            new ViewOptions.ViewOptionsBuilder().version(resolvedVersionContext).build();
        try {
          catalogSupplier.get().dropView(namespaceKey, viewOptions);
        } catch (IOException e) {
          throw new RuntimeException(String.format("View [%s] could not be dropped", id), e);
        }
      } else {
        throw new UnsupportedOperationException(
            String.format(
                "Deleting dataset [%s] of type [%s] is not supported.",
                dataset.getId(), dataset.getType()));
      }
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Catalog entity [%s] of type [%s] can not be deleted.",
              entity.getId(), entity.getClass().getSimpleName()));
    }
  }

  protected Folder createFolder(Folder folder, NamespaceAttribute... attributes)
      throws NamespaceException {
    validateFolder(folder);
    final boolean isVersionedSource =
        CatalogUtil.requestedPluginSupportsVersionedTables(
            new NamespaceKey(folder.getPath()), catalogSupplier.get());
    if (isVersionedSource) {
      return createFolderInVersionedSource(folder, null, null, attributes);
    } else {
      return createFolderInNamespace(folder, attributes);
    }
  }

  private void validateFolder(Folder folder) {
    Preconditions.checkArgument(
        CollectionUtils.isNotEmpty(folder.getPath()), "Folder path can't be empty.");
    Preconditions.checkArgument(
        folder.getPath().size() >= 2, "Folder path should be fully qualified.");
  }

  private Folder createFolderInVersionedSource(
      Folder folder,
      final String refType,
      final String refValue,
      NamespaceAttribute... attributes) {
    SourceFolderPath folderPath = new SourceFolderPath(folder.getPath());
    com.dremio.dac.model.folder.Folder createdFolder =
        sourceService.createFolder(
            new SourceName(folder.getPath().get(0)),
            folderPath,
            securityContext.getUserPrincipal().getName(),
            null,
            null);
    return toFolderAPI(createdFolder, null);
  }

  private Folder createFolderInNamespace(Folder folder, NamespaceAttribute... attributes)
      throws NamespaceException {
    NamespaceKey parentKey =
        new NamespaceKey(folder.getPath().subList(0, folder.getPath().size() - 1));
    List<NameSpaceContainer> entities =
        namespaceService.getEntities(Collections.singletonList(parentKey));

    NameSpaceContainer container = entities.get(0);

    if (container == null) {
      // If we can't find it by id, maybe it is not in the namespace.
      throw new IllegalArgumentException(
          String.format("Could not find entity with path [%s].", folder.getPath()));
    }

    NamespaceKey key = new NamespaceKey(folder.getPath());

    switch (container.getType()) {
      case SPACE:
      case HOME:
      case FOLDER:
        {
          namespaceService.addOrUpdateFolder(key, getFolderConfig(folder), attributes);
          break;
        }

      default:
        {
          throw new UnsupportedOperationException(
              String.format("Can not create a folder inside a [%s].", container.getType()));
        }
    }
    return toFolderAPI(namespaceService.getEntityByPath(key), null);
  }

  protected FolderConfig updateFolder(Folder folder, NamespaceAttribute... attributes)
      throws NamespaceException {
    final NameSpaceContainer rootContainer = getRootContainer(folder.getPath());
    NamespaceKey namespaceKey = new NamespaceKey(folder.getPath());

    // convert non-ns folder to folder
    if (rootContainer.getType() == NameSpaceContainer.Type.SOURCE && isInternalId(folder.getId())) {
      namespaceKey = new NamespaceKey(getPathFromInternalId(folder.getId()));
      FolderConfig config = getFolderConfig(folder);
      config.setId(new EntityId(UUID.randomUUID().toString()));
      namespaceService.addOrUpdateFolder(namespaceKey, config, attributes);
    } else {
      FolderConfig folderConfig = namespaceService.getFolder(namespaceKey);

      Preconditions.checkArgument(
          folder.getPath().equals(folderConfig.getFullPathList()), "Folder path is immutable.");

      namespaceService.addOrUpdateFolder(namespaceKey, getFolderConfig(folder), attributes);
    }

    return namespaceService.getFolder(namespaceKey);
  }

  public Source toSourceAPI(NameSpaceContainer container, List<CatalogItem> children) {
    // TODO: clean up source config creation, move it all into this class
    return sourceService.fromSourceConfig(container.getSource(), children);
  }

  /** Refresh a catalog item. Only supports datasets currently. */
  @WithSpan
  public void refreshCatalogItem(String id) throws UnsupportedOperationException {
    DatasetConfig config = CatalogUtil.getDatasetConfig(catalogSupplier.get(), id);
    if (config == null) {
      throw new IllegalArgumentException(String.format("Could not find dataset with id [%s].", id));
    }
    reflectionServiceHelper.refreshReflectionsForDataset(config.getId().getId());
  }

  private Optional<AccelerationSettings> getStoredReflectionSettingsForDataset(
      DatasetConfig datasetConfig) {
    final String id = datasetConfig.getId().getId();
    final VersionedDatasetId versionedDatasetId = VersionedDatasetId.tryParse(id);
    final CatalogEntityKey.Builder builder = CatalogEntityKey.newBuilder();

    if (versionedDatasetId == null) {
      builder.keyComponents(datasetConfig.getFullPathList());
    } else {
      builder
          .keyComponents(versionedDatasetId.getTableKey())
          .tableVersionContext(versionedDatasetId.getVersionContext());
    }

    return reflectionServiceHelper
        .getReflectionSettings()
        .getStoredReflectionSettings(builder.build());
  }

  public Dataset toDatasetAPI(
      NameSpaceContainer container,
      @Nullable Dataset.RefreshSettings refreshSettings,
      @Nullable DatasetMetadataState metadataState) {
    return toDatasetAPI(container.getDataset(), refreshSettings, metadataState);
  }

  public Dataset toDatasetAPI(
      DatasetConfig config,
      @Nullable Dataset.RefreshSettings refreshSettings,
      @Nullable DatasetMetadataState metadataState) {

    Dataset dataset;
    if (config.getType() == VIRTUAL_DATASET) {
      String sql = null;
      List<String> sqlContext = null;

      VirtualDataset virtualDataset = config.getVirtualDataset();
      if (virtualDataset != null) {
        sql = virtualDataset.getSql();
        sqlContext = virtualDataset.getContextList();
      }

      dataset =
          new Dataset(
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
              null);
    } else {
      FileFormat format = null;

      PhysicalDataset physicalDataset = config.getPhysicalDataset();
      if (physicalDataset != null) {
        FileConfig formatSettings = physicalDataset.getFormatSettings();

        if (formatSettings != null) {
          if (config.getType()
                  == com.dremio.service.namespace.dataset.proto.DatasetType
                      .PHYSICAL_DATASET_SOURCE_FILE
              || config.getType()
                  == com.dremio.service.namespace.dataset.proto.DatasetType
                      .PHYSICAL_DATASET_HOME_FILE) {
            format = FileFormat.getForFile(formatSettings);
          } else {
            format = FileFormat.getForFolder(formatSettings);
          }
        }
      }

      dataset =
          new Dataset(
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
              (physicalDataset != null) ? physicalDataset.getAllowApproxStats() : Boolean.FALSE);
    }

    if (metadataState != null) {
      dataset.setMetadataExpired(
          metadataState.isExpired(), metadataState.lastRefreshTimeMillis().orElse(null));
    }

    return dataset;
  }

  private static Home getHomeFromConfig(HomeConfig config, List<CatalogItem> children) {
    return new Home(
        config.getId().getId(),
        HomeName.getUserHomePath(config.getOwner()).toString(),
        String.valueOf(config.getTag()),
        children);
  }

  protected Space toSpaceAPI(NameSpaceContainer container, List<CatalogItem> children) {
    SpaceConfig config = container.getSpace();
    return new Space(
        config.getId().getId(),
        config.getName(),
        String.valueOf(config.getTag()),
        config.getCtime(),
        children);
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

  protected Folder toFolderAPI(NameSpaceContainer container, List<CatalogItem> children) {
    FolderConfig config = container.getFolder();
    return new Folder(config.getId().getId(), config.getFullPathList(), config.getTag(), children);
  }

  protected Folder toFolderAPI(FolderConfig config, List<CatalogItem> children) {
    return new Folder(config.getId().getId(), config.getFullPathList(), config.getTag(), children);
  }

  protected Folder toFolderAPI(
      com.dremio.dac.model.folder.Folder createdFolder, List<CatalogItem> children) {
    return new Folder(createdFolder.getId(), createdFolder.getFullPathList(), null, null);
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

  // Catalog items that are not in the namespace (files/folders) in file-based sources are given a
  // fake id that
  // is dremio:/path/to/entity - the prefix helps us distinguish between fake and real ids.
  private static final String INTERNAL_ID_PREFIX = "dremio:";

  public static String generateInternalId(List<String> path) {
    return INTERNAL_ID_PREFIX + com.dremio.common.utils.PathUtils.toFSPathString(path);
  }

  private static boolean isInternalId(String id) {
    return id != null && id.startsWith(INTERNAL_ID_PREFIX);
  }

  public static List<String> getPathFromInternalId(String id) {
    return com.dremio.common.utils.PathUtils.toPathComponents(
        id.substring(INTERNAL_ID_PREFIX.length()));
  }

  protected StoragePlugin getStoragePlugin(String sourceName) {
    final StoragePlugin plugin = catalogSupplier.get().getSource(sourceName);

    if (plugin == null) {
      throw new SourceNotFoundException(sourceName);
    }

    return plugin;
  }

  public List<SearchContainer> searchByQuery(String query) throws NamespaceException {
    return searchService.search(query, null);
  }

  @WithSpan
  public List<CatalogItem> search(String query) throws NamespaceException {
    List<SearchContainer> searchResults = searchByQuery(query);

    return searchResults.stream()
        .map(
            searchResult -> {
              return CatalogItem.fromNamespaceContainer(searchResult.getNamespaceContainer());
            })
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
  }

  private List<CatalogItem> applyAdditionalInfoToContainers(
      final List<CatalogItem> items, final List<DetailType> include) {
    Stream<CatalogItem.Builder> resultList = items.stream().map(CatalogItem.Builder::new);

    for (DetailType detail : include) {
      resultList = detail.addInfo(resultList, this);
    }

    return resultList.map(CatalogItem.Builder::build).collect(Collectors.toList());
  }

  public void ensureUserHasHomespace(String userName) {
    try {
      CatalogServiceHelper.ensureUserHasHomespace(
          sabotContext.getNamespaceService(SystemUser.SYSTEM_USERNAME), userName);
    } catch (NamespaceException ignored) {
      logger.warn("Could not ensure user has homespace.");
    }
  }

  public static void ensureUserHasHomespace(
      NamespaceService namespaceService, String userName, OptionManager optionManager)
      throws NamespaceException {
    ensureUserHasHomespace(namespaceService, userName);
  }

  private static void ensureUserHasHomespace(NamespaceService namespaceService, String userName)
      throws NamespaceException {
    final NamespaceKey homeKey = new HomePath(HomeName.getUserHomePath(userName)).toNamespaceKey();
    try {
      if (!namespaceService.exists(homeKey, NameSpaceContainer.Type.HOME)) {
        namespaceService.addOrUpdateHome(
            homeKey, new HomeConfig().setCtime(System.currentTimeMillis()).setOwner(userName));
      }
    } catch (NamespaceException ex) {
      if (!namespaceService.exists(homeKey, NameSpaceContainer.Type.HOME)) {
        throw ex;
      }
    } catch (ConcurrentModificationException ignored) {
      // Ignore, ConcurrentModificationException is only thrown when the key already exists
    }
  }

  /**
   * Retrieve the children for a catalog entity based on the entity path. If the path represents a
   * versioned source, use VersionedPlugin to look for the children; Otherwise, look for them in
   * Namespace. So far this is specifically used by Autocomplete V2.
   */
  public List<CatalogItem> getCatalogChildrenForPath(
      List<String> path, String refType, String refValue) {
    NamespaceKey key = new NamespaceKey(path);
    try {
      final StoragePlugin plugin = getStoragePlugin(path.get(0));
      if (plugin.isWrapperFor(VersionedPlugin.class)) {
        return getChildrenForVersionedSourcePath(
            plugin.unwrap(VersionedPlugin.class), key, refType, refValue);
      }
    } catch (UserException | SourceNotFoundException ignored) {
    }

    return getNamespaceChildrenForPath(key);
  }

  private List<CatalogItem> getChildrenForVersionedSourcePath(
      VersionedPlugin plugin, NamespaceKey sourceKey, String refType, String refValue) {
    final List<CatalogItem> catalogItems = new ArrayList<>();
    VersionContext version = VersionContextUtils.parse(refType, refValue);
    try {
      Stream<ExternalNamespaceEntry> entities =
          plugin.listEntries(sourceKey.getPathWithoutRoot(), version);
      catalogItems.addAll(generateCatalogItemList(sourceKey.getRoot(), entities));
    } catch (ReferenceNotFoundException
        | NoDefaultBranchException
        | ReferenceTypeConflictException ignored) {
    }
    return catalogItems;
  }

  private List<CatalogItem> generateCatalogItemList(
      String sourceName, Stream<ExternalNamespaceEntry> entities) {
    return entities
        .map(
            (entity) -> {
              CatalogItem.Builder builder = new CatalogItem.Builder();
              switch (entity.getType()) {
                case FOLDER:
                  builder
                      .setType(CatalogItem.CatalogItemType.CONTAINER)
                      .setContainerType(CatalogItem.ContainerSubType.FOLDER);
                  break;

                case ICEBERG_VIEW:
                  builder
                      .setType(CatalogItem.CatalogItemType.DATASET)
                      .setDatasetType(CatalogItem.DatasetSubType.VIRTUAL);
                  break;

                case ICEBERG_TABLE:
                  builder
                      .setType(CatalogItem.CatalogItemType.DATASET)
                      .setDatasetType(CatalogItem.DatasetSubType.DIRECT);
                  break;

                case UNKNOWN:
                default:
                  // ignore UNKNOWN entities
                  return null;
              }
              return builder
                  .setId(entity.getId())
                  .setPath(
                      Stream.concat(Stream.of(sourceName), entity.getNameElements().stream())
                          .collect(Collectors.toList()))
                  .build();
            })
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  public Stream<ReferenceInfo> getReferencesForVersionedSource(
      String sourceName, SuggestionsType refType) throws SourceNotFoundException {
    try {
      final StoragePlugin plugin = getStoragePlugin(sourceName);
      if (!(plugin.isWrapperFor(VersionedPlugin.class))) {
        throw new SourceNotFoundException(sourceName + " is not a versioned source.");
      }
      switch (refType) {
        case BRANCH:
          return (plugin.unwrap(VersionedPlugin.class)).listBranches();

        case TAG:
          return (plugin.unwrap(VersionedPlugin.class)).listTags();

        case REFERENCE:
          return (plugin.unwrap(VersionedPlugin.class)).listReferences();

        default:
          throw new RuntimeException("Unknown reference type " + refType);
      }
    } catch (UserException e) {
      if (e.getErrorType() == UserBitShared.DremioPBError.ErrorType.VALIDATION
          && e.getCause().toString().contains("NamespaceNotFoundException")) {
        return Stream.empty();
      }
      throw e;
    }
  }

  private VersionedPlugin.EntityType getVersionedEntityType(
      List<String> fullPath, VersionContext versionContext) {
    try {
      return CatalogUtil.getVersionedEntityType(catalogSupplier.get(), fullPath, versionContext);
    } catch (SourceDoesNotExistException e) {
      throw new SourceNotFoundException(e.getSourceName());
    }
  }

  public VersionedPlugin.EntityType getVersionedEntityType(VersionedDatasetId id) {
    List<String> fullPath = id.getTableKey();
    VersionContext versionContext = id.getVersionContext().asVersionContext();
    return getVersionedEntityType(fullPath, versionContext);
  }

  private Folder createCatalogItemForVersionedFolder(
      List<String> path, String id, boolean includeChildren, String refType, String refValue)
      throws NamespaceException {
    FolderConfig folderConfig =
        new FolderConfig()
            .setFullPathList(path)
            .setName(path.get(path.size() - 1))
            .setId(new EntityId(id));
    List<CatalogItem> children =
        includeChildren ? getChildrenForPath(new NamespaceKey(path), refType, refValue) : null;
    return toFolderAPI(folderConfig, children);
  }
}
