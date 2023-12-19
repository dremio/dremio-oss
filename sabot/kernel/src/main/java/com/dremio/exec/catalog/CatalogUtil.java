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
package com.dremio.exec.catalog;

import static com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET;
import static com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET_HOME_FILE;
import static com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET_HOME_FOLDER;
import static com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET_SOURCE_FILE;
import static com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER;
import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.projectnessie.model.ContentKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.catalog.exception.SourceDoesNotExistException;
import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.catalog.model.dataset.TableVersionType;
import com.dremio.common.exceptions.UserException;
import com.dremio.connector.metadata.DatasetMetadataVerifyResult;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetVerifyAppendOnlyResult;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.options.MetadataVerifyRequest;
import com.dremio.connector.metadata.options.TimeTravelOption;
import com.dremio.connector.metadata.options.VerifyAppendOnlyRequest;
import com.dremio.context.RequestContext;
import com.dremio.context.UserContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.UnAuthenticatedException;
import com.dremio.service.namespace.DatasetMetadataSaver;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.source.SourceNamespaceService;
import com.dremio.service.orphanage.Orphanage;
import com.dremio.service.orphanage.proto.OrphanEntry;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public final class CatalogUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(CatalogUtil.class);

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CatalogUtil.class);

  private CatalogUtil() {
  }

  /**
   * Save a partition chunk listing that refers to all partition chunks since the last invocation
   * of {@link #savePartitionChunksInSplitsStores(DatasetMetadataSaver, PartitionChunkListing)}, or since the creation of this metadata saver, whichever came last.
   * Also calculates to total number of records across every split and every partition chunk listed.
   *
   * @param chunkListing The partition chunks to save.
   * @return The total record count of all splits in chunkListing.
   */
  public static long savePartitionChunksInSplitsStores(DatasetMetadataSaver saver, PartitionChunkListing chunkListing) throws IOException {
    long recordCountFromSplits = 0;
    final Iterator<? extends PartitionChunk> chunks = chunkListing.iterator();
    while (chunks.hasNext()) {
      final PartitionChunk chunk = chunks.next();

      final Iterator<? extends DatasetSplit> splits = chunk.getSplits().iterator();
      while (splits.hasNext()) {
        final DatasetSplit split = splits.next();
        saver.saveDatasetSplit(split);
        recordCountFromSplits += split.getRecordCount();
      }
      saver.savePartitionChunk(chunk);
    }
    return recordCountFromSplits;
  }

  public static boolean requestedPluginSupportsVersionedTables(NamespaceKey key, Catalog catalog) {
    return requestedPluginSupportsVersionedTables(key.getRoot(), catalog);
  }

  public static boolean requestedPluginSupportsVersionedTables(String sourceName, Catalog catalog) {
    try {
      return catalog.getSource(sourceName) instanceof VersionedPlugin;
    } catch (UserException ignored) {
      // Source not found
      return false;
    }
  }

  public static String getDefaultBranch(String sourceName, Catalog catalog) {
    if (!requestedPluginSupportsVersionedTables(sourceName, catalog)) {
      return null;
    }
    try {
      VersionedPlugin versionedPlugin = catalog.getSource(sourceName);
      return (versionedPlugin == null ? null : versionedPlugin.getDefaultBranch());
    } catch (NoDefaultBranchException e1) {
      throw UserException.validationError(e1)
        .message("Unable to get default branch for Source %s", sourceName)
        .buildSilently();
    } catch (UnAuthenticatedException e2) {
      throw UserException.resourceError().message("Unable to get default branch.  Unable to authenticate to the Nessie server. " +
        "Make sure that the token is valid and not expired.").build();
    }
  }

  public static ResolvedVersionContext resolveVersionContext(Catalog catalog, String sourceName, VersionContext version) {
    if (!requestedPluginSupportsVersionedTables(sourceName, catalog)) {
      return null;
    }
    try {
      return catalog.resolveVersionContext(sourceName, version);
    } catch (ReferenceNotFoundException e) {
      throw UserException.validationError(e)
        .message("Requested %s not found on source %s.", version, sourceName)
        .buildSilently();
    } catch (NoDefaultBranchException e) {
      throw UserException.validationError(e)
        .message("Unable to resolve source version. Version was not specified and Source %s does not have a default branch set.", sourceName)
        .buildSilently();
    } catch (ReferenceConflictException e) {
      throw UserException.validationError(e)
        .message("Requested %s in source %s is not the requested type.", version, sourceName)
        .buildSilently();
    }
  }

  public static boolean hasIcebergMetadata(DatasetConfig datasetConfig) {
    if (datasetConfig.getPhysicalDataset() != null) {
      if (datasetConfig.getPhysicalDataset().getIcebergMetadataEnabled() != null &&
        datasetConfig.getPhysicalDataset().getIcebergMetadataEnabled() &&
        datasetConfig.getPhysicalDataset().getIcebergMetadata() != null) {
        return true;
      }
    }
    return false;
  }

  public static OrphanEntry.Orphan createIcebergMetadataOrphan(DatasetConfig datasetConfig) {
    String tableUuid = datasetConfig.getPhysicalDataset().getIcebergMetadata().getTableUuid();
    OrphanEntry.OrphanIcebergMetadata icebergOrphan = OrphanEntry.OrphanIcebergMetadata.newBuilder().setIcebergTableUuid(tableUuid).setDatasetTag(datasetConfig.getTag()).addAllDatasetFullPath(datasetConfig.getFullPathList()).build();
    long currTime = System.currentTimeMillis();
    return OrphanEntry.Orphan.newBuilder().setOrphanType(OrphanEntry.OrphanType.ICEBERG_METADATA).setCreatedAt(currTime).setScheduledAt(currTime).setOrphanDetails(icebergOrphan.toByteString()).build();
  }

  public static void addIcebergMetadataOrphan(DatasetConfig datasetConfig, Orphanage orphanage) {

    if (hasIcebergMetadata(datasetConfig)) {
      OrphanEntry.Orphan orphanEntry = createIcebergMetadataOrphan(datasetConfig);
      addIcebergMetadataOrphan(orphanEntry, orphanage);
    }

  }

  public static void addIcebergMetadataOrphan(OrphanEntry.Orphan orphanEntry, Orphanage orphanage) {
    orphanage.addOrphan(orphanEntry);
  }


  public static SourceNamespaceService.DeleteCallback getDeleteCallback(Orphanage orphanage) {
    SourceNamespaceService.DeleteCallback deleteCallback = (DatasetConfig datasetConfig) -> {
      addIcebergMetadataOrphan(datasetConfig, orphanage);
    };
    return deleteCallback;
  }

  public static void validateResolvedVersionIsBranch(ResolvedVersionContext resolvedVersionContext) {
    if ((resolvedVersionContext != null) && !resolvedVersionContext.isBranch()) {
      throw UserException.validationError()
        .message("DDL and DML operations are only supported for branches - not on tags or commits. %s is not a branch. ",
          resolvedVersionContext.getRefName())
        .buildSilently();
    }
  }

  public static boolean isFSInternalIcebergTableOrJsonTableOrMongo(Catalog catalog, NamespaceKey path, DatasetConfig dataset) {
    StoragePlugin storagePlugin;
    try {
      storagePlugin = catalog.getSource(path.getRoot());
    } catch (UserException uex) {
      throw UserException.validationError()
        .message("Source [%s] not found", path)
        .buildSilently();
    }

    if (!(storagePlugin instanceof MutablePlugin)) {
      return false;
    }

    return ((MutablePlugin) storagePlugin).isSupportUserDefinedSchema(dataset);

  }

  /**
   * Utility to return TimeTravelRequest for query : select * from iceberg_table AT SNAPSHOT/TIMESTAMP
   *
   * @param key
   * @param context
   * @return
   */
  public static TimeTravelOption.TimeTravelRequest getTimeTravelRequest(NamespaceKey key, TableVersionContext context) {
    switch (context.getType()) {
      case SNAPSHOT_ID:
        return TimeTravelOption.newSnapshotIdRequest(context.getValueAs(String.class));
      case TIMESTAMP:
        final long millis = context.getValueAs(Long.class);
        if (millis > System.currentTimeMillis()) {
          throw UserException.validationError()
            .message("For table '%s', the provided time travel timestamp value '%d' is out of range",
              key.getPathComponents(), millis)
            .buildSilently();
        }
        return TimeTravelOption.newTimestampRequest(millis);
      case BRANCH:
      case TAG:
      case COMMIT:
      case REFERENCE:
      case NOT_SPECIFIED:
        return null;
      default:
        throw new AssertionError("Unsupported type " + context.getType());
    }
  }

  /**
   * This Catalog will allow the caller to search for entries but will not promote entries that are missing in Namespace KV store
   * It will not check validity of metadata
   *
   * @param catalogService
   * @return
   */
  public static EntityExplorer getSystemCatalogForReflections(CatalogService catalogService) {
    return catalogService.getCatalog(MetadataRequestOptions.newBuilder()
      .setSchemaConfig(SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME)).build())
      .setCheckValidity(false)
      .setNeverPromote(true)
      .build());
  }

  public static Catalog getSystemCatalogForJobs(CatalogService catalogService) {
    return catalogService.getCatalog(MetadataRequestOptions.newBuilder()
      .setSchemaConfig(SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME)).build())
      .setCheckValidity(false)
      .setNeverPromote(true)
      .build());
  }

  public static Catalog getSystemCatalogForDatasetResource(CatalogService catalogService) {
    return catalogService.getCatalog(MetadataRequestOptions.newBuilder()
      .setSchemaConfig(SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME)).build())
      .setCheckValidity(false)
      .setNeverPromote(true)
      .build());
  }

  public static DremioTable getTableNoResolve(CatalogEntityKey catalogEntityKey, EntityExplorer catalog) {
    NamespaceKey key = catalogEntityKey.toNamespaceKey();
    if (catalogEntityKey.getTableVersionContext() != null) {
      try {
        return catalog.getTableSnapshotNoResolve(key, catalogEntityKey.getTableVersionContext());
      } catch (UserException e) {
        // getTableSnapshot returns a UserException when table is not found.
        return null;
      }
    } else {
      return catalog.getTableNoResolve(key);
    }
  }

  public static DatasetConfig getDatasetConfig(
      CatalogEntityKey catalogEntityKey, EntityExplorer catalog) {
    final DremioTable table = catalog.getTable(catalogEntityKey);

    return (table == null) ? null : table.getDatasetConfig();
  }

  public static DatasetConfig getDatasetConfig(EntityExplorer catalog, String datasetId) {
    DremioTable dremioTable = catalog.getTable(datasetId);
    DatasetConfig datasetConfig = null;
    if (dremioTable != null) {
      datasetConfig = dremioTable.getDatasetConfig();
    }
    return datasetConfig;
  }

  public static DatasetConfig getDatasetConfig(EntityExplorer catalog, NamespaceKey key) {
    DremioTable dremioTable = catalog.getTable(key);
    DatasetConfig datasetConfig = null;
    if (dremioTable != null) {
      datasetConfig = dremioTable.getDatasetConfig();
    }
    return datasetConfig;
  }

  /**
   * Check if the versioned table/view/folder exists in Nessie
   * Note that this check does not retrieve any metadata from S3 unless it is a TIMESTAMP or AT SNAPSHOT specification,
   * in which case we have to go to the Iceberg metadata json file to retrieve the snapshot/timestamp information.
   * @param catalog
   * @param id
   * @return
   */
  public static boolean versionedEntityExists(Catalog catalog, VersionedDatasetId id) {
    NamespaceKey tableNamespaceKey = new NamespaceKey(id.getTableKey());
    if (VersionedDatasetId.isTimeTravelDatasetId(id)) {
      return (catalog.getTable(tableNamespaceKey) != null);
    }
    String source = tableNamespaceKey.getRoot();
    VersionContext versionContext = id.getVersionContext().asVersionContext();
    ResolvedVersionContext resolvedVersionContext = CatalogUtil.resolveVersionContext(catalog, source, versionContext);
    VersionedPlugin versionedPlugin = catalog.getSource(source);
    List<String> versionedTableKey = tableNamespaceKey.getPathWithoutRoot();
    // Run as System User, and not as running User, to check that the table exists.
    String retrievedContentId;
    try {
      retrievedContentId = RequestContext.current()
        .with(UserContext.CTX_KEY, UserContext.SYSTEM_USER_CONTEXT)
        .call(() -> versionedPlugin.getContentId(versionedTableKey, resolvedVersionContext));
    } catch (Exception e) {
      LOGGER.warn("Failed to get content id for {}", id, e);
      // TODO: DX-68489: Investigate proper exception handling in CatalogUtil
      throw UserException.unsupportedError(e).buildSilently();
    }

    return (retrievedContentId != null &&
      (retrievedContentId.equals(id.getContentId())));

  }

  public static CatalogEntityKey getCatalogEntityKey(
      NamespaceKey namespaceKey, ResolvedVersionContext resolvedVersionContext, Catalog catalog) {
    if (!requestedPluginSupportsVersionedTables(namespaceKey, catalog)) {
      return CatalogEntityKey.fromNamespaceKey(namespaceKey);
    }
    String versionType = getRefType(resolvedVersionContext);
    String versionValue = getRefValue(resolvedVersionContext);

    final CatalogEntityKey.Builder builder =
        CatalogEntityKey.newBuilder().keyComponents(namespaceKey.getPathComponents());
    final Optional<TableVersionContext> tableVersionContext =
        TableVersionContext.tryParse(versionType, versionValue);

    if (tableVersionContext.isPresent()) {
      builder.tableVersionContext(tableVersionContext.get());
    } else if (!Strings.isNullOrEmpty(versionType) || !Strings.isNullOrEmpty(versionValue) || resolvedVersionContext == null) {
      throw UserException.validationError()
          .message("Missing a valid versionType/versionValue pair for versioned dataset")
          .buildSilently();
    }

    return builder.build();
  }

  static MetadataVerifyRequest getMetadataVerifyRequest(NamespaceKey key, TableMetadataVerifyRequest tableMetadataVerifyRequest) {
    checkArgument(key != null);
    checkArgument(tableMetadataVerifyRequest != null);

    if (tableMetadataVerifyRequest instanceof TableMetadataVerifyAppendOnlyRequest) {
      TableMetadataVerifyAppendOnlyRequest request = (TableMetadataVerifyAppendOnlyRequest) tableMetadataVerifyRequest;
      return new VerifyAppendOnlyRequest(request.getBeginSnapshotId(), request.getEndSnapshotId());
    }
    throw new UnsupportedOperationException(String.format("Unsupported TableMetadataVerifyRequest type %s for table '%s'",
      tableMetadataVerifyRequest.getClass().toString(), key.getPathComponents()));
  }

  static TableMetadataVerifyResult getMetadataVerifyResult(NamespaceKey key, DatasetMetadataVerifyResult datasetMetadataVerifyResult) {
    checkArgument(key != null);
    checkArgument(datasetMetadataVerifyResult != null);

    if (datasetMetadataVerifyResult instanceof DatasetVerifyAppendOnlyResult) {
      return new VerifyAppendOnlyResultAdapter((DatasetVerifyAppendOnlyResult) datasetMetadataVerifyResult);
    }
    throw new UnsupportedOperationException(String.format("Unsupported DatasetMetadataVerifyResult type %s for table '%s'",
      datasetMetadataVerifyResult.getClass().toString(), key.getPathComponents()));
  }

  /**
   * Returns the validated table path, if one exists.
   *
   * Note: Due to the way the tables get cached, we have to use Catalog.getTableNoResolve, rather than
   * using Catalog.getTable.
   */
  //TODO: DX-68443 Refactor this method.
  public static NamespaceKey getResolvePathForTableManagement(Catalog catalog, NamespaceKey path) {
    return getResolvePathForTableManagement(catalog, path, new TableVersionContext(TableVersionType.NOT_SPECIFIED, ""));
  }

  public static NamespaceKey getResolvePathForTableManagement(Catalog catalog, NamespaceKey path, TableVersionContext versionContextFromSql) {
    NamespaceKey resolvedPath = catalog.resolveToDefault(path);
    StoragePlugin maybeSource;
    DremioTable table;
    CatalogEntityKey.Builder keyBuilder = CatalogEntityKey.newBuilder();
    if (resolvedPath != null) {
      maybeSource = getAndValidateSourceForTableManagement(catalog, versionContextFromSql, resolvedPath);
      keyBuilder = keyBuilder.keyComponents(resolvedPath.getPathComponents());
      if (maybeSource != null && maybeSource instanceof VersionedPlugin) {
        keyBuilder = keyBuilder.tableVersionContext(versionContextFromSql);
      }
      table = getTableNoResolve(keyBuilder.build(), catalog);
      if (table != null) {
        return table.getPath();
      }
    }
    // Since the table was undiscovered with the resolved path, use `path` and try again.
    maybeSource = getAndValidateSourceForTableManagement(catalog, versionContextFromSql, path);
    keyBuilder = CatalogEntityKey.newBuilder()
      .keyComponents(path.getPathComponents());
    if (maybeSource != null && maybeSource instanceof VersionedPlugin) {
      keyBuilder = keyBuilder.tableVersionContext(versionContextFromSql);
    }
    table = getTableNoResolve(keyBuilder.build(), catalog);
    if (table != null) {
      return table.getPath();
    }

    throw UserException.validationError().message("Table [%s] does not exist.", path).buildSilently();
  }

  public static StoragePlugin getAndValidateSourceForTableManagement(Catalog catalog, TableVersionContext tableVersionContext, NamespaceKey path) {
    StoragePlugin source;
    try {
      source = catalog.getSource(path.getRoot());
    } catch (UserException ue) {
      // Cannot find source
      logger.debug(String.format("Cannot find source %s", path.getRoot()));
      return null;
    }

    if ((source != null) && !(source.isWrapperFor(VersionedPlugin.class)) && (tableVersionContext != null) && (tableVersionContext.getType() != TableVersionType.NOT_SPECIFIED)) {
      throw UserException.validationError()
        .message(String.format("Source [%s] does not support AT [%s] version specification for DML operations", path.getRoot(), tableVersionContext))
        .buildSilently();
    }
    return source;
  }

  /**
   * Resolved table for DML and returns catalog entity key
   * */
  public static CatalogEntityKey getResolvedCatalogEntityKey(Catalog catalog, NamespaceKey path, ResolvedVersionContext resolvedVersionContext) {
    NamespaceKey finalPath = getResolvePathForTableManagement(catalog, path);
    return CatalogUtil.getCatalogEntityKey(
      finalPath,
      resolvedVersionContext,
      catalog);
  }

  public static boolean permittedNessieKey(NamespaceKey key) {
    return key.getPathComponents().size() <= ContentKey.MAX_ELEMENTS;
  }

  public static VersionedPlugin.EntityType getVersionedEntityType(
      final Catalog catalog,
      final VersionedDatasetId id) throws SourceDoesNotExistException {
    final List<String> fullPath = id.getTableKey();
    final VersionContext versionContext = id.getVersionContext().asVersionContext();
    return getVersionedEntityType(catalog, fullPath, versionContext);
  }

  public static VersionedPlugin.EntityType getVersionedEntityType(
      final Catalog catalog,
      final List<String> fullPath,
      final VersionContext versionContext) throws SourceDoesNotExistException {
    final VersionedPlugin versionedPlugin = getVersionedPlugin(catalog, fullPath);
    final ResolvedVersionContext resolvedVersionContext = versionedPlugin.resolveVersionContext(versionContext);
    final List<String> contentKey = fullPath.subList(1, fullPath.size());
    final VersionedPlugin.EntityType entityType = versionedPlugin.getType(contentKey, resolvedVersionContext);
    if (entityType == null) {
      return VersionedPlugin.EntityType.UNKNOWN;
    }
    return entityType;
  }

  private static VersionedPlugin getVersionedPlugin(
      final Catalog catalog,
      final List<String> fullPath) throws SourceDoesNotExistException {
    final StoragePlugin plugin = getStoragePlugin(catalog, fullPath.get(0));
    Preconditions.checkArgument(plugin instanceof VersionedPlugin);
    return (VersionedPlugin) plugin;
  }

  private static StoragePlugin getStoragePlugin(
      final Catalog catalog,
      String sourceName) throws SourceDoesNotExistException {
    final StoragePlugin plugin = catalog.getSource(sourceName);

    if (plugin == null) {
      throw new SourceDoesNotExistException(sourceName);
    }

    return plugin;
  }

  public static MutablePlugin getMutablePlugin(NamespaceKey key, String error, Catalog catalog) throws SourceDoesNotExistException {
    StoragePlugin plugin = getStoragePlugin(catalog, key.getRoot());
    if (plugin instanceof MutablePlugin) {
      return (MutablePlugin) plugin;
    }
    throw UserException.unsupportedError().message(key.getRoot() + " " + error).build(logger);
  }

  private static String getRefType(ResolvedVersionContext resolvedVersionContext) {
    if (resolvedVersionContext != null) {
      return resolvedVersionContext.getType().toString();
    }
    return null;
  }
  private static String getRefValue(ResolvedVersionContext resolvedVersionContext) {
    if (resolvedVersionContext != null) {
      if (resolvedVersionContext.getType().equals(ResolvedVersionContext.Type.COMMIT)) {
        return resolvedVersionContext.getCommitHash();
      } else {
        return resolvedVersionContext.getRefName();
      }
    }
    return null;
  }

  public static boolean isDatasetTypeATable(DatasetType type) {
    return type == PHYSICAL_DATASET ||
      type == PHYSICAL_DATASET_SOURCE_FILE ||
      type == PHYSICAL_DATASET_SOURCE_FOLDER ||
      type == PHYSICAL_DATASET_HOME_FILE ||
      type == PHYSICAL_DATASET_HOME_FOLDER;
  }
}
