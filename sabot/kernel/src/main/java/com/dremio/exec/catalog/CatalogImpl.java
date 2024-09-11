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

import static com.dremio.exec.catalog.CatalogOptions.VERSIONED_SOURCE_UDF_ENABLED;
import static com.dremio.exec.catalog.CatalogServiceImpl.SYSTEM_TABLE_SOURCE_NAME;
import static com.dremio.exec.catalog.CatalogUtil.getIcebergTimeTravelRequest;
import static com.dremio.exec.catalog.CatalogUtil.getMetadataVerifyRequest;
import static com.dremio.exec.catalog.CatalogUtil.getMetadataVerifyResult;
import static com.dremio.exec.catalog.VersionedPlugin.EntityType;
import static com.dremio.exec.planner.physical.PlannerSettings.REFLECTION_LINEAGE_TABLE_FUNCTION_ENABLED;
import static com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType.VALIDATION;
import static com.dremio.exec.store.sys.udf.UserDefinedFunctionSerde.fromProto;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import com.dremio.catalog.exception.UnsupportedForgetTableException;
import com.dremio.catalog.model.CatalogEntityId;
import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.VersionedDatasetId;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.concurrent.bulk.BulkRequest;
import com.dremio.common.concurrent.bulk.BulkResponse;
import com.dremio.common.concurrent.bulk.ValueTransformer;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.AttributeValue;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.extensions.SupportsMetadataVerify;
import com.dremio.connector.metadata.options.InternalMetadataTableOption;
import com.dremio.datastore.ProtostuffSerializer;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.Serializer;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByCondition;
import com.dremio.datastore.api.ImmutableFindByCondition;
import com.dremio.exec.catalog.udf.DremioScalarUserDefinedFunction;
import com.dremio.exec.catalog.udf.DremioTabularUserDefinedFunction;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.physical.base.ViewOptions;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.sql.parser.SqlGrant;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.ColumnExtendedProperty;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.MFunctionCatalogMetadata;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.PartitionNotFoundException;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.VersionedDatasetAccessOptions;
import com.dremio.exec.store.dfs.FileDatasetHandle;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.dfs.MetadataIOPool;
import com.dremio.exec.store.iceberg.ViewHandle;
import com.dremio.exec.store.mfunctions.DeltaLakeMFunctionTranslatableTableImpl;
import com.dremio.exec.store.mfunctions.IcebergMetadataFunctionsTable;
import com.dremio.exec.store.mfunctions.TableFilesMFunctionTranslatableTableImpl;
import com.dremio.exec.store.sys.udf.UserDefinedFunction;
import com.dremio.exec.tablefunctions.IcebergMFunctionTranslatableTableImpl;
import com.dremio.exec.tablefunctions.MetadataFunctionsMacro;
import com.dremio.exec.tablefunctions.TableMacroNames;
import com.dremio.exec.tablefunctions.TimeTravelTableMacro;
import com.dremio.exec.tablefunctions.copyerrors.CopyErrorsMacro;
import com.dremio.options.OptionManager;
import com.dremio.plugins.sysflight.ReflectionLineageTableMacro;
import com.dremio.service.catalog.Schema;
import com.dremio.service.catalog.SearchQuery;
import com.dremio.service.catalog.Table;
import com.dremio.service.catalog.TableSchema;
import com.dremio.service.listing.DatasetListingService;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.DatasetIndexKeys;
import com.dremio.service.namespace.ImmutableEntityNamespaceFindOptions;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceIdentity;
import com.dremio.service.namespace.NamespaceIndexKeys;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEvents;
import com.dremio.service.namespace.catalogstatusevents.events.DatasetDeletionCatalogStatusEvent;
import com.dremio.service.namespace.catalogstatusevents.events.SourceDeletionCatalogStatusEvent;
import com.dremio.service.namespace.catalogstatusevents.events.SourceUpdateCatalogStatusEvent;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetField;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.namespace.function.proto.FunctionConfig;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.dremio.service.namespace.source.SourceNamespaceService;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.orphanage.Orphanage;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import io.protostuff.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.commons.collections4.CollectionUtils;

/** Default, non caching, implementation of {@link Catalog} */
@ThreadSafe
public class CatalogImpl implements Catalog {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(CatalogImpl.class);
  private static final int MAX_RETRIES = 5;

  private final MetadataRequestOptions options;
  private final PluginRetriever pluginRetriever;
  private final CatalogServiceImpl.SourceModifier sourceModifier;
  private final String userName;

  private final OptionManager optionManager;
  private final NamespaceService systemNamespaceService;
  private final NamespaceService.Factory namespaceFactory;
  private final Orphanage orphanage;
  private final DatasetListingService datasetListingService;
  private final ViewCreatorFactory viewCreatorFactory;
  private final IdentityResolver identityResolver;

  private final NamespaceService userNamespaceService;
  private final DatasetManager datasets;
  private final InformationSchemaCatalog iscDelegate;
  private final VersionContextResolverImpl versionContextResolverImpl;
  private final CatalogStatusEvents catalogStatusEvents;
  private final VersionedDatasetAdapterFactory versionedDatasetAdapterFactory;
  private final MetadataIOPool metadataIOPool;

  CatalogImpl(
      MetadataRequestOptions options,
      PluginRetriever pluginRetriever,
      CatalogServiceImpl.SourceModifier sourceModifier,
      OptionManager optionManager,
      NamespaceService systemNamespaceService,
      NamespaceService.Factory namespaceFactory,
      Orphanage orphanage,
      DatasetListingService datasetListingService,
      ViewCreatorFactory viewCreatorFactory,
      IdentityResolver identityResolver,
      VersionContextResolverImpl versionContextResolverImpl,
      CatalogStatusEvents catalogStatusEvents,
      VersionedDatasetAdapterFactory versionedDatasetAdapterFactory,
      MetadataIOPool metadataIOPool) {
    this.options = options;
    this.pluginRetriever = pluginRetriever;
    this.sourceModifier = sourceModifier;
    this.userName = options.getSchemaConfig().getUserName();

    this.optionManager = optionManager;
    this.systemNamespaceService = systemNamespaceService;
    this.namespaceFactory = namespaceFactory;
    this.orphanage = orphanage;
    this.datasetListingService = datasetListingService;
    this.viewCreatorFactory = viewCreatorFactory;
    this.identityResolver = identityResolver;
    this.versionedDatasetAdapterFactory = versionedDatasetAdapterFactory;

    final CatalogIdentity identity = options.getSchemaConfig().getAuthContext().getSubject();
    final NamespaceIdentity namespaceIdentity = identityResolver.toNamespaceIdentity(identity);
    this.userNamespaceService = namespaceFactory.get(namespaceIdentity);

    this.versionContextResolverImpl = versionContextResolverImpl;
    this.metadataIOPool = metadataIOPool;
    this.datasets =
        new DatasetManager(
            pluginRetriever,
            userNamespaceService,
            optionManager,
            userName,
            identityResolver,
            versionContextResolverImpl,
            versionedDatasetAdapterFactory,
            metadataIOPool);
    this.iscDelegate =
        new InformationSchemaCatalogImpl(
            userNamespaceService, pluginRetriever, optionManager, namespaceIdentity);
    this.catalogStatusEvents = catalogStatusEvents;
  }

  @Override
  public void addOrUpdateDataset(NamespaceKey datasetPath, DatasetConfig dataset)
      throws NamespaceException {
    userNamespaceService.addOrUpdateDataset(datasetPath, dataset);
  }

  @Override
  public DremioTable getTableNoResolve(NamespaceKey key) {
    return datasets.getTable(key, options, false);
  }

  @Override
  public DremioTable getTableNoColumnCount(NamespaceKey key) {
    return datasets.getTable(key, options, true);
  }

  @Override
  public DremioTable getTable(NamespaceKey key) {
    final NamespaceKey resolvedKey = resolveToDefault(key);

    if (resolvedKey != null) {
      final DremioTable table = getTableHelper(resolvedKey);
      if (table != null) {
        return table;
      }
    }

    return getTableHelper(key);
  }

  @Override
  public DremioTable getTable(CatalogEntityKey catalogEntityKey) {
    NamespaceKey namespaceKey = catalogEntityKey.toNamespaceKey();
    if (CatalogUtil.forATSpecifierAccess(catalogEntityKey, this)) {
      try {
        return getTableSnapshot(catalogEntityKey);
      } catch (UserException e) {
        // getTableSnapshot returns a UserException when table or Reference is not found.
        return null;
      }
    }
    return getTable(namespaceKey);
  }

  @Override
  public BulkResponse<NamespaceKey, Optional<DremioTable>> bulkGetTables(
      BulkRequest<NamespaceKey> keys) {

    // issue a bulk request for the keys as provided
    BulkResponse<NamespaceKey, Optional<DremioTable>> unresolvedKeyResponses =
        datasets.bulkGetTables(keys, options, false);

    // if there is a default schema provided, perform a concurrent lookup with keys resolved
    // to that default schema
    BulkResponse<NamespaceKey, Optional<DremioTable>> combinedResponses = unresolvedKeyResponses;
    if (getDefaultSchema() != null && getDefaultSchema().size() > 0) {

      // issue a bulk request for the keys resolved to the default schema
      BulkResponse<NamespaceKey, Optional<DremioTable>> resolvedKeyResponses =
          keys.bulkTransformAndHandleRequests(
              requests -> datasets.bulkGetTables(requests, options, false), this::resolveToDefault);

      // combine with unresolved key responses - resolved key responses will be preferred over
      // unresolved
      combinedResponses =
          resolvedKeyResponses.combineWith(
              unresolvedKeyResponses,
              (resolvedResponse, unresolvedResponse) ->
                  resolvedResponse.isPresent() ? resolvedResponse : unresolvedResponse);
    }

    // define a value transformer which will update any view tables after retrieval
    ValueTransformer<NamespaceKey, Optional<DremioTable>, NamespaceKey, Optional<DremioTable>>
        updateTableAfterRetrieval =
            (originalKey, resolvedKey, optTable) -> {
              optTable.ifPresent(table -> updateTableIfNeeded(resolvedKey, table));
              return optTable;
            };

    return combinedResponses.transform(
        java.util.function.Function.identity(), updateTableAfterRetrieval);
  }

  @Override
  public BulkResponse<NamespaceKey, Optional<DremioTable>> bulkGetTablesForQuery(
      BulkRequest<NamespaceKey> keys) {
    return bulkGetTables(keys);
  }

  @Override
  public String getDatasetId(NamespaceKey key) {
    final NamespaceKey resolvedKey = resolveToDefault(key);
    return getDatasetIdHelper((resolvedKey != null) ? resolvedKey : key);
  }

  private String getDatasetIdHelper(NamespaceKey key) {
    String datasetId = getDatasetIdForVersionedSource(key);

    if (datasetId != null) {
      return datasetId;
    }

    final DremioTable table = getTable(key);
    if (table == null
        || table.getDatasetConfig() == null
        || table.getDatasetConfig().getId() == null) {
      return null;
    }

    return table.getDatasetConfig().getId().getId();
  }

  private String getDatasetIdForVersionedSource(NamespaceKey key) {
    if (key == null) {
      return null;
    }

    final ManagedStoragePlugin plugin = pluginRetriever.getPlugin(key.getRoot(), false);
    if (plugin == null || !(plugin.getPlugin().isWrapperFor(VersionedPlugin.class))) {
      return null;
    }

    final String sourceName = plugin.getName().getRoot();
    final VersionContext versionContext = options.getVersionForSource(sourceName, key);
    final ResolvedVersionContext resolvedVersionContext =
        versionContextResolverImpl.resolveVersionContext(sourceName, versionContext);
    final List<String> tableKey = key.getPathWithoutRoot();
    final String contentId =
        plugin
            .getPlugin()
            .unwrap(VersionedPlugin.class)
            .getContentId(tableKey, resolvedVersionContext);

    if (contentId == null) {
      logger.debug(
          "Cannot find the content Id for table key: {} version: {}", key, resolvedVersionContext);
      return null;
    }

    final TableVersionContext tableVersionContext = TableVersionContext.of(resolvedVersionContext);
    final VersionedDatasetId versionedDatasetId =
        VersionedDatasetId.newBuilder()
            .setTableKey(key.getPathComponents())
            .setContentId(contentId)
            .setTableVersionContext(tableVersionContext)
            .build();

    return versionedDatasetId.asString();
  }

  @Override
  public DatasetType getDatasetType(CatalogEntityKey key) {
    if (key == null) {
      return DatasetType.OTHERS;
    }

    if (!key.hasTableVersionContext()) {
      final DremioTable table = getTable(key.toNamespaceKey());
      return (table == null || table.getDatasetConfig() == null)
          ? DatasetType.OTHERS
          : table.getDatasetConfig().getType();
    }

    final ManagedStoragePlugin plugin = pluginRetriever.getPlugin(key.getRootEntity(), false);
    if (plugin == null
        || plugin.getPlugin() == null
        || !(plugin.getPlugin().isWrapperFor(VersionedPlugin.class))) {
      return DatasetType.OTHERS;
    }

    final String sourceName = plugin.getName().getRoot();
    final VersionContext versionContext = key.getTableVersionContext().asVersionContext();
    final ResolvedVersionContext resolvedVersionContext =
        versionContextResolverImpl.resolveVersionContext(sourceName, versionContext);
    final List<String> tableKey = key.getPathWithoutRoot();

    final EntityType entityType =
        plugin.getPlugin().unwrap(VersionedPlugin.class).getType(tableKey, resolvedVersionContext);
    if (entityType == null) {
      logger.debug(
          "Cannot determine entityType for catalog key: {} version: {}",
          key,
          resolvedVersionContext);
      return DatasetType.INVALID_DATASET_TYPE;
    }

    // TODO (DX-65443): UI use version context to identify iceberg table/view. It's wrong if we
    // support other formats. We should return the real type instead.
    switch (entityType) {
      case ICEBERG_TABLE:
        return DatasetType.PHYSICAL_DATASET;
      case ICEBERG_VIEW:
        return DatasetType.VIRTUAL_DATASET;
      case FOLDER:
      case UNKNOWN:
      default:
        return DatasetType.INVALID_DATASET_TYPE;
    }
  }

  /**
   * This follows the similar definition of getTableSnapshot(). It can be generalized for timetravel
   * and metadata function query.
   *
   * @param key
   * @param context
   * @param functionName
   * @return
   */
  private TranslatableTable getMFunctionTable(
      NamespaceKey key, TableVersionContext context, String functionName) {
    final NamespaceKey resolvedKey = resolveToDefault(key);

    if (resolvedKey != null) {
      final TranslatableTable table = mFunctionTableForPlugin(resolvedKey, context, functionName);
      if (table != null) {
        return table;
      }
    }

    final TranslatableTable table = mFunctionTableForPlugin(key, context, functionName);
    if (table == null) {
      throw UserException.validationError().message("Table '%s' not found", key).buildSilently();
    }

    return table;
  }

  private TranslatableTable mFunctionTableForPlugin(
      NamespaceKey key, TableVersionContext context, String functionName) {
    final ManagedStoragePlugin plugin = pluginRetriever.getPlugin(key.getRoot(), false);
    if (plugin == null) {
      return null;
    }

    if (plugin.getPlugin().isWrapperFor(VersionedPlugin.class)) {
      return getMFunctionTableForVersionedSource(plugin, key, context, functionName);
    } else {
      return getMFunctionTableForNonVersionedSource(plugin, key, functionName, context);
    }
  }

  private TranslatableTable getMFunctionTableForVersionedSource(
      ManagedStoragePlugin plugin,
      NamespaceKey canonicalKey,
      TableVersionContext context,
      String functionName) {
    final DatasetConfig currentConfig =
        getDatasetConfigForVersionedSource(
            CatalogEntityKey.namespaceKeyToCatalogEntityKey(canonicalKey, context));
    if (currentConfig == null) {
      return null;
    }
    MetadataFunctionsMacro.MacroName mFunctionName =
        MetadataFunctionsMacro.MacroName.valueOf(functionName.toUpperCase(Locale.ROOT));
    final MFunctionVersionedSourceMetadata mFunctionMetadata =
        new MFunctionVersionedSourceMetadata(
            canonicalKey,
            plugin,
            currentConfig,
            options.getSchemaConfig(),
            getVersionedDatasetAccessOptions(canonicalKey, context),
            context);
    final DatasetRetrievalOptions retrievalOptions =
        plugin.getDefaultRetrievalOptions().toBuilder()
            .setTimeTravelRequest(getIcebergTimeTravelRequest(canonicalKey, context))
            .setVersionedDatasetAccessOptions(
                getVersionedDatasetAccessOptions(canonicalKey, context))
            .build();

    final Optional<DatasetHandle> handle;
    try {
      handle = plugin.getDatasetHandle(canonicalKey, null, retrievalOptions);
    } catch (ConnectorException e) {
      throw UserException.validationError(e).buildSilently();
    }
    if (!handle.isPresent()) {
      return null;
    }
    if (!(handle.get() instanceof FileDatasetHandle)) {
      throw UserException.validationError()
          .message(
              "Metadata function ('%s') is not supported on versioned table '%s'",
              functionName, canonicalKey)
          .buildSilently();
    }
    return mFunctionTableUtility(mFunctionName, canonicalKey, mFunctionMetadata);
  }

  private TranslatableTable getMFunctionTableForNonVersionedSource(
      ManagedStoragePlugin plugin,
      NamespaceKey key,
      String functionName,
      TableVersionContext context) {
    MetadataFunctionsMacro.MacroName mFunctionName =
        MetadataFunctionsMacro.MacroName.valueOf(functionName.toUpperCase(Locale.ROOT));
    final DatasetConfig currentConfig = getDatasetConfig(key);

    if (currentConfig == null) {
      return null;
    }

    if (!isMFunctionSupported(currentConfig, mFunctionName)) {
      throw UserException.validationError()
          .message(
              "Metadata function ('%s') is not supported on table '%s'",
              functionName, currentConfig.getFullPathList())
          .buildSilently();
    }
    final NamespaceKey canonicalKey = new NamespaceKey(currentConfig.getFullPathList());
    final MFunctionNonVersionedSourceMetadata mFunctionMetadata =
        new MFunctionNonVersionedSourceMetadata(
            canonicalKey, currentConfig, plugin, options.getSchemaConfig(), context);
    return mFunctionTableUtility(mFunctionName, canonicalKey, mFunctionMetadata);
  }

  private boolean isMFunctionSupported(
      DatasetConfig config, MetadataFunctionsMacro.MacroName mFunctionName) {
    if (!ManagedStoragePlugin.isComplete(config)) {
      return false;
    }

    if (DatasetHelper.isIcebergDataset(config)) {
      return true;
    }

    if (DatasetHelper.isDeltaLakeDataset(config)) {
      return mFunctionName == MetadataFunctionsMacro.MacroName.TABLE_HISTORY
          || mFunctionName == MetadataFunctionsMacro.MacroName.TABLE_SNAPSHOT;
    }

    return false;
  }

  private TranslatableTable mFunctionTableUtility(
      MetadataFunctionsMacro.MacroName mFunctionName,
      NamespaceKey canonicalKey,
      MFunctionMetadata mFunctionMetadata) {
    final MFunctionCatalogMetadata catalogMetadata =
        new MFunctionCatalogMetadata(
            IcebergMetadataFunctionsTable.valueOf(mFunctionName.getName().toUpperCase(Locale.ROOT))
                .getRecordSchema(),
            canonicalKey,
            mFunctionMetadata.getPluginId(),
            mFunctionName);

    if (DatasetHelper.isDeltaLakeDataset(mFunctionMetadata.getCurrentConfig())) {
      switch (mFunctionName) {
        case TABLE_HISTORY:
        case TABLE_SNAPSHOT:
          if (mFunctionMetadata.getOptions().getTimeTravelRequest() != null) {
            throw UserException.validationError()
                .message(
                    "Time Travel is not supported on metadata function '%s' for DeltaLake Table",
                    mFunctionName)
                .buildSilently();
          }
          return new DeltaLakeMFunctionTranslatableTableImpl(catalogMetadata, mFunctionMetadata);
        default:
          throw UserException.validationError()
              .message("Unsupported Function '%s' for DeltaLake Table", mFunctionName)
              .buildSilently();
      }
    }

    switch (mFunctionName) {
      case TABLE_HISTORY:
      case TABLE_MANIFESTS:
      case TABLE_SNAPSHOT:
      case TABLE_PARTITIONS:
        if (mFunctionMetadata.getOptions().getTimeTravelRequest() != null) {
          throw UserException.validationError()
              .message("Time Travel is not supported on metadata function: '%s' ", mFunctionName)
              .buildSilently();
        }
        // For TABLE_PARTITIONS, throw error when the table is not partitioned
        if (mFunctionName == MetadataFunctionsMacro.MacroName.TABLE_PARTITIONS) {
          if (mFunctionMetadata.getCurrentConfig().getReadDefinition() == null
              || mFunctionMetadata.getCurrentConfig().getReadDefinition().getPartitionColumnsList()
                  == null
              || mFunctionMetadata
                      .getCurrentConfig()
                      .getReadDefinition()
                      .getPartitionColumnsList()
                      .size()
                  < 1) {
            throw UserException.validationError()
                .message("Table %s is not partitioned.", canonicalKey.getSchemaPath())
                .buildSilently();
          }
        }
        return new IcebergMFunctionTranslatableTableImpl(
            catalogMetadata,
            mFunctionMetadata.getSchemaConfig().getUserName(),
            mFunctionMetadata.getMetadataLocation());
      case TABLE_FILES:
        final Supplier<Optional<DatasetHandle>> datasetHandleSupplier =
            mFunctionMetadata::getHandle;
        return new TableFilesMFunctionTranslatableTableImpl(
            catalogMetadata,
            mFunctionMetadata.getCurrentConfig(),
            datasetHandleSupplier,
            mFunctionMetadata.getPlugin(),
            mFunctionMetadata.getOptions(),
            mFunctionMetadata.getSchemaConfig().getUserName());
      default:
        throw UserException.validationError()
            .message("Invalid Table Function Name '%s' ", mFunctionName)
            .buildSilently();
    }
  }

  @Override
  public DremioTable getTableNoResolve(CatalogEntityKey catalogEntityKey) {
    NamespaceKey namespaceKey = catalogEntityKey.toNamespaceKey();
    if (CatalogUtil.forATSpecifierAccess(catalogEntityKey, this)) {
      try {
        return getTableSnapshotHelper(
            catalogEntityKey.toNamespaceKey(), catalogEntityKey.getTableVersionContext());
      } catch (UserException e) {
        // getTableSnapshot returns a UserException when table or Reference is not found.
        return null;
      }
    }
    return getTableNoResolve(namespaceKey);
  }

  @Override
  public DremioTable getTableSnapshotForQuery(CatalogEntityKey catalogEntityKey) {
    return getTableSnapshot(catalogEntityKey);
  }

  @Override
  public DremioTable getTableSnapshot(CatalogEntityKey catalogEntityKey) {

    if (!catalogEntityKey.hasTableVersionContext()) {
      return getTable(catalogEntityKey.toNamespaceKey());
    }
    final NamespaceKey resolvedKey = resolveToDefault(catalogEntityKey.toNamespaceKey());

    if (resolvedKey != null) {
      final DremioTable table =
          getTableSnapshotHelper(resolvedKey, catalogEntityKey.getTableVersionContext());
      if (table != null) {
        return table;
      }
    }

    final DremioTable table =
        getTableSnapshotHelper(
            catalogEntityKey.toNamespaceKey(), catalogEntityKey.getTableVersionContext());
    if (table == null) {
      throw UserException.validationError()
          .message("Table '%s' not found", catalogEntityKey.toNamespaceKey())
          .buildSilently();
    }

    return table;
  }

  private DremioTable getTableSnapshotHelper(NamespaceKey key, TableVersionContext context) {
    final ManagedStoragePlugin plugin = pluginRetriever.getPlugin(key.getRoot(), false);
    if (plugin == null || plugin.getPlugin() == null) {
      return null;
    }

    if (plugin.getPlugin().isWrapperFor(VersionedPlugin.class)) {
      return getTableSnapshotForVersionedSource(plugin, key, context);
    } else {
      return getTableSnapshotForNonVersionedSource(plugin, key, context);
    }
  }

  private DremioTable getTableSnapshotForVersionedSource(
      ManagedStoragePlugin plugin,
      NamespaceKey canonicalKey,
      TableVersionContext tableVersionContext) {
    DatasetRetrievalOptions retrievalOptions = null;
    try {
      retrievalOptions =
          plugin.getDefaultRetrievalOptions().toBuilder()
              .setTimeTravelRequest(getIcebergTimeTravelRequest(canonicalKey, tableVersionContext))
              .setVersionedDatasetAccessOptions(
                  getVersionedDatasetAccessOptions(canonicalKey, tableVersionContext))
              .build();
    } catch (VersionNotFoundInNessieException u) {
      //  This is thrown  during table resolution if we lookup a VersionContext specified with the
      // AT syntax
      // in the session context source. In that case the Version will not be found. We need to
      // return null to satisfy the
      // getTable contract.
      logger.debug("Unable to retrieve table metadata for {}", canonicalKey, u);
      return null;
    }

    final Optional<DatasetHandle> handle;
    try {
      handle = plugin.getDatasetHandle(canonicalKey, null, retrievalOptions);
    } catch (ConnectorException e) {
      throw UserException.validationError(e).buildSilently();
    }
    if (!handle.isPresent()) {
      return null;
    }
    if (handle.get() instanceof ViewHandle) {
      final String accessUserName = options.getSchemaConfig().getUserName();
      if (tableVersionContext.isTimeTravelType()) {
        throw UserException.validationError()
            .message(
                "Versioned views do not support AT SNAPSHOT or AT TIMESTAMP '%s'", canonicalKey)
            .buildSilently();
      }
      final VersionedDatasetAdapter versionedDatasetAdapter =
          versionedDatasetAdapterFactory.newInstance(
              canonicalKey.getPathComponents(),
              versionContextResolverImpl.resolveVersionContext(
                  plugin.getName().getRoot(), tableVersionContext.asVersionContext()),
              plugin.getPlugin(),
              plugin.getId(),
              optionManager);
      if (versionedDatasetAdapter == null) {
        return null;
      }
      DremioTable viewTable = versionedDatasetAdapter.getTable(accessUserName);
      Preconditions.checkState(viewTable instanceof ViewTable);
      return ((ViewTable) viewTable)
          .withVersionContext(getVersionContext(canonicalKey, tableVersionContext));
    }
    DatasetRetrievalOptions finalRetrievalOptions = retrievalOptions;
    return handle
        .map(
            datasetHandle ->
                new MaterializedDatasetTableProvider(
                        null,
                        datasetHandle,
                        plugin.getPlugin(),
                        plugin.getId(),
                        options.getSchemaConfig(),
                        finalRetrievalOptions,
                        optionManager)
                    .get())
        .orElse(null);
  }

  private MaterializedDatasetTable getTableSnapshotForNonVersionedSource(
      ManagedStoragePlugin plugin, NamespaceKey key, TableVersionContext tableVersionContext) {
    final DatasetConfig currentConfig = getDatasetConfig(key);

    if (currentConfig == null) {
      return null;
    }
    if (!tableVersionContext.isTimeTravelType()) {
      throw UserException.validationError()
          .message(
              "Source '%s' does not support AT BRANCH/TAG/COMMIT specification ",
              currentConfig.getFullPathList().get(0))
          .buildSilently();
    }
    if (!datasetSupportsTimeTravel(currentConfig)) {
      throw UserException.validationError()
          .message("Time travel is not supported on table '%s'", currentConfig.getFullPathList())
          .buildSilently();
    }

    final NamespaceKey canonicalKey = new NamespaceKey(currentConfig.getFullPathList());

    final DatasetRetrievalOptions.Builder retrievalOptionsBuilder =
        plugin.getDefaultRetrievalOptions().toBuilder()
            .setTimeTravelRequest(getIcebergTimeTravelRequest(canonicalKey, tableVersionContext));
    // For Unlimited Splits, if useInternalMetadataTable option is set, retrieve the TableMetadata
    // from table's iceberg metadata table, not from namespace KV.
    if (DatasetHelper.isInternalIcebergTable(currentConfig) && options.useInternalMetadataTable()) {
      String internalMetadataTableName =
          Optional.ofNullable(currentConfig.getPhysicalDataset())
              .map(PhysicalDataset::getIcebergMetadata)
              .map(IcebergMetadata::getTableUuid)
              .orElse("");
      if (internalMetadataTableName.isEmpty()) {
        throw UserException.invalidMetadataError()
            .message(
                "Error accessing table metadata created by Dremio, re-promote to refresh metadata.")
            .buildSilently();
      }
      retrievalOptionsBuilder.setInternalMetadataTableOption(
          new InternalMetadataTableOption(internalMetadataTableName));
    }
    final DatasetRetrievalOptions retrievalOptions = retrievalOptionsBuilder.build();

    final Optional<DatasetHandle> handle;
    try {
      handle = plugin.getDatasetHandle(canonicalKey, currentConfig, retrievalOptions);
    } catch (ConnectorException e) {
      throw UserException.validationError(e).buildSilently();
    }

    Preconditions.checkArgument(handle.isPresent());
    return new MaterializedDatasetTableProvider(
            currentConfig,
            handle.get(),
            plugin.getPlugin(),
            plugin.getId(),
            options.getSchemaConfig(),
            retrievalOptions,
            optionManager)
        .get();
  }

  @Nonnull
  @Override
  public Optional<TableMetadataVerifyResult> verifyTableMetadata(
      CatalogEntityKey key, TableMetadataVerifyRequest metadataVerifyRequest) {
    DatasetHandle datasetHandle = null;
    ManagedStoragePlugin managedStoragePlugin =
        pluginRetriever.getPlugin(key.getRootEntity(), false);
    if (managedStoragePlugin != null) {
      datasetHandle =
          getDatasetHandleHelper(
              managedStoragePlugin, key.toNamespaceKey(), key.getTableVersionContext());
    }

    if (datasetHandle == null) {
      throw UserException.validationError().message("Table '%s' not found", key).buildSilently();
    }

    StoragePlugin storagePlugin = managedStoragePlugin.getPlugin();
    if (storagePlugin instanceof SupportsMetadataVerify) {
      return ((SupportsMetadataVerify) storagePlugin)
          .verifyMetadata(
              datasetHandle, getMetadataVerifyRequest(key.toNamespaceKey(), metadataVerifyRequest))
          .map(r -> getMetadataVerifyResult(key.toNamespaceKey(), r));
    }

    return Optional.empty();
  }

  private DatasetHandle getDatasetHandleHelper(
      ManagedStoragePlugin managedStoragePlugin, NamespaceKey key, TableVersionContext context) {
    if (managedStoragePlugin.getPlugin().isWrapperFor(VersionedPlugin.class)) {
      return getDatasetHandleForVersionedSource(managedStoragePlugin, key, context);
    } else {
      return getDatasetHandleForNonVersionedSource(managedStoragePlugin, key);
    }
  }

  private DatasetHandle getDatasetHandleForVersionedSource(
      ManagedStoragePlugin managedStoragePlugin, NamespaceKey key, TableVersionContext context) {
    DatasetRetrievalOptions retrievalOptions = null;
    try {
      retrievalOptions =
          managedStoragePlugin.getDefaultRetrievalOptions().toBuilder()
              .setVersionedDatasetAccessOptions(getVersionedDatasetAccessOptions(key, context))
              .build();
    } catch (ReferenceNotFoundException r) {
      logger.debug("Could not resolve VersionContext for {}", key, r);
      return null;
    }

    final Optional<DatasetHandle> handle;
    try {
      handle = managedStoragePlugin.getDatasetHandle(key, null, retrievalOptions);
    } catch (ConnectorException e) {
      throw UserException.validationError(e).buildSilently();
    }

    return handle.orElse(null);
  }

  private DatasetHandle getDatasetHandleForNonVersionedSource(
      ManagedStoragePlugin managedStoragePlugin, NamespaceKey key) {
    final DatasetConfig currentConfig = getDatasetConfig(key);
    if (currentConfig == null) {
      return null;
    }

    final NamespaceKey canonicalKey = new NamespaceKey(currentConfig.getFullPathList());

    final DatasetRetrievalOptions.Builder retrievalOptionsBuilder =
        managedStoragePlugin.getDefaultRetrievalOptions().toBuilder();
    // For Unlimited Splits, if useInternalMetadataTable option is set, get the dataset handle to
    // table's iceberg metadata table.
    if (DatasetHelper.isInternalIcebergTable(currentConfig) && options.useInternalMetadataTable()) {
      String internalMetadataTableName =
          Optional.ofNullable(currentConfig.getPhysicalDataset())
              .map(PhysicalDataset::getIcebergMetadata)
              .map(IcebergMetadata::getTableUuid)
              .orElse("");
      if (internalMetadataTableName.isEmpty()) {
        throw UserException.invalidMetadataError()
            .message(
                "Error accessing table metadata created by Dremio, re-promote to refresh metadata.")
            .buildSilently();
      }
      retrievalOptionsBuilder.setInternalMetadataTableOption(
          new InternalMetadataTableOption(internalMetadataTableName));
    }
    final DatasetRetrievalOptions retrievalOptions = retrievalOptionsBuilder.build();

    final Optional<DatasetHandle> handle;
    try {
      handle = managedStoragePlugin.getDatasetHandle(canonicalKey, currentConfig, retrievalOptions);
    } catch (ConnectorException e) {
      throw UserException.validationError(e).buildSilently();
    }

    Preconditions.checkState(handle.isPresent());
    return handle.get();
  }

  private Optional<VersionedDatasetAccessOptions> getVersionedPluginAccessOption(
      ManagedStoragePlugin managedStoragePlugin, CatalogEntityKey key) {
    if (managedStoragePlugin.getPlugin().isWrapperFor(VersionedPlugin.class)) {
      return Optional.ofNullable(
          getVersionedDatasetAccessOptions(key.toNamespaceKey(), key.getTableVersionContext()));
    }
    return Optional.empty();
  }

  private boolean datasetSupportsTimeTravel(DatasetConfig config) {
    if (!ManagedStoragePlugin.isComplete(config)) {
      return false;
    }

    if (DatasetHelper.isIcebergDataset(config)
        || (DatasetHelper.isInternalIcebergTable(config) && options.useInternalMetadataTable())) {
      return true;
    }

    if (DatasetHelper.isDeltaLakeDataset(config)) {
      return true;
    }

    return false;
  }

  private DatasetConfig getDatasetConfig(NamespaceKey key) {
    DatasetConfig currentConfig = null;
    if (key != null) {
      try {
        currentConfig = userNamespaceService.getDataset(key);
      } catch (NamespaceException ignored) {
      }
      if (currentConfig == null) {
        currentConfig = getConfigByCanonicalKey(key);
      }
    }
    return currentConfig;
  }

  private DatasetConfig getDatasetConfigForVersionedSource(CatalogEntityKey catalogEntityKey) {
    DremioTable dremioTable = getTable(catalogEntityKey);
    if (dremioTable != null) {
      return dremioTable.getDatasetConfig();
    } else {
      return null;
    }
  }

  public VersionedDatasetAccessOptions getVersionedDatasetAccessOptions(
      NamespaceKey key, TableVersionContext context) {
    return new VersionedDatasetAccessOptions.Builder()
        .setVersionContext(resolveVersionContext(key.getRoot(), getVersionContext(key, context)))
        .build();
  }

  private VersionContext getVersionContext(NamespaceKey key, TableVersionContext context) {
    VersionContext versionContext;
    if (context == null) {
      context = TableVersionContext.NOT_SPECIFIED;
    }
    if (context.isTimeTravelType()) {
      // TableVersionContext is SNAPSHOT OR TIMESTAMP specified with AT syntax
      // Get the session version setting for this source from MetadataRequestOptions, if set.
      // Eg  for this use case :
      // use branch dev;
      // select * from T AT SNAPSHOT '242536368'
      // select * from T AT TIMESTAMP '1234566768'
      // In both cases we need to set the VersionContext to branch dev.

      versionContext = options.getSourceVersionMapping().get(key.getRoot());
      if (versionContext == null) {
        versionContext = VersionContext.NOT_SPECIFIED;
      }
    } else {
      // This must be of type BRANCH,TAG or COMMIT
      versionContext = context.asVersionContext();
      if (!versionContext.isSpecified()
          && (CatalogUtil.requestedPluginSupportsVersionedTables(key.getRoot(), this))) {
        // Fall back to the session context setting for the source
        versionContext = options.getVersionForSource(key.getRoot(), key);
      }
    }
    return versionContext;
  }

  private DremioTable getTableHelper(NamespaceKey key) {
    Span.current().setAttribute("dremio.namespace.key.schemapath", key.getSchemaPath());
    final DremioTable table = datasets.getTable(key, options, false);
    if (table != null) {
      return updateTableIfNeeded(key, table);
    }
    return null;
  }

  private DremioTable updateTableIfNeeded(NamespaceKey key, DremioTable table) {
    if (table instanceof ViewTable) {
      View view = ((ViewTable) table).getView();
      NamespaceKey viewPath = table.getPath();
      try {
        if (view.isFieldUpdated()) {
          // TODO: DX-44984
          updateView(viewPath, view, null);
          // Since view got updated, we need the latest version.
          return datasets.getTable(key, options, false);
        }
      } catch (Exception ex) {
        logger.warn("Failed to update view with updated nested schema: ", ex);
      }
    }
    return table;
  }

  @Override
  public DremioTable getTableForQuery(NamespaceKey key) {
    return getTable(key);
  }

  @Override
  public Map<String, List<ColumnExtendedProperty>> getColumnExtendedProperties(DremioTable table) {
    if (table.getDatasetConfig().getReadDefinition() == null) {
      return null;
    }

    try {
      final ManagedStoragePlugin plugin =
          pluginRetriever.getPlugin(table.getPath().getRoot(), true);
      return plugin
          .getPlugin()
          .parseColumnExtendedProperties(
              table.getDatasetConfig().getReadDefinition().getExtendedProperty());
    } catch (ConnectorException | UserException e) {
      logger.warn("Unable to get extended properties for {}", table.getPath(), e);
      return null;
    }
  }

  @Override
  public DremioTable getTable(String datasetId) {
    VersionedDatasetId versionedDatasetId = VersionedDatasetId.tryParse(datasetId);
    final boolean isTimeTravelDataset =
        versionedDatasetId != null && versionedDatasetId.getVersionContext().isTimeTravelType();
    Span.current().setAttribute("dremio.catalog.getTable.isTimeTravelDataset", isTimeTravelDataset);
    if (isTimeTravelDataset) {
      return getTableForTimeTravel(versionedDatasetId);
    }
    return datasets.getTable(datasetId, options);
  }

  @Override
  public Iterable<DremioTable> getAllRequestedTables() {
    throw new UnsupportedOperationException(
        "getAllRequestedTables() not supported on the default implementation");
  }

  @Override
  public NamespaceKey resolveSingle(NamespaceKey key) {
    if (getDefaultSchema() == null || key.size() > 1) {
      return key;
    }
    return getDefaultSchema().getChild(key.getLeaf());
  }

  @Override
  public boolean containerExists(CatalogEntityKey path) {
    return containerExists(path, userNamespaceService);
  }

  private boolean containerExists(CatalogEntityKey path, NamespaceService namespaceService) {
    final SchemaType type = getRootType(path.toNamespaceKey());
    if (type == SchemaType.UNKNOWN) {
      return false;
    }
    List<NameSpaceContainer> containers =
        namespaceService.getEntities(ImmutableList.of(path.toNamespaceKey()));
    NameSpaceContainer c = containers.get(0);
    if (c != null
        && ((c.getType() == NameSpaceContainer.Type.FOLDER
                && type
                    != SchemaType
                        .SOURCE) // DX-10186. Bad behavior in tryCreatePhysicalDataset causes
            // problems with extra phantom folders.
            || c.getType() == NameSpaceContainer.Type.HOME
            || c.getType() == NameSpaceContainer.Type.SPACE
            || c.getType() == NameSpaceContainer.Type.SOURCE)) {
      return true;
    }
    if (type != SchemaType.SOURCE) {
      return false;
    }

    // For some sources, some folders aren't automatically existing in namespace, let's be more
    // invasive...

    // let's check for a dataset in this path. We're looking for a dataset who either has this
    // path as the schema of it or has a schema that starts with this path.

    SearchTypes.SearchQuery searchQuery =
        SearchQueryUtils.and(
            SearchQueryUtils.newTermQuery(
                NamespaceIndexKeys.ENTITY_TYPE.getIndexFieldName(), Type.DATASET.getNumber()),
            SearchQueryUtils.or(
                SearchQueryUtils.newTermQuery(
                    DatasetIndexKeys.UNQUOTED_LC_SCHEMA, path.asLowerCase().toUnescapedString()),
                SearchQueryUtils.newPrefixQuery(
                    DatasetIndexKeys.UNQUOTED_LC_SCHEMA.getIndexFieldName(),
                    path.asLowerCase().toUnescapedString() + ".")));

    FindByCondition findByCondition =
        new ImmutableFindByCondition.Builder()
            .setCondition(searchQuery)
            // fetch only one matching record
            .setLimit(1)
            .build();
    if (!Iterables.isEmpty(
        userNamespaceService.find(
            findByCondition,
            new ImmutableEntityNamespaceFindOptions.Builder().setDisableKeySort(true).build()))) {
      return true;
    }

    // could be a filesystem, let's check the source directly (most expensive).
    ManagedStoragePlugin plugin = pluginRetriever.getPlugin(path.getRootEntity(), false);
    if (plugin == null) {
      // possible race condition where this plugin is no longer still registered.
      return false;
    }

    List<GetMetadataOption> containerMetadataOptions = new ArrayList<>();
    containerMetadataOptions.add(getVersionedPluginAccessOption(plugin, path).orElse(null));
    return plugin
        .unwrap(StoragePlugin.class)
        .containerExists(
            new EntityPath(path.getKeyComponents()),
            containerMetadataOptions.toArray(
                new GetMetadataOption[containerMetadataOptions.size()]));
  }

  @Override
  public Iterable<String> listSchemas(NamespaceKey path) {
    final SearchQuery searchQuery =
        path.size() == 0
            ? null
            : SearchQuery.newBuilder()
                .setEquals(
                    SearchQuery.Equals.newBuilder()
                        .setField(DatasetIndexKeys.UNQUOTED_LC_SCHEMA.getIndexFieldName())
                        .setStringValue(path.toUnescapedString().toLowerCase())
                        .build())
                .build();
    final Iterable<Schema> iterable = () -> listSchemata(searchQuery);

    return StreamSupport.stream(iterable.spliterator(), false).map(Schema::getSchemaName)::iterator;
  }

  @Override
  public Iterable<Table> listDatasets(NamespaceKey path) {
    final SearchQuery searchQuery =
        SearchQuery.newBuilder()
            .setAnd(
                SearchQuery.And.newBuilder()
                    .addClauses(
                        SearchQuery.newBuilder()
                            .setEquals(
                                SearchQuery.Equals.newBuilder()
                                    .setField(
                                        DatasetIndexKeys.UNQUOTED_LC_SCHEMA.getIndexFieldName())
                                    .setStringValue(path.toUnescapedString().toLowerCase())))
                    .addClauses(
                        SearchQuery.newBuilder()
                            .setEquals(
                                SearchQuery.Equals.newBuilder()
                                    .setField(NamespaceIndexKeys.ENTITY_TYPE.getIndexFieldName())
                                    .setIntValue(NameSpaceContainer.Type.DATASET.getNumber()))))
            .build();

    return () -> listTables(searchQuery);
  }

  @Override
  public Collection<Function> getFunctions(CatalogEntityKey path, FunctionType functionType) {
    final NamespaceKey resolvedPath = resolveSingle(path.toNamespaceKey());
    // Resolve version context for the source
    final VersionContext versionContext =
        getVersionContext(resolvedPath, path.getTableVersionContext());
    CatalogEntityKey functionKey =
        CatalogEntityKey.newBuilder()
            .keyComponents(resolvedPath.getPathComponents())
            .tableVersionContext(TableVersionContext.of(versionContext))
            .build();
    switch (functionType) {
      case TABLE:
        return getUserDefinedTableFunctions(functionKey);
      case SCALAR:
        return getUserDefinedScalarFunctions(functionKey);
      default:
        return ImmutableList.of();
    }
  }

  private Optional<UserDefinedFunction> getUserDefinedFunction(CatalogEntityKey path) {
    Optional<UserDefinedFunction> optionalUserDefinedFunction =
        getUserDefinedFunctionImplementation(path);
    if (optionalUserDefinedFunction.isPresent()) {
      return optionalUserDefinedFunction;
    }

    if (path.size() == 1) {
      return optionalUserDefinedFunction;
    }

    // Try again but from the root context
    return getUserDefinedFunctionImplementation(
        CatalogEntityKey.fromNamespaceKey(new NamespaceKey(path.getLeaf())));
  }

  private Optional<UserDefinedFunction> getUserDefinedFunctionImplementation(
      CatalogEntityKey path) {
    FunctionConfig functionConfig;
    ManagedStoragePlugin plugin = pluginRetriever.getPlugin(path.getRootEntity(), false);
    if (optionManager.getOption(VERSIONED_SOURCE_UDF_ENABLED)
        && plugin != null
        && plugin.getPlugin() != null
        && plugin.getPlugin().isWrapperFor(VersionedPlugin.class)) {
      return getUserDefinedFunctionImplementationFromNessie(path);
    }

    try {
      if (!userNamespaceService.exists(path.toNamespaceKey(), Type.FUNCTION)) {
        return Optional.empty();
      }

      functionConfig = userNamespaceService.getFunction(path.toNamespaceKey());
      if (functionConfig == null) {
        return Optional.empty();
      }

      UserDefinedFunction userDefinedFunction = fromProto(functionConfig);
      return Optional.of(userDefinedFunction);
    } catch (NamespaceException e) {
      // TODO what to do on ambiguous results
      throw new RuntimeException(e);
    }
  }

  Optional<UserDefinedFunction> getUserDefinedFunctionImplementationFromNessie(
      CatalogEntityKey path) {
    final ManagedStoragePlugin plugin = pluginRetriever.getPlugin(path.getRootEntity(), false);
    final VersionedPlugin versionedPlugin = plugin.getPlugin().unwrap(VersionedPlugin.class);
    final Optional<FunctionConfig> functionConfig = versionedPlugin.getFunction(path);
    if (!functionConfig.isPresent()) {
      return Optional.empty();
    }
    return Optional.of(fromProto(functionConfig.get()));
  }

  private CatalogIdentity getUserDefinedFunctionOwner(CatalogEntityKey path) {
    ManagedStoragePlugin plugin = pluginRetriever.getPlugin(path.getRootEntity(), false);
    // TODO : DX-91837 : Need to get the actual owner of the versioned udf
    if (optionManager.getOption(VERSIONED_SOURCE_UDF_ENABLED)
        && plugin != null
        && plugin.getPlugin() != null
        && plugin.getPlugin().isWrapperFor(VersionedPlugin.class)) {
      return new CatalogUser(SYSTEM_USERNAME);
    }
    try {
      CatalogIdentity owner = identityResolver.getOwner(path.getKeyComponents());
      if (owner == null) {
        owner = new CatalogUser(userName);
      }
      return owner;
    } catch (NamespaceException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  private Collection<Function> getUserDefinedScalarFunctions(CatalogEntityKey path) {
    Optional<UserDefinedFunction> optionalUserDefinedFunction = getUserDefinedFunction(path);
    if (!optionalUserDefinedFunction.isPresent()) {
      return ImmutableList.of();
    }

    UserDefinedFunction userDefinedFunction = optionalUserDefinedFunction.get();
    // DX-50618:
    // We need to add a type marker for tabular UDF, since in the future a scalar udf can return a
    // struct
    // (Or maybe they are the same at that point?)
    if (userDefinedFunction.getReturnType().isStruct()) {
      return ImmutableList.of();
    }
    final List<String> resolvedUdfPath = userDefinedFunction.getFullPath();
    CatalogEntityKey resolvedUdfKey =
        CatalogEntityKey.newBuilder()
            .keyComponents(resolvedUdfPath)
            .tableVersionContext(path.getTableVersionContext())
            .build();
    Function function =
        new DremioScalarUserDefinedFunction(
            getUserDefinedFunctionOwner(resolvedUdfKey), userDefinedFunction);
    return ImmutableList.of(function);
  }

  private Collection<Function> getUserDefinedTableFunctions(CatalogEntityKey path) {
    if (isMetadataFunctions(path.getLeaf())) {

      Function function =
          new MetadataFunctionsMacro(
              (tablePath, versionContext) ->
                  getMFunctionTable(new NamespaceKey(tablePath), versionContext, path.getLeaf()));

      return ImmutableList.of(function);
    } else if (isCopyErrorsFunction(path.toNamespaceKey())) {
      return ImmutableList.of(new CopyErrorsMacro(this));
    } else if (isReflectionLineageFunction(path.toNamespaceKey())) {
      ManagedStoragePlugin storagePlugin =
          pluginRetriever.getPlugin(SYSTEM_TABLE_SOURCE_NAME, false);
      return ImmutableList.of(new ReflectionLineageTableMacro(storagePlugin, userName));
    }

    if (TableMacroNames.TIME_TRAVEL.equals(path.getKeyComponents())) {
      Function function =
          new TimeTravelTableMacro(
              (tablePath, versionContext) ->
                  getTableSnapshotForQuery(
                      CatalogEntityKey.newBuilder()
                          .keyComponents(tablePath)
                          .tableVersionContext(versionContext)
                          .build()));
      return ImmutableList.of(function);
    }

    Optional<UserDefinedFunction> optionalUserDefinedFunction = getUserDefinedFunction(path);
    if (optionalUserDefinedFunction.isPresent()) {
      UserDefinedFunction userDefinedFunction = optionalUserDefinedFunction.get();
      if (!userDefinedFunction.getReturnType().getType().isComplex()) {
        return ImmutableList.of();
      }

      Function function =
          new DremioTabularUserDefinedFunction(
              getUserDefinedFunctionOwner(path), userDefinedFunction);

      return ImmutableList.of(function);
    }

    if (containerExists(
        CatalogEntityKey.fromNamespaceKey(path.getParent().toNamespaceKey()),
        systemNamespaceService)) {
      return getFunctionsInternal(path);
    }

    return ImmutableList.of();
  }

  private static boolean isCopyErrorsFunction(NamespaceKey path) {
    return CopyErrorsMacro.MACRO_NAME.equalsIgnoreCase(path.getLeaf());
  }

  private boolean isReflectionLineageFunction(NamespaceKey path) {
    return optionManager.getOption(REFLECTION_LINEAGE_TABLE_FUNCTION_ENABLED)
        && ReflectionLineageTableMacro.isReflectionLineageFunction(path);
  }

  /**
   * Validate If table function is of type metadata functions
   *
   * @param functionName
   * @return
   */
  private boolean isMetadataFunctions(String functionName) {
    return Arrays.stream(MetadataFunctionsMacro.MacroName.values())
        .anyMatch(e -> e.name().equalsIgnoreCase(functionName));
  }

  private Collection<Function> getFunctionsInternal(CatalogEntityKey path) {

    switch (getRootType(path.toNamespaceKey())) {
      case SOURCE:
        return sourceModifier
            .getSource(path.getRootEntity())
            .getFunctions(path.getKeyComponents(), options.getSchemaConfig());

      case HOME:
        try {
          return getHomeFilesPlugin()
              .getFunctions(path.getKeyComponents(), options.getSchemaConfig());
        } catch (ExecutionSetupException e) {
          throw new RuntimeException(e);
        }
      case SPACE:
      default:
        return Collections.emptyList();
    }
  }

  @Override
  public NamespaceKey getDefaultSchema() {
    return options.getSchemaConfig().getDefaultSchema();
  }

  @Override
  public NamespaceKey resolveToDefault(NamespaceKey key) {
    if (options.getSchemaConfig().getDefaultSchema() == null) {
      return null;
    }
    return new NamespaceKey(
        ImmutableList.<String>builder()
            .addAll(options.getSchemaConfig().getDefaultSchema().getPathComponents())
            .addAll(key.getPathComponents())
            .build());
  }

  @Override
  public Catalog resolveCatalog(Map<String, VersionContext> sourceVersionMapping) {
    return new CatalogImpl(
        options.cloneWith(sourceVersionMapping),
        pluginRetriever,
        sourceModifier,
        optionManager,
        systemNamespaceService,
        namespaceFactory,
        orphanage,
        datasetListingService,
        viewCreatorFactory,
        identityResolver,
        versionContextResolverImpl,
        catalogStatusEvents,
        versionedDatasetAdapterFactory,
        metadataIOPool);
  }

  @Override
  public Catalog resolveCatalogResetContext(String sourceName, VersionContext versionContext) {
    // Invalidate an existing entry if any
    versionContextResolverImpl.invalidateVersionContext(sourceName, versionContext);
    return new CatalogImpl(
        options,
        pluginRetriever,
        sourceModifier,
        optionManager,
        systemNamespaceService,
        namespaceFactory,
        orphanage,
        datasetListingService,
        viewCreatorFactory,
        identityResolver,
        versionContextResolverImpl,
        catalogStatusEvents,
        versionedDatasetAdapterFactory,
        metadataIOPool);
  }

  @Override
  public Catalog resolveCatalog(CatalogIdentity subject) {
    return new CatalogImpl(
        options.cloneWith(
            subject, options.getSchemaConfig().getDefaultSchema(), options.checkValidity()),
        pluginRetriever,
        sourceModifier.cloneWith(subject),
        optionManager,
        systemNamespaceService,
        namespaceFactory,
        orphanage,
        datasetListingService,
        viewCreatorFactory,
        identityResolver,
        versionContextResolverImpl,
        catalogStatusEvents,
        versionedDatasetAdapterFactory,
        metadataIOPool);
  }

  @Override
  public Catalog resolveCatalog(NamespaceKey newDefaultSchema) {
    return new CatalogImpl(
        options.cloneWith(
            options.getSchemaConfig().getAuthContext().getSubject(),
            newDefaultSchema,
            options.checkValidity()),
        pluginRetriever,
        sourceModifier.cloneWith(options.getSchemaConfig().getAuthContext().getSubject()),
        optionManager,
        systemNamespaceService,
        namespaceFactory,
        orphanage,
        datasetListingService,
        viewCreatorFactory,
        identityResolver,
        versionContextResolverImpl,
        catalogStatusEvents,
        versionedDatasetAdapterFactory,
        metadataIOPool);
  }

  private FileSystemPlugin getHomeFilesPlugin() throws ExecutionSetupException {
    return pluginRetriever.getPlugin("__home", true).unwrap(FileSystemPlugin.class);
  }

  @Override
  public void createEmptyTable(
      NamespaceKey key, BatchSchema batchSchema, final WriterOptions writerOptions) {
    asMutable(key, "does not support create table operations.")
        .createEmptyTable(key, options.getSchemaConfig(), batchSchema, writerOptions);
  }

  @Override
  public CreateTableEntry createNewTable(
      final NamespaceKey key,
      final IcebergTableProps icebergTableProps,
      final WriterOptions writerOptions,
      final Map<String, Object> storageOptions) {
    return asMutable(key, "does not support create table operations.")
        .createNewTable(
            key,
            options.getSchemaConfig(),
            icebergTableProps,
            writerOptions,
            storageOptions,
            false);
  }

  @Override
  public CreateTableEntry createNewTable(
      NamespaceKey key,
      IcebergTableProps icebergTableProps,
      WriterOptions writerOptions,
      Map<String, Object> storageOptions,
      boolean isResultsTable) {
    return asMutable(key, "does not support create table operations.")
        .createNewTable(
            key,
            options.getSchemaConfig(),
            icebergTableProps,
            writerOptions,
            storageOptions,
            isResultsTable);
  }

  @Override
  public void createView(
      final NamespaceKey key, View view, ViewOptions viewOptions, NamespaceAttribute... attributes)
      throws IOException {
    switch (getRootType(key)) {
      case SOURCE:
        asMutable(key, "does not support create view")
            .createOrUpdateView(key, options.getSchemaConfig(), view, viewOptions);
        break;
      case SPACE:
      case HOME:
        if (view.hasDeclaredFieldNames()) {
          throw UserException.unsupportedError()
              .message("Dremio doesn't support field aliases defined in view creation.")
              .buildSilently();
        }
        viewCreatorFactory
            .get(userName)
            .createView(
                key.getPathComponents(),
                view.getSql(),
                view.getWorkspaceSchemaPath(),
                false,
                attributes);
        break;
      default:
        throw UserException.unsupportedError()
            .message(
                "Cannot create view in %s.",
                key.getPathComponents().isEmpty() ? key : key.getRoot())
            .build(logger);
    }
  }

  @Override
  public void updateView(
      NamespaceKey key, View view, ViewOptions viewOptions, NamespaceAttribute... attributes)
      throws IOException {
    switch (getRootType(key)) {
      case SOURCE:
        asMutable(key, "does not support update view")
            .createOrUpdateView(key, options.getSchemaConfig(), view, viewOptions);
        break;
      case SPACE:
      case HOME:
        viewCreatorFactory
            .get(userName)
            .updateView(
                key.getPathComponents(), view.getSql(), view.getWorkspaceSchemaPath(), attributes);
        break;
      default:
        throw UserException.unsupportedError()
            .message(
                "Cannot update view in %s.",
                key.getPathComponents().isEmpty() ? key : key.getRoot())
            .build(logger);
    }
  }

  @Override
  public void dropView(final NamespaceKey key, ViewOptions viewOptions) throws IOException {
    switch (getRootType(key)) {
      case SOURCE:
        asMutable(key, "does not support view operations.")
            .dropView(key, viewOptions, options.getSchemaConfig());
        catalogStatusEvents.publish(new DatasetDeletionCatalogStatusEvent(key.toString()));
        return;
      case SPACE:
      case HOME:
        viewCreatorFactory.get(userName).dropView(key.getPathComponents());
        catalogStatusEvents.publish(new DatasetDeletionCatalogStatusEvent(key.toString()));
        return;
      default:
        throw UserException.unsupportedError()
            .message("Invalid request to drop " + key)
            .build(logger);
    }
  }

  /**
   * Determines which SchemaType the root of this path corresponds to.
   *
   * @param key The key whose root we will evaluate.
   * @return The SchemaType associated with this key.
   */
  private SchemaType getRootType(NamespaceKey key) {
    if (key.getPathComponents().isEmpty()) {
      return SchemaType.UNKNOWN;
    } else if (("@" + userName).equalsIgnoreCase(key.getRoot())) {
      return SchemaType.HOME;
    }

    NameSpaceContainer container =
        systemNamespaceService
            .getEntities(ImmutableList.of(new NamespaceKey(key.getRoot())))
            .get(0);
    if (container == null) {
      return SchemaType.UNKNOWN;
    }

    Type type = container.getType();
    switch (type) {
      case SOURCE:
        return SchemaType.SOURCE;
      case SPACE:
        return SchemaType.SPACE;

      case HOME:
        throw UserException.permissionError()
            .message("No permission to requested path.")
            .build(logger);

      default:
        throw new IllegalStateException("Unexpected container type: " + type.name());
    }
  }

  private MutablePlugin asMutable(NamespaceKey key, String error) {
    StoragePlugin plugin = sourceModifier.getSource(key.getRoot());
    if (plugin instanceof MutablePlugin) {
      return (MutablePlugin) plugin;
    }

    throw UserException.unsupportedError().message(key.getRoot() + " " + error).build(logger);
  }

  @Override
  public void dropTable(NamespaceKey key, TableMutationOptions tableMutationOptions) {
    final boolean existsInNamespace = systemNamespaceService.exists(key);
    boolean isLayered = false;
    DatasetConfig dataset = null;
    // CTAS does not create a namespace entry (DX-13454), but we want to allow dropping it, so
    // handle the cases
    // where it does not exist in the namespace but does exist at the plugin level.
    if (existsInNamespace) {
      try {
        dataset = systemNamespaceService.getDataset(key);
      } catch (NamespaceException ex) {
        throw UserException.validationError(ex).message("Table [%s] not found.", key).build(logger);
      }

      if (dataset == null) {
        throw UserException.validationError().message("Table [%s] not found.", key).build(logger);
      } else if (!isDroppable(dataset)) {
        throw UserException.validationError().message("[%s] is not a TABLE", key).build(logger);
      }

      // Get the key from the datasetConfig to avoid case sensitivity issues
      key = new NamespaceKey(dataset.getFullPathList());
      if (userNamespaceService.hasChildren(key)) {
        throw UserException.validationError()
            .message("Cannot drop table [%s] since it has child tables ", key)
            .buildSilently();
      }

      isLayered = DatasetHelper.isIcebergDataset(dataset);
    }

    MutablePlugin mutablePlugin;
    // If we can't find the source, we can't find the table.
    try {
      mutablePlugin = asMutable(key, "does not support dropping tables");
    } catch (UserException e) {
      if (e.getErrorType() == VALIDATION) {
        throw UserException.validationError().message("Table [%s] not found.", key).build(logger);
      }
      throw e;
    }

    TableMutationOptions localTableMutationOptions =
        tableMutationOptions != null
            ? ImmutableTableMutationOptions.copyOf(tableMutationOptions)
                .withIsLayered(isLayered)
                .withShouldDeleteCatalogEntry(isLayered)
            : null;

    mutablePlugin.dropTable(key, options.getSchemaConfig(), localTableMutationOptions);

    if (existsInNamespace) {
      try {
        if (CatalogUtil.hasIcebergMetadata(dataset)) {
          CatalogUtil.addIcebergMetadataOrphan(dataset, orphanage);
        }
        systemNamespaceService.deleteEntity(key);
      } catch (NamespaceException e) {
        throw Throwables.propagate(e);
      }
    }
    catalogStatusEvents.publish(new DatasetDeletionCatalogStatusEvent(key.toString()));
  }

  private boolean isDroppable(DatasetConfig datasetConfig) {
    return !isSystemTable(datasetConfig) && datasetConfig.getType() != DatasetType.VIRTUAL_DATASET;
  }

  private boolean isForgettable(DatasetConfig datasetConfig) {
    return isDroppable(datasetConfig);
  }

  private boolean isSystemTable(DatasetConfig config) {
    // check if system tables and information schema.
    final String root = config.getFullPathList().get(0);
    return ("sys").equals(root) || ("INFORMATION_SCHEMA").equals(root);
  }

  private DatasetConfig getConfigFromNamespace(NamespaceKey key) {
    try {
      return userNamespaceService.getDataset(key);
    } catch (NamespaceNotFoundException ex) {
      return null;
    } catch (NamespaceException ex) {
      throw new RuntimeException(ex);
    }
  }

  private DatasetConfig getConfigByCanonicalKey(NamespaceKey key) {

    final ManagedStoragePlugin plugin = pluginRetriever.getPlugin(key.getRoot(), false);

    if (plugin == null || plugin.getPlugin().isWrapperFor(VersionedPlugin.class)) {
      // short circuit for versioned plugin: entries are not saved in namespace
      return null;
    }

    final Optional<DatasetHandle> handle;
    try {
      handle = plugin.getDatasetHandle(key, null, plugin.getDefaultRetrievalOptions());
    } catch (ConnectorException e) {
      throw UserException.validationError(e)
          .message("Failure while retrieving dataset [%s].", key)
          .build(logger);
    }

    if (handle.isPresent()) {
      final NamespaceKey canonicalKey =
          MetadataObjectsUtils.toNamespaceKey(handle.get().getDatasetPath());
      if (!canonicalKey.equals(key)) {
        // before we do anything with this accessor, we should reprobe namespace as it is possible
        // that the request the
        // user made was not the canonical key and therefore we missed when trying to retrieve data
        // from the namespace.
        return getConfigFromNamespace(canonicalKey);
      }
    } // handle is present

    return null;
  }

  @Override
  public void alterTable(
      NamespaceKey key,
      DatasetConfig datasetConfig,
      AlterTableOption alterTableOption,
      TableMutationOptions tableMutationOptions) {
    MutablePlugin mutablePlugin = asMutable(key, "does not support dropping tables");
    mutablePlugin.alterTable(
        key, datasetConfig, alterTableOption, options.getSchemaConfig(), tableMutationOptions);
  }

  @Override
  public void forgetTable(NamespaceKey key) throws UnsupportedForgetTableException {

    if (CatalogUtil.requestedPluginSupportsVersionedTables(key, this)) {
      throw new UnsupportedForgetTableException(
          String.format(
              "Forget table is not a valid operation for objects in '%s' which is a versioned source.",
              key.getRoot()));
    }

    String root = key.getRoot();
    if (root.startsWith("@")
        || "sys".equalsIgnoreCase(root)
        || "INFORMATION_SCHEMA".equalsIgnoreCase(root)) {
      throw new UnsupportedForgetTableException(
          "FORGET METADATA is not supported on tables in homespace, sys, or INFORMATION_SCHEMA.");
    }
    int count = 0;

    while (true) {
      DatasetConfig dataset;
      try {
        dataset = systemNamespaceService.getDataset(key);
      } catch (NamespaceNotFoundException ex) {
        dataset = null;
      } catch (NamespaceException ex) {
        throw new RuntimeException(ex);
      }

      if (dataset == null) {
        // try to check if canonical key finds
        dataset = getConfigByCanonicalKey(key);
      } // passed-in key not found in kv

      if (dataset == null || !isForgettable(dataset)) {
        throw UserException.validationError()
            .message("Dataset %s does not exist or is not a table.", key)
            .build(logger);
      }
      try {
        if (CatalogUtil.hasIcebergMetadata(dataset)) {
          CatalogUtil.addIcebergMetadataOrphan(dataset, orphanage);
        }
        userNamespaceService.deleteDataset(
            new NamespaceKey(dataset.getFullPathList()), dataset.getTag());
        break;
      } catch (NamespaceNotFoundException ex) {
        logger.debug("Table to delete not found", ex);
      } catch (ConcurrentModificationException ex) {
        if (count++ < MAX_RETRIES) {
          logger.debug("Concurrent failure.", ex);
        } else {
          throw ex;
        }
      } catch (NamespaceException ex) {
        throw new RuntimeException(ex);
      }
    } // while loop
  }

  @Override
  public void truncateTable(NamespaceKey key, TableMutationOptions tableMutationOptions) {
    final boolean existsInNamespace = systemNamespaceService.exists(key);
    if (existsInNamespace) {
      final DatasetConfig dataset;
      try {
        dataset = systemNamespaceService.getDataset(key);
      } catch (NamespaceException ex) {
        throw UserException.validationError(ex).message("Table [%s] not found.", key).build(logger);
      }

      if (dataset == null) {
        throw UserException.validationError().message("Table [%s] not found.", key).build(logger);
      }

      // Get the key from the datasetConfig to avoid case sensitivity issues
      key = new NamespaceKey(dataset.getFullPathList());
    }
    asMutable(key, "does not support truncating tables")
        .truncateTable(key, options.getSchemaConfig(), tableMutationOptions);
  }

  @Override
  public void rollbackTable(
      NamespaceKey key,
      DatasetConfig datasetConfig,
      RollbackOption rollbackOption,
      TableMutationOptions tableMutationOptions) {
    MutablePlugin mutablePlugin = asMutable(key, "does not support rollback table");
    mutablePlugin.rollbackTable(
        key, datasetConfig, options.getSchemaConfig(), rollbackOption, tableMutationOptions);
  }

  @Override
  public void addColumns(
      NamespaceKey key,
      DatasetConfig datasetConfig,
      List<Field> colsToAdd,
      TableMutationOptions tableMutationOptions) {
    MutablePlugin mutablePlugin = asMutable(key, "does not support dropping tables");
    mutablePlugin.addColumns(
        key, datasetConfig, options.getSchemaConfig(), colsToAdd, tableMutationOptions);
  }

  @Override
  public void dropColumn(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      String columnToDrop,
      TableMutationOptions tableMutationOptions) {
    MutablePlugin mutablePlugin = asMutable(table, "does not support dropping tables");
    mutablePlugin.dropColumn(
        table, datasetConfig, options.getSchemaConfig(), columnToDrop, tableMutationOptions);
  }

  @Override
  public void changeColumn(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      String columnToChange,
      Field fieldFromSql,
      TableMutationOptions tableMutationOptions) {
    MutablePlugin mutablePlugin = asMutable(table, "does not support dropping tables");
    mutablePlugin.changeColumn(
        table,
        datasetConfig,
        options.getSchemaConfig(),
        columnToChange,
        fieldFromSql,
        tableMutationOptions);
  }

  /**
   * Sets table properties and refreshes dataset if properties changed
   *
   * @param catalogEntityKey
   * @param attributes
   * @return if dataset config is updated
   */
  @Override
  public boolean alterDataset(
      final CatalogEntityKey catalogEntityKey, final Map<String, AttributeValue> attributes) {
    DatasetConfig datasetConfig;
    NamespaceKey key = catalogEntityKey.toNamespaceKey();
    try {
      datasetConfig = systemNamespaceService.getDataset(key);
      if (datasetConfig.getType() == DatasetType.VIRTUAL_DATASET) {
        boolean changed = updateOptions(datasetConfig.getVirtualDataset(), attributes);
        if (changed) {
          // user userNamespaceService so that only those with "CAN EDIT" permission can make the
          // change
          userNamespaceService.addOrUpdateDataset(key, datasetConfig);
        }
        return changed;
      }
    } catch (ConcurrentModificationException ex) {
      throw UserException.validationError(ex)
          .message("Failure while accessing dataset")
          .buildSilently();
    } catch (NamespaceException handleLater) {
      datasetConfig = null;
    }

    final ManagedStoragePlugin plugin = pluginRetriever.getPlugin(key.getRoot(), true);
    if (plugin == null) {
      throw UserException.validationError()
          .message("Unknown source [%s]", key.getRoot())
          .buildSilently();
    }

    if (datasetConfig == null) {
      if (plugin.getPlugin() != null && plugin.getPlugin().isWrapperFor(VersionedPlugin.class)) {
        final ResolvedVersionContext resolvedVersionContext =
            versionContextResolverImpl.resolveVersionContext(
                key.getRoot(), catalogEntityKey.getTableVersionContext().asVersionContext());
        final DremioTable dremioTable = getTableNoResolve(catalogEntityKey);
        final DatasetConfig config = dremioTable.getDatasetConfig();

        if (config.getType() != DatasetType.VIRTUAL_DATASET) {
          throw UserException.validationError()
              .message("Can only save properties to virtual dataset")
              .buildSilently();
        }
        final Map<String, String> properties =
            attributes.entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> String.valueOf(entry.getValue().getValueAsObject())));
        final ViewOptions viewOptions =
            new ViewOptions.ViewOptionsBuilder()
                .version(resolvedVersionContext)
                .actionType(ViewOptions.ActionType.ALTER_VIEW_PROPERTIES)
                .icebergViewVersion(optionManager)
                .properties(properties)
                .build();

        CatalogUtil.validateResolvedVersionIsBranch(resolvedVersionContext);

        try {
          return asMutable(key, "does not support update view")
              .createOrUpdateView(key, options.getSchemaConfig(), null, viewOptions);
        } catch (IOException e) {
          throw UserException.validationError(e)
              .message("Failure while accessing view")
              .buildSilently();
        }
      }

      try {
        // try resolving names with "default" namespace; for example, if the key is
        // hivestore.datatab then try to resolve it using hivestore."default".datatab
        final Optional<DatasetHandle> handle =
            plugin.getDatasetHandle(key, null, plugin.getDefaultRetrievalOptions());
        // TODO: handle.get() is called without an isPresent() check.
        final NamespaceKey namespaceKey =
            MetadataObjectsUtils.toNamespaceKey(handle.get().getDatasetPath());
        datasetConfig = systemNamespaceService.getDataset(namespaceKey);
      } catch (ConnectorException e) {
        throw UserException.validationError(e)
            .message("Failure while retrieving dataset")
            .buildSilently();
      } catch (NamespaceException e) {
        throw UserException.validationError(e)
            .message("Unable to find requested dataset")
            .buildSilently();
      }
    }

    return plugin.alterDataset(key, datasetConfig, attributes);
  }

  /**
   * Sets column properties and refreshes dataset if properties changed
   *
   * @param key
   * @param columnToChange
   * @param attributeName
   * @param attributeValue
   * @return if dataset config is updated
   */
  @Override
  public boolean alterColumnOption(
      final NamespaceKey key,
      String columnToChange,
      final String attributeName,
      final AttributeValue attributeValue) {
    final DatasetConfig datasetConfig;
    try {
      datasetConfig = systemNamespaceService.getDataset(key);
      if (datasetConfig.getType() == DatasetType.VIRTUAL_DATASET) {
        throw UserException.validationError()
            .message("Cannot alter column options on a virtual dataset")
            .buildSilently();
      }
    } catch (NamespaceException | ConcurrentModificationException ex) {
      throw UserException.validationError(ex)
          .message("Failure while accessing dataset")
          .buildSilently();
    }
    final ManagedStoragePlugin plugin = pluginRetriever.getPlugin(key.getRoot(), true);
    if (plugin == null) {
      throw UserException.validationError()
          .message("Unknown source [%s]", key.getRoot())
          .buildSilently();
    }
    return plugin.alterDatasetSetColumnOption(
        key, datasetConfig, columnToChange, attributeName, attributeValue);
  }

  @Override
  public void addPrimaryKey(
      NamespaceKey table, List<String> columns, VersionContext statementVersion) {
    final MutablePlugin mutablePlugin = asMutable(table, "does not support adding primary keys");
    final VersionContext sessionVersion =
        options.getVersionForSource(mutablePlugin.getId().getName(), table);
    VersionContext sourceVersion = null;
    sourceVersion = statementVersion.orElse(sessionVersion);

    ResolvedVersionContext resolvedVersionContext = null;
    final CatalogEntityKey catalogEntityKey =
        CatalogEntityKey.newBuilder()
            .keyComponents(table.getPathComponents())
            .tableVersionContext(TableVersionContext.of(sourceVersion))
            .build();
    if (mutablePlugin.isWrapperFor(VersionedPlugin.class)) {
      ManagedStoragePlugin managedStoragePlugin = pluginRetriever.getPlugin(table.getRoot(), false);
      resolvedVersionContext =
          versionContextResolverImpl.resolveVersionContext(
              managedStoragePlugin.getName().getRoot(), sourceVersion);
    }
    final DremioTable dremioTable = getTable(catalogEntityKey);
    final DatasetConfig datasetConfig;
    try {
      datasetConfig = dremioTable.getDatasetConfig();
      if (datasetConfig.getType() == DatasetType.VIRTUAL_DATASET) {
        throw UserException.validationError()
            .message("Cannot add primary key to virtual dataset")
            .buildSilently();
      }
    } catch (ConcurrentModificationException ex) {
      throw UserException.validationError(ex)
          .message("Failure while accessing dataset")
          .buildSilently();
    }

    Map<String, Field> fieldNames =
        dremioTable.getSchema().getFields().stream()
            .collect(Collectors.toMap(f -> f.getName().toLowerCase(Locale.ROOT), f -> f));
    List<Field> columnFields = new ArrayList<>();
    columns.forEach(
        c -> {
          Field field = fieldNames.get(c.toLowerCase(Locale.ROOT));
          if (field == null) {
            throw UserException.validationError()
                .message(String.format("Column %s not found", c))
                .buildSilently();
          }
          columnFields.add(field);
        });
    mutablePlugin.addPrimaryKey(
        table, datasetConfig, options.getSchemaConfig(), columnFields, resolvedVersionContext);
  }

  @Override
  public void dropPrimaryKey(NamespaceKey table, VersionContext statementVersion) {
    final MutablePlugin mutablePlugin = asMutable(table, "does not support dropping primary keys");
    final VersionContext sessionVersion =
        options.getVersionForSource(mutablePlugin.getId().getName(), table);

    ResolvedVersionContext resolvedVersionContext = null;
    VersionContext sourceVersion = null;
    sourceVersion = statementVersion.orElse(sessionVersion);

    final CatalogEntityKey catalogEntityKey =
        CatalogEntityKey.newBuilder()
            .keyComponents(table.getPathComponents())
            .tableVersionContext(TableVersionContext.of(sourceVersion))
            .build();
    if (mutablePlugin.isWrapperFor(VersionedPlugin.class)) {
      ManagedStoragePlugin managedStoragePlugin = pluginRetriever.getPlugin(table.getRoot(), false);
      resolvedVersionContext =
          versionContextResolverImpl.resolveVersionContext(
              managedStoragePlugin.getName().getRoot(), sourceVersion);
    }
    final DremioTable dremioTable = getTable(catalogEntityKey);
    final DatasetConfig datasetConfig;
    try {
      datasetConfig = dremioTable.getDatasetConfig();
      if (datasetConfig.getType() == DatasetType.VIRTUAL_DATASET) {
        throw UserException.validationError()
            .message("Cannot drop primary key from virtual dataset")
            .buildSilently();
      }
    } catch (ConcurrentModificationException ex) {
      throw UserException.validationError(ex)
          .message("Failure while accessing dataset")
          .buildSilently();
    }

    List<String> primaryKey;
    try {
      primaryKey =
          mutablePlugin.getPrimaryKey(
              table, datasetConfig, options.getSchemaConfig(), resolvedVersionContext, true);
    } catch (Exception ex) {
      logger.debug("Failed to get primary key", ex);
      primaryKey = null;
    }
    if (CollectionUtils.isEmpty(primaryKey)) {
      throw UserException.validationError().message("No primary key to drop").buildSilently();
    }

    mutablePlugin.dropPrimaryKey(
        table, datasetConfig, options.getSchemaConfig(), resolvedVersionContext);
  }

  @Override
  public List<String> getPrimaryKey(NamespaceKey table) {
    final MutablePlugin mutablePlugin = asMutable(table, "does not support primary keys");
    final DremioTable dremioTable = getTable(table);
    final DatasetConfig datasetConfig;
    try {
      datasetConfig = dremioTable.getDatasetConfig();
      if (datasetConfig.getType() == DatasetType.VIRTUAL_DATASET) {
        throw UserException.validationError()
            .message("Virtual dataset cannot have primary keys")
            .buildSilently();
      }
    } catch (ConcurrentModificationException ex) {
      throw UserException.validationError(ex)
          .message("Failure while accessing dataset")
          .buildSilently();
    }

    ResolvedVersionContext versionContext = null;
    if (mutablePlugin.isWrapperFor(VersionedPlugin.class)) {
      ManagedStoragePlugin managedStoragePlugin = pluginRetriever.getPlugin(table.getRoot(), false);
      versionContext =
          versionContextResolverImpl.resolveVersionContext(
              managedStoragePlugin.getName().getRoot(),
              options.getVersionForSource(managedStoragePlugin.getName().getRoot(), table));
    }

    List<String> primaryKey;
    try {
      primaryKey =
          mutablePlugin.getPrimaryKey(
              table, datasetConfig, options.getSchemaConfig(), versionContext, true);
    } catch (Exception ex) {
      logger.debug("Failed to get primary key", ex);
      primaryKey = null;
    }
    return primaryKey;
  }

  @Override
  public boolean toggleSchemaLearning(NamespaceKey table, boolean enableSchemaLearning) {
    return asMutable(table, "does not support schema update")
        .toggleSchemaLearning(table, options.getSchemaConfig(), enableSchemaLearning);
  }

  @Override
  public void alterSortOrder(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      BatchSchema schema,
      List<String> sortOrderColumns,
      TableMutationOptions tableMutationOptions) {
    MutablePlugin mutablePlugin = asMutable(table, "does not support iceberg sort order updates");
    mutablePlugin.alterSortOrder(
        table,
        datasetConfig,
        schema,
        options.getSchemaConfig(),
        sortOrderColumns,
        tableMutationOptions);
  }

  @Override
  public void updateTableProperties(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      BatchSchema schema,
      Map<String, String> tableProperties,
      TableMutationOptions tableMutationOptions,
      boolean isRemove) {
    MutablePlugin mutablePlugin =
        asMutable(table, "does not support iceberg table properties updates");
    mutablePlugin.updateTableProperties(
        table,
        datasetConfig,
        schema,
        options.getSchemaConfig(),
        tableProperties,
        tableMutationOptions,
        isRemove);
  }

  private boolean updateOptions(
      VirtualDataset virtualDataset, Map<String, AttributeValue> attributes) {
    boolean changed = false;
    for (Entry<String, AttributeValue> attribute : attributes.entrySet()) {
      if (attribute.getKey().toLowerCase().equals("enable_default_reflection")) {
        AttributeValue.BooleanValue value = (AttributeValue.BooleanValue) attribute.getValue();
        boolean oldValue =
            Optional.ofNullable(virtualDataset.getDefaultReflectionEnabled()).orElse(true);
        if (value.getValue() != oldValue) {
          changed = true;
          virtualDataset.setDefaultReflectionEnabled(value.getValue());
        }
      } else {
        throw UserException.validationError()
            .message("Unknown option [%s]", attribute.getKey())
            .buildSilently();
      }
    }
    return changed;
  }

  @Override
  public void createDataset(
      NamespaceKey key,
      com.google.common.base.Function<DatasetConfig, DatasetConfig> datasetMutator) {
    final ManagedStoragePlugin plugin = pluginRetriever.getPlugin(key.getRoot(), true);
    if (plugin == null) {
      throw UserException.validationError()
          .message("Unknown source %s", key.getRoot())
          .buildSilently();
    }

    datasets.createDataset(key, plugin, datasetMutator);
  }

  @Override
  public UpdateStatus refreshDataset(NamespaceKey key, DatasetRetrievalOptions retrievalOptions) {
    ManagedStoragePlugin plugin;
    try {
      plugin = pluginRetriever.getPlugin(key.getRoot(), true);
      if (plugin.getPlugin().isWrapperFor(VersionedPlugin.class)) {
        final boolean requestedPluginSupportsVersionedTables =
            CatalogUtil.requestedPluginSupportsVersionedTables(key, this);
        Span.current()
            .setAttribute(
                "dremio.catalog.refreshDataset.requestedPluginSupportsVersionedTables",
                requestedPluginSupportsVersionedTables);
        return UpdateStatus.UNCHANGED;
      }
    } catch (UserException ue) {
      DatasetConfig datasetConfig = getConfigFromNamespace(key);
      if (datasetConfig != null && datasetConfig.getType() == DatasetType.VIRTUAL_DATASET) {
        throw UserException.validationError()
            .message("Only tables can be refreshed. Dataset %s is a view.", key)
            .buildSilently();
      }
      throw ue;
    }

    return plugin.refreshDataset(key, retrievalOptions);
  }

  @Override
  public UpdateStatus refreshDataset(
      NamespaceKey key,
      DatasetRetrievalOptions retrievalOptions,
      boolean isPrivilegeValidationNeeded) {
    return refreshDataset(key, retrievalOptions);
  }

  @Override
  public SourceState refreshSourceStatus(NamespaceKey key) throws Exception {
    final ManagedStoragePlugin plugin = pluginRetriever.getPlugin(key.getRoot(), true);
    if (plugin == null) {
      throw UserException.validationError()
          .message("Unknown source %s", key.getRoot())
          .buildSilently();
    }
    return plugin.refreshState().get();
  }

  @Override
  public Iterable<String> getSubPartitions(
      NamespaceKey key, List<String> partitionColumns, List<String> partitionValues)
      throws PartitionNotFoundException {

    if (pluginRetriever.getPlugin(key.getRoot(), true) == null) {
      throw UserException.validationError()
          .message("Unknown source %s", key.getRoot())
          .buildSilently();
    }

    final StoragePlugin plugin = sourceModifier.getSource(key.getRoot());
    FileSystemPlugin fsPlugin;
    if (plugin instanceof FileSystemPlugin) {
      fsPlugin = (FileSystemPlugin) plugin;
      return fsPlugin.getSubPartitions(
          key.getPathComponents(), partitionColumns, partitionValues, options.getSchemaConfig());
    }

    throw new UnsupportedOperationException(
        plugin.getClass().getName() + " does not support partition retrieval.");
  }

  @Override
  @WithSpan
  public boolean createOrUpdateDataset(
      NamespaceKey source,
      NamespaceKey datasetPath,
      DatasetConfig datasetConfig,
      NamespaceAttribute... attributes)
      throws NamespaceException {
    final ManagedStoragePlugin plugin = pluginRetriever.getPlugin(source.getRoot(), true);
    if (plugin == null) {
      throw UserException.validationError()
          .message("Unknown source %s", datasetPath.getRoot())
          .buildSilently();
    }

    return datasets.createOrUpdateDataset(plugin, datasetPath, datasetConfig, attributes);
  }

  @Override
  public void updateDatasetSchema(NamespaceKey datasetKey, BatchSchema newSchema) {
    final ManagedStoragePlugin plugin = pluginRetriever.getPlugin(datasetKey.getRoot(), true);

    boolean success;
    try {
      do {
        DatasetConfig oldConfig = systemNamespaceService.getDataset(datasetKey);
        DatasetConfig newConfig = plugin.getUpdatedDatasetConfig(oldConfig, newSchema);
        success = storeSchema(datasetKey, newConfig);
      } while (!success);
    } catch (NamespaceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void updateDatasetField(
      NamespaceKey datasetKey, String originField, CompleteType fieldSchema) {
    boolean success;
    try {
      do {
        DatasetConfig oldDatasetConfig = systemNamespaceService.getDataset(datasetKey);

        Serializer<DatasetConfig, byte[]> serializer =
            ProtostuffSerializer.of(DatasetConfig.getSchema());
        DatasetConfig newDatasetConfig =
            serializer.deserialize(serializer.serialize(oldDatasetConfig));

        List<DatasetField> datasetFields = newDatasetConfig.getDatasetFieldsList();
        if (datasetFields == null) {
          datasetFields = Lists.newArrayList();
        }

        DatasetField datasetField =
            datasetFields.stream()
                .filter(input -> originField.equals(input.getFieldName()))
                .findFirst()
                .orElse(null);

        if (datasetField == null) {
          datasetField = new DatasetField().setFieldName(originField);
          datasetFields.add(datasetField);
        }

        datasetField.setFieldSchema(ByteString.copyFrom(fieldSchema.serialize()));
        newDatasetConfig.setDatasetFieldsList(datasetFields);

        success = storeSchema(datasetKey, newDatasetConfig);
      } while (!success);
    } catch (NamespaceException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean storeSchema(NamespaceKey key, DatasetConfig config) throws NamespaceException {
    try {
      systemNamespaceService.addOrUpdateDataset(key, config);
      return true;
    } catch (ConcurrentModificationException ex) {
      return false;
    }
  }

  @Override
  public <T extends StoragePlugin> T getSource(String name) {
    return sourceModifier.getSource(name);
  }

  @Override
  @WithSpan
  public void createSource(SourceConfig config, NamespaceAttribute... attributes) {
    if (Boolean.TRUE.equals(config.getIsPrimaryCatalog())) {
      List<SourceConfig> sources = systemNamespaceService.getSources();
      for (SourceConfig source : sources) {
        if (Boolean.TRUE.equals(source.getIsPrimaryCatalog())) {
          throw UserException.validationError()
              .message("Only one primary catalog is allowed.")
              .buildSilently();
        }
      }
    }

    sourceModifier.createSource(config, attributes);
  }

  @Override
  public void updateSource(SourceConfig config, NamespaceAttribute... attributes) {
    try {
      SourceConfig source = systemNamespaceService.getSource(config.getKey());
      if (Boolean.TRUE.equals(config.getIsPrimaryCatalog())
          != Boolean.TRUE.equals(source.getIsPrimaryCatalog())) {
        throw UserException.validationError()
            .message("Cannot modify primary catalog setting of a source.")
            .buildSilently();
      }
    } catch (NamespaceException ignore) {
    }

    final SourceConfig before = pluginRetriever.getPlugin(config.getName(), true).getConfig();

    sourceModifier.updateSource(config, attributes);

    catalogStatusEvents.publish(SourceUpdateCatalogStatusEvent.of(before, config));
  }

  @Override
  @WithSpan
  public void deleteSource(SourceConfig config) {
    if (Boolean.TRUE.equals(config.getIsPrimaryCatalog())) {
      throw UserException.validationError()
          .message(
              "Deletion of source %s is prohibited as it is the primary catalog", config.getName())
          .buildSilently();
    }
    SourceNamespaceService.DeleteCallback deleteCallback =
        (DatasetConfig datasetConfig) -> {
          CatalogUtil.addIcebergMetadataOrphan(datasetConfig, orphanage);
        };

    sourceModifier.deleteSource(config, deleteCallback);

    catalogStatusEvents.publish(SourceDeletionCatalogStatusEvent.of(config));
  }

  @Override
  public ResolvedVersionContext resolveVersionContext(
      String sourceName, VersionContext versionContext)
      throws ReferenceNotFoundException, NoDefaultBranchException, ReferenceConflictException {
    return versionContextResolverImpl.resolveVersionContext(sourceName, versionContext);
  }

  @Override
  public void validatePrivilege(NamespaceKey key, SqlGrant.Privilege privilege) {
    // For the default implementation, don't validate privilege.
  }

  @Override
  public void validateOwnership(CatalogEntityKey key) {
    // For the default implementation, don't validate privilege.
  }

  private enum SchemaType {
    SOURCE,
    SPACE,
    HOME,
    UNKNOWN
  }

  @Override
  public Iterator<com.dremio.service.catalog.Catalog> listCatalogs(SearchQuery searchQuery) {
    return iscDelegate.listCatalogs(searchQuery);
  }

  @Override
  public Iterator<Schema> listSchemata(SearchQuery searchQuery) {
    return iscDelegate.listSchemata(searchQuery);
  }

  @Override
  public Iterator<Table> listTables(SearchQuery searchQuery) {
    return iscDelegate.listTables(searchQuery);
  }

  @Override
  public Iterator<com.dremio.service.catalog.View> listViews(SearchQuery searchQuery) {
    return iscDelegate.listViews(searchQuery);
  }

  @Override
  public Iterator<TableSchema> listTableSchemata(SearchQuery searchQuery) {
    return iscDelegate.listTableSchemata(searchQuery);
  }

  @Override
  public Catalog visit(java.util.function.Function<Catalog, Catalog> catalogRewrite) {
    return catalogRewrite.apply(this);
  }

  public interface IdentityResolver {
    CatalogIdentity getOwner(List<String> path) throws NamespaceException;

    NamespaceIdentity toNamespaceIdentity(CatalogIdentity identity);
  }

  private DremioTable getTableForTimeTravel(VersionedDatasetId versionedDatasetId) {
    Preconditions.checkNotNull(versionedDatasetId);
    Preconditions.checkState(versionedDatasetId.getVersionContext().isTimeTravelType());
    CatalogEntityKey catalogEntityKey =
        CatalogEntityKey.newBuilder()
            .keyComponents(versionedDatasetId.getTableKey())
            .tableVersionContext(versionedDatasetId.getVersionContext())
            .build();
    try {
      return getTableSnapshotForQuery(catalogEntityKey);
    } catch (UserException e) {
      // getTableSnapshot returns a UserException when table is not found.
      return null;
    }
  }

  @Override
  public MetadataRequestOptions getMetadataRequestOptions() {
    return options;
  }

  @Override
  public void clearDatasetCache(final NamespaceKey dataset, final TableVersionContext context) {
    if (context != null) {
      versionContextResolverImpl.invalidateVersionContext(
          dataset.getRoot(), context.asVersionContext());
      versionContextResolverImpl.invalidateVersionContext(
          dataset.getRoot(), VersionContext.NOT_SPECIFIED);
    }
  }

  @Override
  public void clearPermissionCache(String sourceName) {
    throw new UnsupportedOperationException();
  }

  //// Begin: NamespacePassthrough Methods
  @Override
  public boolean existsById(CatalogEntityId id) {
    if (id.toVersionedDatasetId().isPresent()) {
      VersionedDatasetId versionedDatasetId = id.toVersionedDatasetId().get();
      List<String> fullPath = versionedDatasetId.getTableKey();
      VersionContext versionContext = versionedDatasetId.getVersionContext().asVersionContext();
      StoragePlugin plugin = getSource(fullPath.get(0));
      Preconditions.checkArgument(plugin.isWrapperFor(VersionedPlugin.class));
      VersionedPlugin versionedPlugin = plugin.unwrap(VersionedPlugin.class);
      ResolvedVersionContext resolvedVersionContext =
          versionedPlugin.resolveVersionContext(versionContext);
      List<String> contentKey = fullPath.subList(1, fullPath.size());
      String contentId = versionedPlugin.getContentId(contentKey, resolvedVersionContext);
      return contentId != null;
    }

    Optional<String> namespaceEntityId = id.toNamespaceEntityId();
    return namespaceEntityId
        .filter(s -> userNamespaceService.getEntityById(new EntityId(s)).isPresent())
        .isPresent();
  }

  @Override
  public List<NameSpaceContainer> getEntities(List<NamespaceKey> lookupKeys) {
    return userNamespaceService.getEntities(lookupKeys);
  }

  @Override
  public void addOrUpdateDataset(
      NamespaceKey datasetPath, DatasetConfig dataset, NamespaceAttribute... attributes)
      throws NamespaceException {
    userNamespaceService.addOrUpdateDataset(datasetPath, dataset, attributes);
  }

  @Override
  public DatasetConfig renameDataset(NamespaceKey oldDatasetPath, NamespaceKey newDatasetPath)
      throws NamespaceException {
    return userNamespaceService.renameDataset(oldDatasetPath, newDatasetPath);
  }

  @Override
  public String getEntityIdByPath(NamespaceKey entityPath) throws NamespaceNotFoundException {
    return userNamespaceService.getEntityIdByPath(entityPath);
  }

  @Override
  public NameSpaceContainer getEntityByPath(NamespaceKey entityPath) throws NamespaceException {
    return userNamespaceService.getEntityByPath(entityPath);
  }

  @Override
  public DatasetConfig getDataset(NamespaceKey datasetPath) throws NamespaceException {
    return userNamespaceService.getDataset(datasetPath);
  }

  @Override
  public Iterable<NamespaceKey> getAllDatasets(final NamespaceKey parent)
      throws NamespaceException {
    return userNamespaceService.getAllDatasets(parent);
  }

  @Override
  public void deleteDataset(
      NamespaceKey datasetPath, String version, NamespaceAttribute... attributes)
      throws NamespaceException {
    userNamespaceService.deleteDataset(datasetPath, version, attributes);
  }

  @Override
  public List<Integer> getCounts(SearchTypes.SearchQuery... queries) throws NamespaceException {
    return userNamespaceService.getCounts(queries);
  }

  @Override
  public Iterable<Document<NamespaceKey, NameSpaceContainer>> find(FindByCondition condition) {
    return userNamespaceService.find(condition);
  }

  @Override
  public List<NameSpaceContainer> getEntitiesByIds(List<EntityId> ids) {
    return userNamespaceService.getEntitiesByIds(ids);
  }
  //// End: NamespacePassthrough Methods
}
