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
package com.dremio.service.reflection.refresh;

import static com.dremio.exec.planner.sql.CalciteArrowHelper.fromCalciteRowType;
import static com.dremio.exec.store.iceberg.IcebergSerDe.serializedSchemaAsJson;
import static com.dremio.exec.store.iceberg.IcebergUtils.getIcebergPartitionSpecFromTransforms;
import static com.dremio.exec.store.iceberg.IcebergUtils.getWriterSchema;
import static com.dremio.service.reflection.ReflectionUtils.buildPartitionTransforms;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.catalog.model.dataset.TableVersionType;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.Closeable;
import com.dremio.common.utils.protos.AttemptId;
import com.dremio.datastore.ProtostuffSerializer;
import com.dremio.datastore.Serializer;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.ops.DremioCatalogReader;
import com.dremio.exec.ops.SnapshotDiffContext;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.IcebergWriterOptions;
import com.dremio.exec.physical.base.ImmutableIcebergWriterOptions;
import com.dremio.exec.physical.base.ImmutableTableFormatWriterOptions;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.TableFormatWriterOptions;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.common.MoreRelOptUtil.SimpleReflectionFinderVisitor;
import com.dremio.exec.planner.events.PlannerEventBus;
import com.dremio.exec.planner.events.PlannerEventHandler;
import com.dremio.exec.planner.events.ReflectionRequiresExpansionEvent;
import com.dremio.exec.planner.logical.IncrementalRefreshByPartitionWriterRel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.ScreenRel;
import com.dremio.exec.planner.logical.WriterRel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.sql.PartitionTransform;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.DrelTransformer;
import com.dremio.exec.planner.sql.handlers.PlanLogUtil;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.commands.HandlerToPreparePlanBase;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.handlers.query.SqlToPlanHandler;
import com.dremio.exec.planner.sql.parser.SqlRefreshReflection;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.exec.store.sys.accel.AccelerationManager.ExcludedReflectionsProvider;
import com.dremio.options.OptionValue;
import com.dremio.resource.common.ReflectionRoutingManager;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.service.reflection.ReflectionGoalChecker;
import com.dremio.service.reflection.ReflectionOptions;
import com.dremio.service.reflection.ReflectionService;
import com.dremio.service.reflection.ReflectionServiceImpl;
import com.dremio.service.reflection.ReflectionSettings;
import com.dremio.service.reflection.ReflectionUtils;
import com.dremio.service.reflection.WriterOptionManager;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.MaterializationState;
import com.dremio.service.reflection.proto.ReflectionDetails;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.proto.RefreshDecision;
import com.dremio.service.reflection.refresh.ReflectionPlanGenerator.RefreshDecisionWrapper;
import com.dremio.service.reflection.store.DependenciesStore;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.protostuff.ByteString;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

/**
 * Sql syntax handler for the $MATERIALIZE command, an internal command used to materialize
 * reflections.
 */
public class RefreshHandler implements SqlToPlanHandler {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(RefreshHandler.class);
  public static final String DECISION_NAME = RefreshDecision.class.getName();
  public static final Serializer<RefreshDecision, byte[]> ABSTRACT_SERIALIZER =
      ProtostuffSerializer.of(RefreshDecision.getSchema());

  private final WriterOptionManager writerOptionManager;

  private String textPlan;
  private Rel drel;

  public RefreshHandler() {
    this.writerOptionManager = WriterOptionManager.Instance;
  }

  @Override
  public PhysicalPlan getPlan(SqlHandlerConfig config, String sql, SqlNode sqlNode)
      throws Exception {
    try {
      // Disable row level runtime filtering so the materialization can be properly marked if it can
      // be used as starflake or not.
      config
          .getContext()
          .getOptions()
          .setOption(
              OptionValue.createBoolean(
                  OptionValue.OptionType.QUERY,
                  ExecConstants.ENABLE_ROW_LEVEL_RUNTIME_FILTERING.getOptionName(),
                  false));
      final SqlRefreshReflection materialize =
          SqlNodeUtil.unwrap(sqlNode, SqlRefreshReflection.class);

      if (!SystemUser.SYSTEM_USERNAME.equals(config.getContext().getQueryUserName())) {
        throw SqlExceptionHelper.parseError(
                "User \""
                    + config.getContext().getQueryUserName()
                    + "\" is not allowed to run REFRESH REFLECTION command.",
                sql,
                materialize.getParserPosition())
            .build(logger);
      }

      ReflectionService service =
          config.getContext().getAccelerationManager().unwrap(ReflectionService.class);

      // Let's validate the plan.
      ReflectionId reflectionId = new ReflectionId(materialize.getReflectionId());
      Optional<ReflectionGoal> goalOpt = service.getGoal(reflectionId);
      if (!goalOpt.isPresent()) {
        throw SqlExceptionHelper.parseError(
                "Unknown reflection id.", sql, materialize.getReflectionIdPos())
            .build(logger);
      }
      final ReflectionGoal goal = goalOpt.get();

      Optional<ReflectionEntry> entryOpt = service.getEntry(reflectionId);
      if (!entryOpt.isPresent()) {
        throw SqlExceptionHelper.parseError(
                "Unknown reflection id.", sql, materialize.getReflectionIdPos())
            .build(logger);
      }
      final ReflectionEntry entry = entryOpt.get();
      if (!ReflectionGoalChecker.checkGoal(goal, entry)) {
        throw UserException.validationError()
            .message("Reflection has been updated since reflection was scheduled.")
            .build(logger);
      }

      Optional<Materialization> materializationOpt =
          service.getMaterialization(new MaterializationId(materialize.getMaterializationId()));
      if (!materializationOpt.isPresent()) {
        throw SqlExceptionHelper.parseError(
                "Unknown materialization id.", sql, materialize.getReflectionIdPos())
            .build(logger);
      }
      final Materialization materialization = materializationOpt.get();
      if (!ReflectionGoalChecker.checkGoal(goal, materialization)) {
        throw UserException.validationError()
            .message("Reflection has been updated since reflection was scheduled.")
            .build(logger);
      }

      if (materialization.getState() != MaterializationState.RUNNING) {
        throw UserException.validationError()
            .message(
                "Materialization in unexpected state for Reflection %s, Materialization %s. State: %s",
                reflectionId.getId(), materialization.getId(), materialization.getState())
            .build(logger);
      }

      final RefreshHelper helper = ((ReflectionServiceImpl) service).getRefreshHelper();
      final ReflectionSettings reflectionSettings = helper.getReflectionSettings();
      final MaterializationStore materializationStore = helper.getMaterializationStore();
      final CatalogService catalogService = helper.getCatalogService();
      final DependenciesStore dependenciesStore = helper.getDependenciesStore();

      RefreshDecision[] refreshDecisions = new RefreshDecision[1];

      SnapshotDiffContext[] snapshotDiffContextPointer = new SnapshotDiffContext[1];

      final RelNode initial =
          determineMaterializationPlan(
              config,
              goal,
              entry,
              materialization,
              service.getExcludedReflectionsProvider(),
              catalogService,
              config.getContext().getConfig(),
              reflectionSettings,
              materializationStore,
              dependenciesStore,
              refreshDecisions,
              snapshotDiffContextPointer);
      final BatchSchema batchSchema = fromCalciteRowType(initial.getRowType());
      drel = DrelTransformer.convertToDrelMaintainingNames(config, initial);

      // Append the attempt number to the table path
      final UserBitShared.QueryId queryId = config.getContext().getQueryId();
      final AttemptId attemptId = AttemptId.of(queryId);

      final List<String> tablePath =
          getRefreshPath(reflectionId, materialization, refreshDecisions[0], attemptId);

      List<String> primaryKey = getPrimaryKeyFromMaterializationPlan(initial);
      if (!CollectionUtils.isEmpty(primaryKey)) {
        materialization.setPrimaryKeyList(primaryKey);
        materializationStore.save(materialization);
      }

      boolean isCreate = !isIcebergInsertRefresh(materialization, refreshDecisions[0]);

      ByteString extendedByteString = null;
      DremioTable oldReflection = null;
      if (!isCreate && materialization.getIsIcebergDataset()) {
        Materialization lastDoneMaterialization =
            Preconditions.checkNotNull(
                materializationStore.getLastMaterializationDone(reflectionId),
                "Incremental refresh did not find previous DONE materialization");
        NamespaceKey namespaceKey =
            new NamespaceKey(
                Lists.newArrayList(
                    ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME,
                    reflectionId.getId(),
                    lastDoneMaterialization.getId().getId()));
        oldReflection = config.getContext().getCatalog().getTable(namespaceKey);
        Preconditions.checkState(oldReflection != null, "Base table doesn't exist");
        extendedByteString =
            oldReflection.getDatasetConfig().getReadDefinition().getExtendedProperty();
      }

      final List<String> fields = drel.getRowType().getFieldNames();

      WriterOptions writerOptions =
          writerOptionManager.buildWriterOptionForReflectionGoal(
              0,
              goal,
              fields,
              materialization.getIsIcebergDataset(),
              isCreate,
              extendedByteString,
              snapshotDiffContextPointer[0],
              materialization.getPreviousIcebergSnapshot());

      final ReflectionPartitionInfo reflectionPartitionInfo =
          buildReflectionPartitionInfo(materialization, config, goal, batchSchema);

      IcebergTableProps icebergTableProps =
          materialization.getIsIcebergDataset()
              ? getIcebergTableProps(
                  materialization,
                  refreshDecisions,
                  attemptId,
                  writerOptions.getPartitionColumns(),
                  writerOptions,
                  reflectionPartitionInfo,
                  snapshotDiffContextPointer[0])
              : null;

      Rel writerDrel = null;
      if (snapshotDiffContextPointer[0] != null
          && snapshotDiffContextPointer[0].getFilterApplyOptions()
              == SnapshotDiffContext.FilterApplyOptions.FILTER_PARTITIONS) {
        writerDrel =
            getIcebergIncrementalRefreshWriterRel(
                snapshotDiffContextPointer[0],
                materialize,
                sql,
                entry,
                config,
                tablePath,
                icebergTableProps,
                writerOptions,
                initial,
                oldReflection,
                materializationStore,
                materialization);
      }
      // either first republish, or we could not use incremental refresh
      if (writerDrel == null) {
        writerDrel =
            new WriterRel(
                drel.getCluster(),
                drel.getCluster().traitSet().plus(Rel.LOGICAL),
                drel,
                config
                    .getContext()
                    .getCatalog()
                    .createNewTable(
                        new NamespaceKey(tablePath),
                        icebergTableProps,
                        writerOptions,
                        ImmutableMap.of()),
                initial.getRowType());
      }

      final RelNode doubleWriter =
          SqlHandlerUtil.storeQueryResultsIfNeeded(
              config.getConverter().getParserConfig(), config.getContext(), writerDrel);

      final ScreenRel screen =
          new ScreenRel(writerDrel.getCluster(), writerDrel.getTraitSet(), doubleWriter);

      final Pair<Prel, String> convertToPrel = PrelTransformer.convertToPrel(config, screen);
      final Prel prel = convertToPrel.getKey();
      this.textPlan = convertToPrel.getValue();
      PhysicalOperator pop = PrelTransformer.convertToPop(config, prel);
      PhysicalPlan plan = PrelTransformer.convertToPlan(config, pop);
      if (logger.isTraceEnabled()) {
        PlanLogUtil.log(config, "Dremio Plan", plan, logger);
      }

      // before return, check and set reflection routing information
      final String datasetId = goal.getDatasetId();
      final Catalog catalog = config.getContext().getCatalog();
      ReflectionRoutingManager reflectionRoutingManager =
          config.getContext().getReflectionRoutingManager();
      if (reflectionRoutingManager != null) {
        DatasetConfig datasetConfig = catalog.getTable(datasetId).getDatasetConfig();
        boolean inheritanceEnabled =
            config
                .getContext()
                .getOptions()
                .getOption("planner.reflection_routing_inheritance_enabled")
                .getBoolVal();
        if (reflectionRoutingManager.getIsQueue()) {
          String queueId = datasetConfig.getQueueId();
          final String queueName;
          if (queueId == null && inheritanceEnabled) {
            queueName = getInheritedReflectionRouting(true, datasetConfig, config);
          } else {
            queueName = reflectionRoutingManager.getQueueNameById(queueId);
          }
          if (queueName != null && reflectionRoutingManager.checkQueueExists(queueName)) {
            config.getContext().getSession().setRoutingQueue(queueName);
          } else if (queueName != null) {
            logger.warn(
                String.format(
                    "Cannot route to queue %s. Using the default queue instead.", queueName));
          }
        } else {
          String engineName = datasetConfig.getEngineName();
          if (engineName == null && inheritanceEnabled) {
            engineName = getInheritedReflectionRouting(false, datasetConfig, config);
          }
          if (engineName != null && reflectionRoutingManager.checkEngineExists(engineName)) {
            config.getContext().getSession().setRoutingEngine(engineName);
          } else if (engineName != null) {
            logger.warn(
                String.format(
                    "Cannot route to engine %s. Using the default engine instead.", engineName));
          }
        }
      }

      return plan;

    } catch (Exception ex) {
      throw SqlExceptionHelper.coerceException(logger, sql, ex, true);
    }
  }

  /**
   * Builds an IncrementalRefreshByPartitionWriterRel that is used for persisting the updated
   * reflection For now this class is only used when we have Incremental Refresh by Partition We
   * pass in some additional arguments that are needed to build a delete plan which will remove any
   * data from the old reflection belonging to the updated partitions. The data in those partition
   * is then recalculated, and inserted into the reflection
   *
   * @param snapshotDiffContext - Snapshot Diff context for the current incremental refresh
   * @param materialize the SqlRefreshReflection for the reflection
   * @param sql the SQL for the reflection as a string
   * @param entry the reflection entry
   * @param config the SqlHandlerConfig for the reflection
   * @param tablePath path to the reflection table
   * @param icebergTableProps Iceberg Table Properties for the reflection
   * @param writerOptions writer options for the reflection
   * @param initial Relational algebra representing the initial plan for the reflection
   * @param oldReflection Dremio table for the reflection before the current materialization
   * @param materializationStore the materialization store to use to find the latest refresh
   * @param materialization current materialization
   * @return IncrementalRefreshByPartitionWriterRel to use in the plan
   */
  private Rel getIcebergIncrementalRefreshWriterRel(
      final SnapshotDiffContext snapshotDiffContext,
      final SqlRefreshReflection materialize,
      final String sql,
      final ReflectionEntry entry,
      final SqlHandlerConfig config,
      final List<String> tablePath,
      final IcebergTableProps icebergTableProps,
      final WriterOptions writerOptions,
      final RelNode initial,
      final DremioTable oldReflection,
      final MaterializationStore materializationStore,
      final Materialization materialization) {
    // build currentReflection using OldReflection and the previous snapshot ID from the
    // materialization
    final Refresh latestRefresh = materializationStore.getMostRecentRefresh(entry.getId());
    if (latestRefresh == null) {
      throw SqlExceptionHelper.parseError(
              "Could not find the latest refresh of a reflection to incrementally update.",
              sql,
              materialize.getReflectionIdPos())
          .build(logger);
    }
    final NamespaceKey namespaceKey = oldReflection.getPath();
    String previousReflectionSnapshotID = null;
    if (latestRefresh.getIsIcebergRefresh() != null && latestRefresh.getIsIcebergRefresh()) {
      previousReflectionSnapshotID = String.valueOf(materialization.getPreviousIcebergSnapshot());
    }
    if (previousReflectionSnapshotID == null) {
      throw SqlExceptionHelper.parseError(
              "Could not find the Iceberg table and latest SnapshotID of a reflection to incrementally update.",
              sql,
              materialize.getReflectionIdPos())
          .build(logger);
    }

    final MetadataRequestOptions mdRequestOpt =
        MetadataRequestOptions.of(
            SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME)).build());
    DremioTable currentReflection = null;
    final String errorMessage =
        "Could not find reflection snapshot to perform incremental update on. Details: latestRefresh path="
            + latestRefresh.getPath()
            + ". SnapshotID="
            + previousReflectionSnapshotID
            + ". oldReflection path="
            + oldReflection.getPath().toString();
    try {
      CatalogEntityKey catalogEntityKey =
          CatalogEntityKey.newBuilder()
              .keyComponents(namespaceKey.getPathComponents())
              .tableVersionContext(
                  new TableVersionContext(
                      TableVersionType.SNAPSHOT_ID, previousReflectionSnapshotID))
              .build();
      currentReflection =
          config
              .getContext()
              .getCatalogService()
              .getCatalog(mdRequestOpt)
              .getTableSnapshot(catalogEntityKey);
    } catch (IllegalArgumentException e) {
      SqlExceptionHelper.parseError(errorMessage, sql, materialize.getReflectionIdPos())
          .build(logger);
    }
    if (currentReflection == null) {
      SqlExceptionHelper.parseError(errorMessage, sql, materialize.getReflectionIdPos())
          .build(logger);
    }
    // build a dremioPrepareTable for currentReflection
    final DremioCatalogReader dremioCatalogReader =
        new DremioCatalogReader(config.getConverter().getPlannerCatalog());
    DremioPrepareTable dremioPrepareTable =
        new DremioPrepareTable(
            dremioCatalogReader, JavaTypeFactoryImpl.INSTANCE, currentReflection);

    // Create IncrementalRefreshByPartitionWriterRel instead of WriterRel
    // We need IncrementalRefreshByPartitionWriterRel to pass some additional parameters
    return new IncrementalRefreshByPartitionWriterRel(
        drel.getCluster(),
        drel.getCluster().traitSet().plus(Rel.LOGICAL),
        drel,
        config
            .getContext()
            .getCatalog()
            .createNewTable(
                new NamespaceKey(tablePath), icebergTableProps, writerOptions, ImmutableMap.of()),
        initial.getRowType(),
        config.getContext(),
        currentReflection,
        dremioPrepareTable,
        snapshotDiffContext);
  }

  /**
   * Returns the expected refresh path for the current refresh of a reflection
   *
   * @param reflectionId - the ID of the reflection we are finding the path for
   * @param materialization - materialization for the reflection
   * @param refreshDecision - refresh decision for the reflection
   * @param attemptId - current attempt ID
   * @return The refresh path represented as a list of strings
   */
  public static List<String> getRefreshPath(
      final ReflectionId reflectionId,
      final Materialization materialization,
      final RefreshDecision refreshDecision,
      final AttemptId attemptId) {

    final boolean isIcebergIncrementalRefresh =
        isIcebergInsertRefresh(materialization, refreshDecision);
    final String materializationPath =
        isIcebergIncrementalRefresh
            ? materialization.getBasePath()
            : materialization.getId().getId() + "_" + attemptId.getAttemptNum();
    // if you change the order of materializationPath in return value of this function
    // please make sure you update MATERIALIZATION_ID_REFRESH_PATH_OFFSET
    return ImmutableList.of(
        ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME,
        reflectionId.getId(),
        materializationPath);
  }

  private List<String> getPrimaryKeyFromMaterializationPlan(RelNode node) {
    SimpleReflectionFinderVisitor visitor = new SimpleReflectionFinderVisitor();
    node.accept(visitor);
    final List<String> primaryKey = visitor.getPrimaryKey();
    if (!CollectionUtils.isEmpty(primaryKey)) {
      final List<Integer> indices = visitor.getIndices();
      RelMetadataQuery mq = node.getCluster().getMetadataQuery();
      Set<Integer> topmostNodeFields = new HashSet<>();
      for (int i = 0; i < node.getRowType().getFieldList().size(); i++) {
        RelColumnOrigin origin = mq.getColumnOrigin(node, i);
        if (origin != null) {
          topmostNodeFields.add(origin.getOriginColumnOrdinal());
        }
      }
      if (topmostNodeFields.containsAll(indices)) {
        return primaryKey;
      }
    }
    return null;
  }

  private String getInheritedReflectionRouting(
      boolean isQueue, DatasetConfig datasetConfig, SqlHandlerConfig config) throws Exception {
    ImmutableList<String> pathList = ImmutableList.copyOf(datasetConfig.getFullPathList());
    // We want to try to inherit routing queue from folder or space level.
    // The last entry in the path list will be the name of the current dataset,
    // so we remove it since it isn't a space or folder.
    pathList = pathList.subList(0, pathList.size() - 1);
    ReflectionRoutingManager reflectionRoutingManager =
        config.getContext().getReflectionRoutingManager();
    while (!pathList.isEmpty()) {
      if (pathList.size() == 1) {
        try {
          SpaceConfig spaceConfig =
              config.getContext().getSystemNamespaceService().getSpace(new NamespaceKey(pathList));
          if (isQueue) {
            String inheritedQueueId = spaceConfig.getQueueId();
            if (inheritedQueueId != null) {
              final String queueName = reflectionRoutingManager.getQueueNameById(inheritedQueueId);
              if (queueName != null && reflectionRoutingManager.checkQueueExists(queueName)) {
                return queueName;
              }
            }
          } else {
            String inheritedEngineName = spaceConfig.getEngineName();
            if (inheritedEngineName != null
                && reflectionRoutingManager.checkEngineExists(inheritedEngineName)) {
              return inheritedEngineName;
            }
          }
        } catch (NamespaceException e) {
          logger.trace("Could not find space: " + pathList);
        }
      } else {
        try {
          FolderConfig folderConfig =
              config.getContext().getSystemNamespaceService().getFolder(new NamespaceKey(pathList));
          if (isQueue) {
            String inheritedQueueId = folderConfig.getQueueId();
            if (inheritedQueueId != null) {
              final String queueName = reflectionRoutingManager.getQueueNameById(inheritedQueueId);
              if (queueName != null && reflectionRoutingManager.checkQueueExists(queueName)) {
                return queueName;
              }
            }
          } else {
            String inheritedEngineName = folderConfig.getEngineName();
            if (inheritedEngineName != null
                && reflectionRoutingManager.checkEngineExists(inheritedEngineName)) {
              return inheritedEngineName;
            }
          }
        } catch (NamespaceException e) {
          logger.trace("Could not find folder: " + pathList);
        }
      }
      pathList = pathList.subList(0, pathList.size() - 1);
    }
    return null;
  }

  private IcebergTableProps getIcebergTableProps(
      final Materialization materialization,
      final RefreshDecision[] refreshDecisions,
      final AttemptId attemptId,
      final List<String> partitionColumns,
      final WriterOptions writerOptions,
      final ReflectionPartitionInfo reflectionPartitionInfo,
      final SnapshotDiffContext snapshotDiffContext) {
    final IcebergTableProps icebergTableProps;
    if (isIcebergInsertRefresh(materialization, refreshDecisions[0])) {
      IcebergCommandType icebergCommandType = IcebergCommandType.INSERT;
      if (snapshotDiffContext != null
          && snapshotDiffContext.getFilterApplyOptions()
              == SnapshotDiffContext.FilterApplyOptions.FILTER_PARTITIONS) {
        icebergCommandType = IcebergCommandType.UPDATE;
      }
      icebergTableProps =
          new IcebergTableProps(
              null,
              attemptId.toString(),
              reflectionPartitionInfo.getBatchSchemaToUse(),
              partitionColumns,
              icebergCommandType,
              null,
              materialization.getBasePath(),
              null,
              null,
              reflectionPartitionInfo.getPartitionSpecByteString(),
              reflectionPartitionInfo.getSerializedIcebergSchemaAsJson(),
              null,
              null,
              Collections.emptyMap(),
              null);
    } else {
      icebergTableProps =
          new IcebergTableProps(
              null,
              attemptId.toString(),
              reflectionPartitionInfo.getBatchSchemaToUse(),
              partitionColumns,
              IcebergCommandType.CREATE,
              null,
              materialization.getId().getId() + "_" + attemptId.getAttemptNum(),
              null,
              null,
              reflectionPartitionInfo.getPartitionSpecByteString(),
              reflectionPartitionInfo.getSerializedIcebergSchemaAsJson(),
              null,
              null,
              Collections.emptyMap(),
              null);
    }

    if (reflectionPartitionInfo.getPartitionSpecByteString() != null) {
      final IcebergWriterOptions icebergOptions =
          new ImmutableIcebergWriterOptions.Builder()
              .from(writerOptions.getTableFormatOptions().getIcebergSpecificOptions())
              .setIcebergTableProps(icebergTableProps)
              .build();
      final TableFormatWriterOptions tableFormatWriterOptions =
          new ImmutableTableFormatWriterOptions.Builder()
              .from(writerOptions.getTableFormatOptions())
              .setIcebergSpecificOptions(icebergOptions)
              .build();

      writerOptions.setTableFormatOptions(tableFormatWriterOptions);

      final BatchSchema writerSchema =
          getWriterSchema(reflectionPartitionInfo.batchSchemaToUse, writerOptions);
      icebergTableProps.setFullSchema(writerSchema);
      icebergTableProps.setPersistedFullSchema(reflectionPartitionInfo.batchSchemaToUse);
    }
    return icebergTableProps;
  }

  private class ReflectionPartitionInfo {
    private final BatchSchema batchSchemaToUse;
    private final ByteString partitionSpecByteString;
    private final String serializedIcebergSchemaAsJson;

    public ReflectionPartitionInfo(
        final BatchSchema batchSchemaToUse,
        final ByteString partitionSpecByteString,
        final String serializedIcebergSchemaAsJson) {
      this.batchSchemaToUse = batchSchemaToUse;
      this.partitionSpecByteString = partitionSpecByteString;
      this.serializedIcebergSchemaAsJson = serializedIcebergSchemaAsJson;
    }

    public BatchSchema getBatchSchemaToUse() {
      return batchSchemaToUse;
    }

    public ByteString getPartitionSpecByteString() {
      return partitionSpecByteString;
    }

    public String getSerializedIcebergSchemaAsJson() {
      return serializedIcebergSchemaAsJson;
    }
  }

  /**
   * Build Iceberg partition info for the reflection. We need to build Iceberg Schema to carry the
   * Iceberg partition Info. Some Arrow types do NOT have corresponding Iceberg types, so building
   * Iceberg Schema will fail In that case we will set all the incoming pointers to null and fall
   * back to using partition columns if possible If not possible we will error out and fail Example
   * of problematic cases - CONVERT_FROM(ddd,'JSON') as col1, NULL as col_2 In both cases above the
   * type is NULL, and we will fail when we try to build an Iceberg Schema from the batchSchema
   */
  private ReflectionPartitionInfo buildReflectionPartitionInfo(
      final Materialization materialization,
      final SqlHandlerConfig sqlHandlerConfig,
      final ReflectionGoal goal,
      final BatchSchema batchSchema) {
    final ReflectionDetails reflectionDetails = goal.getDetails();
    final List<PartitionTransform> transforms =
        buildPartitionTransforms(reflectionDetails.getPartitionFieldList());

    Optional<PartitionTransform> firstNonIdentity =
        transforms.stream().filter(x -> !x.isIdentity()).findFirst();
    if (firstNonIdentity.isPresent()) {
      ReflectionUtils.validateNonIdentityTransformAllowed(
          sqlHandlerConfig.getContext().getOptions(), firstNonIdentity.get().getType().getName());
      if (!materialization.getIsIcebergDataset()) {
        throw new UnsupportedOperationException(
            String.format(
                "[%s] partition transform is present, but the reflection uses a non-Iceberg materialization.",
                firstNonIdentity.get().getType().getName()));
      }
    }
    final SchemaConverter schemaConverter = SchemaConverter.getBuilder().build();
    final Schema icebergSchema;
    try {
      icebergSchema = schemaConverter.toIcebergSchema(batchSchema);
    } catch (final UserException e) {
      if (firstNonIdentity.isPresent()) {
        // rethrow the error, we cannot support transformation functions and unsupported datatype
        // together
        throw new UnsupportedOperationException(
            "A combination of Iceberg partition transform and non-Iceberg data types is not supported in the same reflection."
                + e.getMessage());
      }
      // if there are no transforms (all are identity) we can support this workflow by falling back
      // to non-Iceberg workflow using partition
      // this will prevent regressions on cases like dx-59542 and dx-47572
      return new ReflectionPartitionInfo(null, null, null);
    }
    final PartitionSpec partitionSpec =
        getIcebergPartitionSpecFromTransforms(batchSchema, transforms, icebergSchema);
    return new ReflectionPartitionInfo(
        batchSchema,
        ByteString.copyFrom(IcebergSerDe.serializePartitionSpec(partitionSpec)),
        serializedSchemaAsJson(icebergSchema));
  }

  private static boolean isIcebergInsertRefresh(
      Materialization materialization, RefreshDecision refreshDecision) {
    return materialization.getIsIcebergDataset()
        && !refreshDecision.getInitialRefresh()
        && materialization.getBasePath() != null
        && !materialization.getBasePath().isEmpty();
  }

  private RelNode determineMaterializationPlan(
      final SqlHandlerConfig sqlHandlerConfig,
      ReflectionGoal goal,
      ReflectionEntry entry,
      Materialization materialization,
      ExcludedReflectionsProvider exclusionsProvider,
      CatalogService catalogService,
      SabotConfig config,
      ReflectionSettings reflectionSettings,
      MaterializationStore materializationStore,
      DependenciesStore dependenciesStore,
      RefreshDecision[] refreshDecisions,
      SnapshotDiffContext[] snapshotDiffContextPointer) {
    PlannerEventBus plannerEventBus = sqlHandlerConfig.getPlannerEventBus();
    DisableDefaultReflectionEventHandler handler = new DisableDefaultReflectionEventHandler();
    try (Closeable ignored = plannerEventBus.register(handler)) {
      // avoid accelerating this CTAS with the materialization itself
      // we set exclusions before we get to the logical phase (since toRel() is triggered in
      // SqlToRelConverter, prior to planning).
      final List<String> exclusions =
          ImmutableList.<String>builder()
              .addAll(exclusionsProvider.getExcludedReflections(goal.getId().getId()))
              .add(goal.getId().getId())
              .build();
      sqlHandlerConfig
          .getConverter()
          .getSession()
          .getSubstitutionSettings()
          .setExclusions(exclusions);

      // First, generate the plan with no DRRs to determine if the refresh method is incremental or
      // full.
      sqlHandlerConfig.getConverter().getSubstitutionProvider().disableDefaultRawReflection();
      HandlerToPreparePlanBase.RecordingObserver recordingObserver =
          new HandlerToPreparePlanBase.RecordingObserver();
      SqlHandlerConfig recordingConfig = sqlHandlerConfig.cloneWithNewObserver(recordingObserver);
      ReflectionPlanGenerator planGenerator =
          new ReflectionPlanGenerator(
              recordingConfig,
              catalogService,
              config,
              goal,
              entry,
              materialization,
              reflectionSettings,
              materializationStore,
              dependenciesStore,
              getForceFullRefresh(materialization),
              false);
      RelNode normalizedPlan = planGenerator.generateNormalizedPlan();
      final RefreshDecisionWrapper noDefaultReflectionDecisionWrapper =
          planGenerator.getRefreshDecisionWrapper();

      // Default raw reflections can be used to accelerate refresh jobs if the support key for it is
      // enabled AND we are doing a full refresh.
      // Otherwise, record the steps from the original plan to the profile.
      if (sqlHandlerConfig
              .getContext()
              .getOptions()
              .getOption(ReflectionOptions.ACCELERATION_ENABLE_DEFAULT_RAW_REFRESH)
          && (planGenerator
              .getRefreshDecisionWrapper()
              .getRefreshDecision()
              .getAccelerationSettings()
              .getMethod()
              .equals(RefreshMethod.FULL))) {
        planGenerator =
            new ReflectionPlanGenerator(
                sqlHandlerConfig,
                catalogService,
                config,
                goal,
                entry,
                materialization,
                reflectionSettings,
                materializationStore,
                dependenciesStore,
                getForceFullRefresh(materialization),
                false);
        planGenerator.setNoDefaultReflectionDecisionWrapper(noDefaultReflectionDecisionWrapper);
        sqlHandlerConfig.getConverter().getSubstitutionProvider().resetDefaultRawReflection();
        sqlHandlerConfig.getConverter().getPlannerCatalog().clearConvertedCache();
        normalizedPlan = planGenerator.generateNormalizedPlan();
      } else {
        recordingObserver.replay(sqlHandlerConfig.getObserver());
      }
      sqlHandlerConfig
          .getObserver()
          .planRefreshDecision(
              noDefaultReflectionDecisionWrapper.getPlanRefreshDecision(),
              noDefaultReflectionDecisionWrapper.getDuration());

      final RefreshDecision noDefaultReflectionDecision =
          noDefaultReflectionDecisionWrapper.getRefreshDecision();
      refreshDecisions[0] =
          noDefaultReflectionDecision.setDisableDefaultReflection(handler.eventReceived);

      if (noDefaultReflectionDecision.getAccelerationSettings().getMethod()
              == RefreshMethod.INCREMENTAL
          && sqlHandlerConfig
              .getContext()
              .getOptions()
              .getOption(
                  "reflections.planning.exclude.file_based_incremental.iceberg.accelerations")
              .getBoolVal()) {
        sqlHandlerConfig
            .getConverter()
            .getSession()
            .getSubstitutionSettings()
            .setExcludeFileBasedIncremental(true);
      }

      // Save the materialization plan without default raw reflections for reflection matching.
      sqlHandlerConfig
          .getConverter()
          .getObserver()
          .recordExtraInfo(
              DECISION_NAME, ABSTRACT_SERIALIZER.serialize(noDefaultReflectionDecision));

      logger.trace("Refresh decision: {}", noDefaultReflectionDecision);
      if (logger.isTraceEnabled()) {
        logger.trace(RelOptUtil.toString(normalizedPlan));
      }

      snapshotDiffContextPointer[0] = noDefaultReflectionDecisionWrapper.getSnapshotDiffContext();
      return normalizedPlan;
    }
  }

  private Boolean getForceFullRefresh(Materialization materialization) {
    Boolean forceRefresh = materialization.getForceFullRefresh();
    return forceRefresh == null ? false : forceRefresh;
  }

  @Override
  public String getTextPlan() {
    return textPlan;
  }

  @Override
  public Rel getLogicalPlan() {
    return drel;
  }

  private static final class DisableDefaultReflectionEventHandler
      implements PlannerEventHandler<ReflectionRequiresExpansionEvent> {
    private boolean eventReceived = false;

    @Override
    public void handle(ReflectionRequiresExpansionEvent event) {
      eventReceived = true;
    }

    @Override
    public Class<ReflectionRequiresExpansionEvent> supports() {
      return ReflectionRequiresExpansionEvent.class;
    }
  }
}
