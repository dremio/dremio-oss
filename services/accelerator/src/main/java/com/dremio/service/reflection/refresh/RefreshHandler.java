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

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.util.List;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.protos.AttemptId;
import com.dremio.datastore.ProtostuffSerializer;
import com.dremio.datastore.Serializer;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.acceleration.StrippingFactory;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.ScreenRel;
import com.dremio.exec.planner.logical.WriterRel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.handlers.query.SqlToPlanHandler;
import com.dremio.exec.planner.sql.parser.SqlRefreshReflection;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.exec.store.sys.accel.AccelerationManager.ExcludedReflectionsProvider;
import com.dremio.resource.common.ReflectionRoutingManager;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.service.reflection.ReflectionGoalChecker;
import com.dremio.service.reflection.ReflectionService;
import com.dremio.service.reflection.ReflectionServiceImpl;
import com.dremio.service.reflection.ReflectionSettings;
import com.dremio.service.reflection.WriterOptionManager;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.MaterializationState;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.RefreshDecision;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import io.protostuff.ByteString;

/**
 * Sql syntax handler for the $MATERIALIZE command, an internal command used to materialize reflections.
 */
public class RefreshHandler implements SqlToPlanHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RefreshHandler.class);

  public static final String DECISION_NAME = RefreshDecision.class.getName();
  public static final Serializer<RefreshDecision, byte[]> ABSTRACT_SERIALIZER = ProtostuffSerializer.of(RefreshDecision.getSchema());

  private final WriterOptionManager writerOptionManager;

  private String textPlan;

  public RefreshHandler() {
    this.writerOptionManager = WriterOptionManager.Instance;
  }

  @Override
  public PhysicalPlan getPlan(SqlHandlerConfig config, String sql, SqlNode sqlNode) throws Exception {
    try{
      final SqlRefreshReflection materialize = SqlNodeUtil.unwrap(sqlNode, SqlRefreshReflection.class);

      if(!SystemUser.SYSTEM_USERNAME.equals(config.getContext().getQueryUserName())) {
        throw SqlExceptionHelper.parseError("$MATERIALIZE not supported.", sql, materialize.getParserPosition())
          .build(logger);
      }

      ReflectionService service = config.getContext().getAccelerationManager().unwrap(ReflectionService.class);

      // Let's validate the plan.
      ReflectionId reflectionId = new ReflectionId(materialize.getReflectionId());
      Optional<ReflectionGoal> goalOpt = service.getGoal(reflectionId);
      if(!goalOpt.isPresent()) {
        throw SqlExceptionHelper.parseError("Unknown reflection id.", sql, materialize.getReflectionIdPos()).build(logger);
      }
      final ReflectionGoal goal = goalOpt.get();

      Optional<ReflectionEntry> entryOpt = service.getEntry(reflectionId);
      if(!entryOpt.isPresent()) {
        throw SqlExceptionHelper.parseError("Unknown reflection id.", sql, materialize.getReflectionIdPos()).build(logger);
      }
      final ReflectionEntry entry = entryOpt.get();
      if(!ReflectionGoalChecker.checkGoal(goal, entry)) {
        throw UserException.validationError().message("Reflection has been updated since reflection was scheduled.").build(logger);
      }

      Optional<Materialization> materializationOpt = service.getMaterialization(new MaterializationId(materialize.getMaterializationId()));
      if(!materializationOpt.isPresent()) {
        throw SqlExceptionHelper.parseError("Unknown materialization id.", sql, materialize.getReflectionIdPos()).build(logger);
      }
      final Materialization materialization = materializationOpt.get();
      if(!ReflectionGoalChecker.checkGoal(goal, materialization)) {
        throw UserException.validationError().message("Reflection has been updated since reflection was scheduled.").build(logger);
      }

      if(materialization.getState() != MaterializationState.RUNNING) {
        throw UserException.validationError()
        .message("Materialization in unexpected state for Reflection %s, Materialization %s. State: %s", reflectionId.getId(), materialization.getId(), materialization.getState())
        .build(logger);
      }

      final RefreshHelper helper = ((ReflectionServiceImpl) service).getRefreshHelper();
      final NamespaceService namespace = helper.getNamespace();
      final ReflectionSettings reflectionSettings = helper.getReflectionSettings();
      final MaterializationStore materializationStore = helper.getMaterializationStore();

      // Disable default raw reflections during plan generation for a refresh
      config.getConverter().getSubstitutionProvider().disableDefaultRawReflection();
      RefreshDecision[] refreshDecisions = new RefreshDecision[1];
      final RelNode initial = determineMaterializationPlan(
          config,
          goal,
          entry,
          materialization,
          service.getExcludedReflectionsProvider(),
          namespace,
          config.getContext().getConfig(),
          reflectionSettings,
          materializationStore,
          refreshDecisions);
      config.getConverter().getSubstitutionProvider().resetDefaultRawReflection();

      final Rel drel = PrelTransformer.convertToDrelMaintainingNames(config, initial);

      // Append the attempt number to the table path
      final UserBitShared.QueryId queryId = config.getContext().getQueryId();
      final AttemptId attemptId = AttemptId.of(queryId);

      final boolean isIcebergIncrementalRefresh = isIcebergInsertRefresh(materialization, refreshDecisions[0]);
      final String materializationPath =  isIcebergIncrementalRefresh ?
        materialization.getBasePath() : materialization.getId().getId() + "_" + attemptId.getAttemptNum();
      final String materializationId = materializationPath.split("_")[0];
      final List<String> tablePath =  ImmutableList.of(
          ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME,
          reflectionId.getId(),
          materializationPath);

      boolean isCreate = !isIcebergInsertRefresh(materialization, refreshDecisions[0]);

      ByteString extendedByteString = null;
      if(!isCreate && materialization.getIsIcebergDataset()) {
        DremioTable table = config.getContext().getCatalog().getTable(
          new NamespaceKey(
            Lists.newArrayList(ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME,
            reflectionId.getId(),
            materializationId)));
        Preconditions.checkState(table != null, "Base table doesn't exist");
        extendedByteString = table.getDatasetConfig().getReadDefinition().getExtendedProperty();
      }

      final List<String> fields = drel.getRowType().getFieldNames();

      WriterOptions writerOptions = writerOptionManager.buildWriterOptionForReflectionGoal(
        0, goal, fields, materialization.getIsIcebergDataset(), isCreate, extendedByteString);

      IcebergTableProps icebergTableProps = materialization.getIsIcebergDataset() ?
        getIcebergTableProps(materialization, refreshDecisions, attemptId, writerOptions.getPartitionColumns())
        : null;

      final Rel writerDrel = new WriterRel(
        drel.getCluster(),
        drel.getCluster().traitSet().plus(Rel.LOGICAL),
        drel,
        config.getContext().getCatalog().createNewTable(
          new NamespaceKey(tablePath),
          icebergTableProps,
          writerOptions,
          ImmutableMap.of()),
        initial.getRowType());

      final RelNode doubleWriter = SqlHandlerUtil.storeQueryResultsIfNeeded(config.getConverter().getParserConfig(), config.getContext(), writerDrel);

      final ScreenRel screen = new ScreenRel(writerDrel.getCluster(), writerDrel.getTraitSet(), doubleWriter);

      final Pair<Prel, String> convertToPrel = PrelTransformer.convertToPrel(config, screen);
      final Prel prel = convertToPrel.getKey();
      this.textPlan = convertToPrel.getValue();
      PhysicalOperator pop = PrelTransformer.convertToPop(config, prel);
      PhysicalPlan plan = PrelTransformer.convertToPlan(config, pop);
      if (logger.isTraceEnabled()) {
        PrelTransformer.log(config, "Dremio Plan", plan, logger);
      }

      //before return, check and set reflection routing information
      final String datasetId = goal.getDatasetId();
      final Catalog catalog = config.getContext().getCatalog();
      ReflectionRoutingManager reflectionRoutingManager = config.getContext().getReflectionRoutingManager();
      if (reflectionRoutingManager != null) {
        DatasetConfig datasetConfig = catalog.getTable(datasetId).getDatasetConfig();
        if (reflectionRoutingManager.getIsQueue()) {
          String queueId = datasetConfig.getQueueId();
          if (queueId == null) {
            queueId = getInheritedReflectionRouting(true, datasetConfig, config);
          }
          final String queueName = reflectionRoutingManager.getQueueNameById(queueId);
          if (queueName != null && reflectionRoutingManager.checkQueueExists(queueName)) {
            config.getContext().getSession().setRoutingQueue(queueName);
          } else if (queueName != null) {
            logger.warn(String.format("Cannot route to queue %s. Using the default queue instead.", queueName));
          }
        } else {
          String engineName = datasetConfig.getEngineName();
          if (engineName == null) {
            engineName = getInheritedReflectionRouting(false, datasetConfig, config);
          }
          if (engineName != null && reflectionRoutingManager.checkEngineExists(engineName)) {
            config.getContext().getSession().setRoutingEngine(engineName);
          } else if (engineName != null) {
            logger.warn(String.format("Cannot route to engine %s. Using the default engine instead.", engineName));
          }
        }
      }

      return plan;

    }catch(Exception ex){
      throw SqlExceptionHelper.coerceException(logger, sql, ex, true);
    }
  }

  private String getInheritedReflectionRouting(boolean isQueue, DatasetConfig datasetConfig, SqlHandlerConfig config) {
    ImmutableList<String> pathList = ImmutableList.copyOf(datasetConfig.getFullPathList());
    // We want to try inherit routing queue from folder or space level.
    // The last entry in the path list will be the name of the current dataset,
    // so we remove it since it isn't a space or folder.
    pathList = pathList.subList(0, pathList.size() - 1);
    while (!pathList.isEmpty()) {
      if (pathList.size() == 1) {
        try {
          SpaceConfig spaceConfig = config.getContext().getNamespaceService(SYSTEM_USERNAME).getSpace(new NamespaceKey(pathList));
          if (isQueue) {
            String inheritedQueueId = spaceConfig.getQueueId();
            if (inheritedQueueId != null) {
              return inheritedQueueId;
            }
          } else {
            String inheritedEngineName = spaceConfig.getEngineName();
            if (inheritedEngineName != null) {
              return inheritedEngineName;
            }
          }
        } catch (NamespaceException e) {
          logger.trace("Could not find space: " + pathList);
        }
      } else {
        try {
          FolderConfig folderConfig = config.getContext().getNamespaceService(SYSTEM_USERNAME)
            .getFolder(new NamespaceKey(pathList));
          if (isQueue) {
            String inheritedQueueId = folderConfig.getQueueId();
            if (inheritedQueueId != null) {
              return inheritedQueueId;
            }
          } else {
            String inheritedEngineName = folderConfig.getQueueId();
            if (inheritedEngineName != null) {
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

  private IcebergTableProps getIcebergTableProps(Materialization materialization, RefreshDecision[] refreshDecisions,
                                                 AttemptId attemptId, List<String> partitionColumns) {
    IcebergTableProps icebergTableProps;
    if (isIcebergInsertRefresh(materialization, refreshDecisions[0])) {
      icebergTableProps = new IcebergTableProps(null, attemptId.toString(),
        null, partitionColumns,
        IcebergCommandType.INSERT, materialization.getBasePath(), null, null);

    } else {
      icebergTableProps = new IcebergTableProps(null, attemptId.toString(),
        null, partitionColumns,
        IcebergCommandType.CREATE, materialization.getId().getId() + "_" + attemptId.getAttemptNum(), null, null);
    }
    return icebergTableProps;
  }

  private boolean isIcebergInsertRefresh(Materialization materialization, RefreshDecision refreshDecision) {
    return materialization.getIsIcebergDataset() &&
      !refreshDecision.getInitialRefresh() &&
      materialization.getBasePath() != null &&
      !materialization.getBasePath().isEmpty();
  }

  private RelNode determineMaterializationPlan(
      final SqlHandlerConfig sqlHandlerConfig,
      ReflectionGoal goal,
      ReflectionEntry entry,
      Materialization materialization,
      ExcludedReflectionsProvider exclusionsProvider,
      NamespaceService namespace,
      SabotConfig config,
      ReflectionSettings reflectionSettings,
      MaterializationStore materializationStore,
      RefreshDecision[] refreshDecisions) {

    final ReflectionPlanGenerator planGenerator = new ReflectionPlanGenerator(sqlHandlerConfig, namespace,
      config, goal, entry, materialization,
      reflectionSettings, materializationStore, getForceFullRefresh(materialization), StrippingFactory.LATEST_STRIP_VERSION);

    final RelNode normalizedPlan = planGenerator.generateNormalizedPlan();


    // avoid accelerating this CTAS with the materialization itself
    // we set exclusions before we get to the logical phase (since toRel() is triggered in SqlToRelConverter, prior to planning).
    final List<String> exclusions = ImmutableList.<String>builder()
      .addAll(exclusionsProvider.getExcludedReflections(goal.getId().getId()))
      .add(goal.getId().getId())
      .build();
    sqlHandlerConfig.getConverter().getSession().getSubstitutionSettings().setExclusions(exclusions);

    RefreshDecision decision = planGenerator.getRefreshDecision();
    refreshDecisions[0] = decision;

    // save the decision for later.
    sqlHandlerConfig.getConverter().getObserver().recordExtraInfo(DECISION_NAME, ABSTRACT_SERIALIZER.serialize(decision));

    logger.trace("Refresh decision: {}", decision);
    if(logger.isTraceEnabled()) {
      logger.trace(RelOptUtil.toString(normalizedPlan));
    }

    return normalizedPlan;
  }

  private Boolean getForceFullRefresh(Materialization materialization) {
    Boolean forceRefresh = materialization.getForceFullRefresh();
    return forceRefresh == null ? false : forceRefresh;
  }

  @Override
  public String getTextPlan() {
    return textPlan;
  }

}
