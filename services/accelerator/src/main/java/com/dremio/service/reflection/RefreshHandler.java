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
package com.dremio.service.reflection;

import static com.dremio.service.reflection.ReflectionUtils.removeUpdateColumn;
import static com.dremio.service.reflection.proto.ReflectionType.AGGREGATION;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.datastore.ProtostuffSerializer;
import com.dremio.datastore.Serializer;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils.ColumnMaterializationShuttle;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils.FileMaterializationShuttle;
import com.dremio.exec.planner.acceleration.KryoLogicalPlanSerializers;
import com.dremio.exec.planner.acceleration.normalization.Normalizer;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.ScreenRel;
import com.dremio.exec.planner.logical.WriterRel;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.sql.ExtendedToRelContext;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.RelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.handlers.query.SqlToPlanHandler;
import com.dremio.exec.planner.sql.parser.PartitionDistributionStrategy;
import com.dremio.exec.planner.sql.parser.SqlRefreshReflection;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.sys.accel.AccelerationManager.ExcludedReflectionsProvider;
import com.dremio.exec.work.AttemptId;
import com.dremio.options.OptionManager;
import com.dremio.service.job.proto.ScanPath;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.reflection.handlers.ReflectionPlanGenerator;
import com.dremio.service.reflection.handlers.ReflectionPlanGenerator.PassThruNormalizer;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.MaterializationState;
import com.dremio.service.reflection.proto.ReflectionDetails;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.ReflectionType;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.proto.RefreshDecision;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import io.protostuff.ByteString;

/**
 * Sql syntax handler for the $MATERIALIZE command, an internal command used to materialize reflections.
 */
public class RefreshHandler implements SqlToPlanHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RefreshHandler.class);

  public static final String DECISION_NAME = RefreshDecision.class.getName();
  public static final Serializer<RefreshDecision> SERIALIZER = ProtostuffSerializer.of(RefreshDecision.getSchema());

  private String textPlan;

  public RefreshHandler() {
  }

  @Override
  public PhysicalPlan getPlan(SqlHandlerConfig config, String sql, SqlNode sqlNode) throws Exception {
    try{
      final SqlRefreshReflection materialize = SqlNodeUtil.unwrap(sqlNode, SqlRefreshReflection.class);

      if(!SystemUser.SYSTEM_USERNAME.equals(config.getContext().getQueryUserName())) {
        throw SqlExceptionHelper.parseError("$MATERIALIZE not supported.", sql, materialize.getParserPosition()).build(logger);
      }

      final AttemptObserver observer = config.getObserver();

      ReflectionService service = config.getContext().getAccelerationManager().unwrap(ReflectionService.class);

      // Let's validate the plan.

      Stopwatch watch = Stopwatch.createStarted();

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
      if(!Objects.equal(goal.getVersion(), entry.getGoalVersion())) {
        throw UserException.validationError().message("Reflection has been updated since reflection was scheduled.").build(logger);
      }

      Optional<Materialization> materializationOpt = service.getMaterialization(new MaterializationId(materialize.getMaterializationId()));
      if(!materializationOpt.isPresent()) {
        throw SqlExceptionHelper.parseError("Unknown materialization id.", sql, materialize.getReflectionIdPos()).build(logger);
      }
      final Materialization materialization = materializationOpt.get();
      if(!Objects.equal(goal.getVersion(), materialization.getReflectionGoalVersion())) {
        throw UserException.validationError().message("Reflection has been updated since reflection was scheduled.").build(logger);
      }

      if(materialization.getState() != MaterializationState.RUNNING) {
        throw UserException.validationError()
        .message("Materialization in unexpected state for Reflection {}, Materialization {}. State: {}", reflectionId.getId(), materialization.getId(), materialization.getState())
        .build(logger);
      }

      observer.planValidated(RecordWriter.SCHEMA.toCalciteRecordType(config.getConverter().getCluster().getTypeFactory()), materialize, watch.elapsed(TimeUnit.MILLISECONDS));

      watch.reset();

      final RefreshHelper helper = ((ReflectionServiceImpl) service).getRefreshHelper();
      final NamespaceService namespace = helper.getNamespace();
      final ReflectionSettings reflectionSettings = helper.getReflectionSettings();
      final MaterializationStore materializationStore = helper.getMaterializationStore();

      final RelNode initial = determineMaterializationPlan(
          config,
          goal,
          entry,
          materialization,
          service.getExcludedReflectionsProvider(),
          namespace,
          new ExtendedToRelContext(config.getConverter()),
          config.getContext().getConfig(),
          reflectionSettings,
          materializationStore);

      observer.planConvertedToRel(initial, watch.elapsed(TimeUnit.MILLISECONDS));

      final Rel drel = PrelTransformer.convertToDrel(config, initial);
      final Set<String> fields = ImmutableSet.copyOf(drel.getRowType().getFieldNames());

      // Append the attempt number to the table path
      final UserBitShared.QueryId queryId = config.getContext().getQueryId();
      final AttemptId attemptId = AttemptId.of(queryId);

      final List<String> tablePath = ImmutableList.of(
        ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME,
        reflectionId.getId(),
        materialization.getId().getId() + "_" + attemptId.getAttemptNum());

      final Rel writerDrel = new WriterRel(drel.getCluster(), drel.getCluster().traitSet().plus(Rel.LOGICAL),
          drel, config.getConverter().getCatalog().createNewTable(
              new NamespaceKey(tablePath),
              getWriterOptions(0, goal, fields), ImmutableMap.of()
              ), initial.getRowType());

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
      return plan;

    }catch(Exception ex){
      throw SqlExceptionHelper.coerceException(logger, sql, ex, true);
    }
  }

  private WriterOptions getWriterOptions(Integer ringCount, ReflectionGoal goal, Set<String> availableFields) {
    ReflectionDetails details = goal.getDetails();

    PartitionDistributionStrategy dist;
    switch(details.getPartitionDistributionStrategy()) {
    case STRIPED:
      dist = PartitionDistributionStrategy.STRIPED;
      break;
    case CONSOLIDATED:
    default:
      dist = PartitionDistributionStrategy.HASH;
    }

    return new WriterOptions(ringCount,
        toStrings(details.getPartitionFieldList(), availableFields),
        toStrings(details.getSortFieldList(), availableFields),
        toStrings(details.getDistributionFieldList(), availableFields),
        dist,
        false,
        Long.MAX_VALUE);
  }

  private static List<String> toStrings(List<ReflectionField> fields, Set<String> knownFields){
    if(fields == null || fields.isEmpty()) {
      return ImmutableList.of();
    }

    ImmutableList.Builder<String> fieldList = ImmutableList.builder();
    for(ReflectionField f : fields) {
      if(!knownFields.contains(f.getName())) {
        throw UserException.validationError().message("Unable to find field %s.", f).build(logger);
      }

      fieldList.add(f.getName());
    }
    return fieldList.build();
  }

  private RelNode determineMaterializationPlan(
      final SqlHandlerConfig sqlHandlerConfig,
      ReflectionGoal goal,
      ReflectionEntry entry,
      Materialization materialization,
      ExcludedReflectionsProvider exclusionsProvider,
      NamespaceService namespace,
      ExtendedToRelContext context,
      SabotConfig config,
      ReflectionSettings reflectionSettings,
      MaterializationStore materializationStore) {
    final ReflectionPlanGenerator planGenerator = new ReflectionPlanGenerator(sqlHandlerConfig, namespace, context.getPlannerSettings().getOptions(), config);

    PlanNormalizer planNormalizer = new PlanNormalizer(
      sqlHandlerConfig,
      goal,
      entry,
      materialization,
      namespace,
      config,
      reflectionSettings,
      materializationStore
    );

    final RelNode normalizedPlan = planGenerator.generateNormalizedPlan(goal, planNormalizer);

    // avoid accelerating this CTAS with the materialization itself
    // we set exclusions before we get to the logical phase (since toRel() is triggered in SqlToRelConverter, prior to planning).
    final List<String> exclusions = ImmutableList.<String>builder()
      .addAll(exclusionsProvider.getExcludedReflections(goal.getId().getId()))
      .add(goal.getId().getId())
      .build();
    context.getSession().getSubstitutionSettings().setExclusions(exclusions);

    RefreshDecision decision = planNormalizer.refreshDecision;
    decision.setLogicalPlan(ByteString.copyFrom(KryoLogicalPlanSerializers.serialize(planNormalizer.serializablePlan)));

    // save the decision for later.
    context.recordExtraInfo(DECISION_NAME, SERIALIZER.serialize(decision));
    logger.trace("Refresh decision: {}", decision);
    if(logger.isTraceEnabled()) {
      logger.trace(RelOptUtil.toString(normalizedPlan));
    }

    return normalizedPlan;
  }

  private class PlanNormalizer implements RelTransformer {
    private final SqlHandlerConfig sqlHandlerConfig;
    private final ReflectionGoal goal;
    private final ReflectionEntry entry;
    private final Materialization materialization;
    private final NamespaceService namespace;
    private final SabotConfig config;
    private final ReflectionSettings reflectionSettings;
    private final MaterializationStore materializationStore;
    private final OptionManager optionManager;

    private RefreshDecision refreshDecision;
    private RelNode serializablePlan;

    public PlanNormalizer(SqlHandlerConfig sqlHandlerConfig,
                          ReflectionGoal goal,
                          ReflectionEntry entry,
                          Materialization materialization,
                          NamespaceService namespace,
                          SabotConfig config,
                          ReflectionSettings reflectionSettings,
                          MaterializationStore materializationStore) {
      this.sqlHandlerConfig = sqlHandlerConfig;
      this.goal = goal;
      this.entry = entry;
      this.materialization = materialization;
      this.namespace = namespace;
      this.config = config;
      this.reflectionSettings = reflectionSettings;
      this.materializationStore = materializationStore;
      this.optionManager = sqlHandlerConfig.getContext().getOptions();
    }

    @Override
    public RelNode transform(RelNode relNode) {
      final RelNode datasetPlan = removeUpdateColumn(relNode);
      final DatasetConfig dataset = namespace.findDatasetByUUID(goal.getDatasetId());
      if (dataset == null) {
        throw new IllegalStateException(String.format("reflection %s has no corresponding dataset", ReflectionUtils.getId(goal)));
      }
      final ReflectionExpander expander = new ReflectionExpander(datasetPlan, dataset);
      final RelNode plan = expander.expand(goal);
      final Normalizer normalizer = getNormalizerInstance(goal.getType());
      RelNode normalizedPlan = normalizer.normalize(plan);

      this.serializablePlan = normalizedPlan;

      Iterable<DremioTable> requestedTables = sqlHandlerConfig.getContext().getCatalog().getAllRequestedTables();

      refreshDecision = getRefreshDecision(
        entry,
        materialization,
        reflectionSettings,
        namespace,
        materializationStore,
        normalizedPlan,
        requestedTables);

      if (useShuttle(refreshDecision)) {
        return normalizedPlan.accept(getShuttle(refreshDecision));
      }

      return normalizedPlan;
    }

    private Normalizer getNormalizerInstance(ReflectionType reflectionType) {
      final boolean removeProject = optionManager.getOption(ExecConstants.ACCELERATION_RAW_REMOVE_PROJECT);
      // DX-6734
      if (reflectionType == AGGREGATION || removeProject) {
        return config.getInstance(DREMIO_ACCELERATOR_PLANNING_AGGREGATION_NORMALIZER, Normalizer.class, PassThruNormalizer.class);
      } else {
        return config.getInstance(DREMIO_ACCELERATOR_PLANNING_RAW_VIEW_NORMALIZER, Normalizer.class, PassThruNormalizer.class);
      }
    }

  }


  public static final String DREMIO_ACCELERATOR_PLANNING_AGGREGATION_NORMALIZER = "dremio.accelerator.planning.aggregation-normalizer.class";
  public static final String DREMIO_ACCELERATOR_PLANNING_RAW_VIEW_NORMALIZER = "dremio.accelerator.planning.raw-view-normalizer.class";

  /**
   * Determine whether the provided materialization will be a partial or a full along with associated updateId, seriesId, etc.
   * @return The refresh decisions made
   */
  private RefreshDecision getRefreshDecision(ReflectionEntry entry, Materialization materialization,
      ReflectionSettings reflectionSettings, NamespaceService namespace, MaterializationStore materializationStore,
      RelNode normalizedPlan, Iterable<DremioTable> requestedTables) {

    final long newSeriesId = System.currentTimeMillis();

    final RefreshDecision decision = new RefreshDecision();

    // We load settings here to determine what type of update we need to do (full or incremental)
    final AccelerationSettings settings = IncrementalUpdateUtils.extractRefreshSettings(normalizedPlan, reflectionSettings);

    decision.setAccelerationSettings(settings);

    if (requestedTables != null && !Iterables.isEmpty(requestedTables)) {
      // store all physical dataset paths in the refresh decision
      final List<ScanPath> scanPathsList = FluentIterable.from(requestedTables)
        .filter(new Predicate<DremioTable>() {
          @Override
          public boolean apply(DremioTable table) {
            final DatasetConfig dataset = table.getDatasetConfig();
            return dataset != null && ReflectionUtils.isPhysicalDataset(dataset.getType());
          }
        }).transform(new Function<DremioTable, ScanPath>() {
          @Override
          public ScanPath apply(DremioTable table) {
            final List<String> datasetPath = table.getPath().getPathComponents();
            return new ScanPath().setPathList(datasetPath);
          }
        }).toList();
      decision.setScanPathsList(scanPathsList);
    }

    if(settings.getMethod() == RefreshMethod.FULL) {
      logger.trace("Incremental either not set or not supported for this query.");
      return decision.setInitialRefresh(true)
          .setSeriesId(newSeriesId);
    }

    try {
      final DatasetConfig dataset = namespace.findDatasetByUUID(entry.getDatasetId());
      decision.setDatasetHash(ReflectionUtils.computeDatasetHash(dataset, namespace));
    } catch (Exception e) {
      throw UserException.validationError()
        .message("Couldn't expand a materialized view on a non existing dataset")
        .addContext("reflectionId", entry.getId().getId())
        .addContext("datasetId", entry.getDatasetId())
        .build(logger);
    }

    // This is an incremental update dataset.
    // if we already have valid refreshes, we should use the their seriesId
    final Refresh refresh = materializationStore.getMostRecentRefresh(materialization.getReflectionId());

    // if this the first refresh of this materialization, let's do a initial refresh.
    if(refresh == null) {
      logger.trace("No existing refresh, doing an initial refresh.");
      return decision.setInitialRefresh(true)
          .setUpdateId(Long.MIN_VALUE)
          .setSeriesId(newSeriesId);
    }

    // if the refresh settings changed, do an initial refresh.
    if (entry.getRefreshMethod() != settings.getMethod() || !Objects.equal(entry.getRefreshField(), settings.getRefreshField())) {
      logger.trace("Change in refresh method, doing an initial refresh.");
      return decision.setInitialRefresh(true)
          .setUpdateId(Long.MIN_VALUE)
          .setSeriesId(newSeriesId);
    }

    if (!Objects.equal(entry.getDatasetHash(), decision.getDatasetHash())) {
      logger.trace("Change in dataset hash, doing an initial refresh.");
      return decision.setInitialRefresh(true)
          .setUpdateId(Long.MIN_VALUE)
          .setSeriesId(newSeriesId);
    }

    return decision.setInitialRefresh(false)
        .setUpdateId(refresh.getUpdateId())
        .setSeriesId(refresh.getSeriesId())
        .setSeriesOrdinal(refresh.getSeriesOrdinal() + 1);
  }

  private static boolean useShuttle(RefreshDecision decision) {
    return decision.getAccelerationSettings().getMethod() == RefreshMethod.INCREMENTAL;
  }

  private static RelShuttle getShuttle(RefreshDecision decision) {
    Preconditions.checkArgument(useShuttle(decision));
    return getShuttle(decision.getAccelerationSettings(), decision.getUpdateId());
  }

  public static RelShuttle getShuttle(AccelerationSettings settings, long updateId) {
    if (settings.getRefreshField() != null) {
      return new ColumnMaterializationShuttle(settings.getRefreshField(), updateId);
    } else {
      return new FileMaterializationShuttle(updateId);
    }
  }

  @Override
  public String getTextPlan() {
    return textPlan;
  }

}
