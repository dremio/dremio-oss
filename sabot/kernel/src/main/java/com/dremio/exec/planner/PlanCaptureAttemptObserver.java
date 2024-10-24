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
package com.dremio.exec.planner;

import com.dremio.common.exceptions.UserCancellationException;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.ops.ViewExpansionContext;
import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.acceleration.RelWithInfo;
import com.dremio.exec.planner.acceleration.descriptor.MaterializationDescriptor;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionInfo;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionInfo.Substitution;
import com.dremio.exec.planner.common.PlannerMetrics;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.observer.AbstractAttemptObserver;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.visitor.QueryProfileProcessor;
import com.dremio.exec.planner.plancache.CachedPlan;
import com.dremio.exec.planner.serialization.RelSerializerFactory;
import com.dremio.exec.planner.sql.DremioCompositeSqlOperatorTable;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.AccelerationProfile;
import com.dremio.exec.proto.UserBitShared.DatasetProfile.Builder;
import com.dremio.exec.proto.UserBitShared.FragmentRpcSizeStats;
import com.dremio.exec.proto.UserBitShared.LayoutMaterializedViewProfile;
import com.dremio.exec.proto.UserBitShared.PlanPhaseProfile;
import com.dremio.exec.proto.UserBitShared.PlannerPhaseRulesStats;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.work.foreman.ExecutionPlan;
import com.dremio.reflection.hints.ReflectionExplanationsAndQueryDistance;
import com.dremio.resource.GroupResourceInformation;
import com.dremio.resource.ResourceSchedulingDecisionInfo;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;

public class PlanCaptureAttemptObserver extends AbstractAttemptObserver {
  public static final String PLAN_CACHE_USED = "Plan Cache Used";

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(PlanCaptureAttemptObserver.class);

  private final boolean verbose;
  private final boolean includeDatasetProfiles;
  private final FunctionImplementationRegistry funcRegistry;
  private final List<PlanPhaseProfile> planPhases = Lists.newArrayList();
  private final Map<String, LayoutMaterializedViewProfile> mapIdToAccelerationProfile =
      new LinkedHashMap<>();
  private final AccelerationDetailsPopulator detailsPopulator;
  private final ImmutableList.Builder<UserBitShared.DatasetProfile> datasetProfileBuilder =
      ImmutableList.builder();
  private final RelSerializerFactory relSerializerFactory;

  private Map<String, UserBitShared.RelNodeInfo> finalPrelInfo = new HashMap<>();

  private String text;
  private String json;
  private BatchSchema schema;
  private String schemaJSON;

  private boolean accelerated = false;
  private long findMaterializationMillis = 0;
  private long normalizationMillis = 0;
  private long substitutionMillis = 0;
  private int numSubstitutions = 0;
  private int numFragments;
  private List<String> normalizedQueryPlans;

  private volatile ByteString accelerationDetails;
  private byte[] serializedPlan;

  private int numPlanCacheUses = 0;

  private Integer numJoinsInUserQuery = null;

  private Integer numJoinsInFinalPrel = null;

  public PlanCaptureAttemptObserver(
      final boolean verbose,
      final boolean includeDatasetProfiles,
      final FunctionImplementationRegistry funcRegistry,
      AccelerationDetailsPopulator detailsPopulator,
      RelSerializerFactory relSerializerFactory) {
    this.verbose = verbose;
    this.includeDatasetProfiles = includeDatasetProfiles;
    this.funcRegistry = funcRegistry;
    this.detailsPopulator = detailsPopulator;
    this.relSerializerFactory = relSerializerFactory;
  }

  public AccelerationProfile buildAccelerationProfile(boolean truncate) {
    AccelerationProfile.Builder builder =
        AccelerationProfile.newBuilder()
            .setAccelerated(accelerated)
            .setNumSubstitutions(numSubstitutions)
            .setMillisTakenGettingMaterializations(findMaterializationMillis)
            .setMillisTakenNormalizing(normalizationMillis)
            .setMillisTakenSubstituting(substitutionMillis);
    if (normalizedQueryPlans != null && !truncate) {
      builder.addAllNormalizedQueryPlans(normalizedQueryPlans);
    }
    if (!mapIdToAccelerationProfile.isEmpty()) {
      Collection<LayoutMaterializedViewProfile> layoutProfiles =
          mapIdToAccelerationProfile.values().stream()
              .map(
                  lmvProfile ->
                      truncate
                          ? LayoutMaterializedViewProfile.newBuilder(lmvProfile)
                              .clearSubstitutions()
                              .clearNormalizedPlans()
                              .clearNormalizedQueryPlans()
                              .clearOptimizedPlan()
                              .clearPlan()
                              .build()
                          : lmvProfile)
              .collect(Collectors.toCollection(LinkedList::new));
      builder.addAllLayoutProfiles(layoutProfiles);
    }
    if (accelerationDetails != null) {
      builder.setAccelerationDetails(accelerationDetails);
    }
    return builder.build();
  }

  @Override
  public void addAccelerationProfileToCachedPlan(CachedPlan cachedPlan) {
    cachedPlan.setAccelerationProfile(buildAccelerationProfile(true));
  }

  @Override
  public void restoreAccelerationProfileFromCachedPlan(AccelerationProfile accelerationProfile) {
    accelerated = accelerationProfile.getAccelerated();
    numSubstitutions = accelerationProfile.getNumSubstitutions();
    findMaterializationMillis = accelerationProfile.getMillisTakenGettingMaterializations();
    normalizationMillis = accelerationProfile.getMillisTakenNormalizing();
    substitutionMillis = accelerationProfile.getMillisTakenSubstituting();
    for (int i = 0; i < accelerationProfile.getLayoutProfilesCount(); i++) {
      LayoutMaterializedViewProfile profile = accelerationProfile.getLayoutProfiles(i);
      mapIdToAccelerationProfile.put(profile.getLayoutId(), profile);
    }
    accelerationDetails = accelerationProfile.getAccelerationDetails();
    detailsPopulator.restoreAccelerationProfile(accelerationProfile);
  }

  public Iterable<UserBitShared.DatasetProfile> getDatasets() {
    return datasetProfileBuilder.build();
  }

  public List<PlanPhaseProfile> getPlanPhases() {
    return planPhases;
  }

  public byte[] getSerializedPlan() {
    return serializedPlan;
  }

  @Override
  public void planText(String text, long millisTaken) {
    this.text = text;
    planPhases.add(
        PlanPhaseProfile.newBuilder()
            .setPhaseName(PlannerPhase.PLAN_FINAL_PHYSICAL)
            .setDurationMillis(millisTaken)
            .setPlan(text)
            .build());
  }

  @Override
  public void planJsonPlan(String text) {
    this.json = text;
  }

  @Override
  public void finalPrelPlanGenerated(Prel plan) {
    if (plan != null) {
      PlannerSettings settings = PrelUtil.getSettings(plan.getCluster());
      if (settings.isPrettyPlanScrapingEnabled()) {
        try {
          finalPrelInfo = QueryProfileProcessor.process(plan);
        } catch (Exception e) {
          logger.warn("Failed to serialize the pretty Plan info", e);
        }
      }
    }
    accelerationDetails = ByteString.copyFrom(detailsPopulator.computeAcceleration());
  }

  public Map<String, UserBitShared.RelNodeInfo> getFinalPrelInfo() {
    return finalPrelInfo;
  }

  @Override
  public void planAccelerated(final SubstitutionInfo info) {
    accelerated = true;
    Set<String> targetTypes = new TreeSet<>();

    for (Substitution sub : info.getSubstitutions()) {
      final MaterializationDescriptor descriptor = sub.getMaterialization();
      final String key = descriptor.getLayoutId();

      final LayoutMaterializedViewProfile lmvProfile = mapIdToAccelerationProfile.get(key);
      if (lmvProfile != null) {
        LayoutMaterializedViewProfile profile =
            LayoutMaterializedViewProfile.newBuilder(lmvProfile)
                .setNumUsed(lmvProfile.getNumUsed() + 1)
                .build();
        // All materialization targets in the same reflection have the same target type so we only
        // need to record the first one.
        if (profile.getNormalizedPlansCount() > 0 && lmvProfile.getNumUsed() == 0) {
          final int index = profile.getNormalizedPlans(0).indexOf("_target");
          if (index >= 0 && index < ViewExpansionContext.DEFAULT_RAW_TARGET.length()) {
            final String targetType = profile.getNormalizedPlans(0).substring(0, index);
            targetTypes.add(targetType);
          }
        }
        mapIdToAccelerationProfile.put(key, profile);
      }
    }
    if (targetTypes.isEmpty()) {
      targetTypes.add("unknown_matching");
    }
    // FlightSQL replays the profile for the execute job so don't double count on the execute job
    if (this.numJoinsInUserQuery != null) {
      PlannerMetrics.getAcceleratedQueriesCounter()
          .withTag(PlannerMetrics.TAG_TARGET, targetTypes.toString())
          .increment();
    }
    detailsPopulator.planAccelerated(info);
  }

  @Override
  public void planFindMaterializations(long millisTaken) {
    findMaterializationMillis = millisTaken;
    planPhases.add(
        PlanPhaseProfile.newBuilder()
            .setPhaseName(PlannerPhase.PLAN_FIND_MATERIALIZATIONS)
            .setDurationMillis(millisTaken)
            .setPlan("")
            .build());
  }

  @Override
  public void planNormalized(long millisTaken, List<RelWithInfo> normalizedQueryPlans) {
    normalizationMillis = millisTaken;
    planPhases.add(
        PlanPhaseProfile.newBuilder()
            .setPhaseName(PlannerPhase.PLAN_NORMALIZED)
            .setDurationMillis(normalizationMillis)
            .setPlan("")
            .build());
    if (verbose) {
      this.normalizedQueryPlans = new ArrayList<>();
      normalizedQueryPlans.stream()
          .forEach(
              plan -> {
                this.normalizedQueryPlans.add(toProfilePlan(plan));
              });
    }
  }

  @Override
  public void planSubstituted(long millisTaken) {
    substitutionMillis = millisTaken;
    planPhases.add(
        PlanPhaseProfile.newBuilder()
            .setPhaseName(PlannerPhase.PLAN_MATCH_MATERIALIZATIONS)
            .setDurationMillis(substitutionMillis)
            .setPlan("")
            .build());
  }

  private String toProfilePlan(RelWithInfo relWithInfo) {
    return String.format(
        "%s (%d ms)\n%s",
        relWithInfo.getInfo(),
        relWithInfo.getElapsed().toMillis(),
        toStringOrEmpty(relWithInfo.getRel(), false));
  }

  /**
   * planSubstituted is called for both considered and matched target materializations. This method
   * can be called multiple times for the same reflection as it can have different target
   * materializations and substitutions. When called for considered target materializations,
   * substitutions should be empty as we haven't done matching yet. When called for matched target
   * materializations, substitutions will never be empty. Furthermore, we don't need to worry about
   * capturing the normalized target plans (i.e. canonicalized reflection plan) because they have
   * already been saved earlier when tracking the considered target materializations.
   *
   * @param materialization
   * @param substitutions number of plans returned after substitution finished
   * @param target
   * @param millisTaken
   * @param defaultReflection
   */
  @Override
  public void planSubstituted(
      DremioMaterialization materialization,
      List<RelWithInfo> substitutions,
      RelWithInfo target,
      long millisTaken,
      boolean defaultReflection) {
    final String key = materialization.getReflectionId();
    final LayoutMaterializedViewProfile oldProfile = mapIdToAccelerationProfile.get(key);

    final LayoutMaterializedViewProfile.Builder layoutBuilder;
    if (oldProfile == null) {
      layoutBuilder =
          LayoutMaterializedViewProfile.newBuilder()
              .setLayoutId(materialization.getReflectionId())
              .setName(materialization.getLayoutInfo().getName())
              .setType(materialization.getLayoutInfo().getType())
              .setMaterializationId(materialization.getMaterializationId())
              .setMaterializationExpirationTimestamp(materialization.getExpirationTimestamp())
              .addAllDimensions(materialization.getLayoutInfo().getDimensions())
              .addAllMeasureColumns(materialization.getLayoutInfo().getMeasures())
              .addAllSortedColumns(materialization.getLayoutInfo().getSortColumns())
              .addAllPartitionedColumns(materialization.getLayoutInfo().getPartitionColumns())
              .addAllDistributionColumns(materialization.getLayoutInfo().getDistributionColumns())
              .addAllDisplayColumns(materialization.getLayoutInfo().getDisplayColumns())
              .setNumSubstitutions(substitutions.size())
              .setMillisTakenSubstituting(millisTaken)
              .setPlan(toStringOrEmpty(materialization.getQueryRel(), false))
              .addNormalizedPlans(toProfilePlan(target))
              .setSnowflake(materialization.isSnowflake())
              .setDefaultReflection(defaultReflection);
    } else {
      layoutBuilder =
          LayoutMaterializedViewProfile.newBuilder(oldProfile)
              .setNumSubstitutions(oldProfile.getNumSubstitutions() + substitutions.size())
              .setMillisTakenSubstituting(oldProfile.getMillisTakenSubstituting() + millisTaken);
      if (substitutions.isEmpty()) {
        layoutBuilder.addNormalizedPlans(toProfilePlan(target));
      }
    }
    if (verbose) {
      layoutBuilder.addAllSubstitutions(
          substitutions.stream()
              .map(
                  substitution -> {
                    return UserBitShared.SubstitutionProfile.newBuilder()
                        .setPlan(toProfilePlan(substitution))
                        .build();
                  })
              .collect(Collectors.toList()));
    }
    LayoutMaterializedViewProfile profile = layoutBuilder.build();
    mapIdToAccelerationProfile.put(key, profile);
    numSubstitutions += substitutions.size();

    detailsPopulator.planSubstituted(
        materialization, substitutions, target.getRel(), millisTaken, defaultReflection);
  }

  @Override
  public void substitutionFailures(Iterable<String> errors) {
    detailsPopulator.substitutionFailures(errors);
  }

  @Override
  public void planCompleted(ExecutionPlan plan, BatchSchema batchSchema) {
    if (plan != null) {
      try {
        schema = RootSchemaFinder.getSchema(plan.getRootOperator());
        numFragments = plan.getFragments().size();
      } catch (Exception e) {
        logger.warn("Failed to capture query output schema", e);
      }
    } else if (batchSchema != null) {
      schema = batchSchema;
    }
  }

  @Override
  public void resourcesPlanned(GroupResourceInformation resourceInformation, long millisTaken) {
    StringBuilder builder = new StringBuilder();
    builder.append("\ngroupResourceInformation {\n");
    resourceInformation.appendProfileLogging(builder, this.funcRegistry.getOptionManager());
    builder.append("\n}");
    planPhases.add(
        PlanPhaseProfile.newBuilder()
            .setPhaseName(PlannerPhase.PLAN_RESOURCES_PLANNED)
            .setDurationMillis(millisTaken)
            .setPlan(builder.toString())
            .build());
  }

  @Override
  public void resourcesScheduled(ResourceSchedulingDecisionInfo info) {
    if (info.getSchedulingEndTimeMs() == 0) {
      return;
    }
    planPhases.add(
        PlanPhaseProfile.newBuilder()
            .setPhaseName(PlannerPhase.PLAN_RESOURCES_ALLOCATED)
            .setDurationMillis(info.getSchedulingEndTimeMs() - info.getSchedulingStartTimeMs())
            .setPlan("")
            .build());
  }

  @Override
  public void planValidated(
      RelDataType rowType,
      SqlNode node,
      long millisTaken,
      boolean isMaterializationCacheInitialized) {
    if (!isMaterializationCacheInitialized) {
      planPhases.add(
          PlanPhaseProfile.newBuilder()
              .setPhaseName(
                  String.format(
                      "Materialization cache not available.  Longer planning times may occur in %s and %s.",
                      PlannerPhase.PLAN_VALIDATED, PlannerPhase.PLAN_FIND_MATERIALIZATIONS))
              .build());
    }

    planPhases.add(
        PlanPhaseProfile.newBuilder()
            .setPhaseName(PlannerPhase.PLAN_VALIDATED)
            .setDurationMillis(millisTaken)
            .setPlan("")
            .build());
  }

  @Override
  public void planCacheUsed(int count) {
    planPhases.add(
        PlanPhaseProfile.newBuilder()
            .setPhaseName(PLAN_CACHE_USED)
            .setPlan(
                String.format(
                    "Cached Plan is used for the query, the cached plan entry has been used %d times",
                    count))
            .build());
    numPlanCacheUses = count;
  }

  public int getNumPlanCacheUses() {
    return numPlanCacheUses;
  }

  public Integer getNumJoinsInUserQuery() {
    return numJoinsInUserQuery;
  }

  public Integer getNumJoinsInFinalPrel() {
    return numJoinsInFinalPrel;
  }

  // Serializes and stores plans
  private void serializeAndStoreRel(RelNode converted) throws Exception {
    PlannerSettings settings = PrelUtil.getSettings(converted.getCluster());
    if (!settings.isPlanSerializationEnabled()) {
      return;
    }
    RelNode toSerialize = converted.accept(new RelShuttleImpl());
    String planString = RelOptUtil.toString(toSerialize);
    if (planString != null && planString.length() < settings.getSerializationLengthLimit()) {
      this.serializedPlan =
          relSerializerFactory
              .getSerializer(
                  converted.getCluster(), DremioCompositeSqlOperatorTable.create(funcRegistry))
              .serializeToBytes(toSerialize);
    } else {
      logger.debug("Plan Serialization skipped due to size");
    }
  }

  @Override
  public void planConvertedToRel(RelNode converted, long millisTaken) {
    final String convertedRelTree = toStringOrEmpty(converted, true);
    try {
      final Stopwatch stopwatch = Stopwatch.createStarted();
      serializeAndStoreRel(converted);
      millisTaken += stopwatch.elapsed(TimeUnit.MILLISECONDS);
    } catch (Throwable e) {
      logger.debug("Error", e);
    }
    planPhases.add(
        PlanPhaseProfile.newBuilder()
            .setPhaseName(PlannerPhase.PLAN_CONVERTED_TO_REL)
            .setDurationMillis(millisTaken)
            .setPlan(convertedRelTree)
            .build());

    detailsPopulator.planConvertedToRel(converted);
  }

  @Override
  public void planConvertedScan(RelNode converted, long millisTaken) {
    final String convertedRelTree = toStringOrEmpty(converted, false);
    planPhases.add(
        PlanPhaseProfile.newBuilder()
            .setPhaseName(PlannerPhase.PLAN_CONVERTED_SCAN)
            .setDurationMillis(millisTaken)
            .setPlan(convertedRelTree)
            .build());
  }

  /**
   * Gets the refresh decision and how long it took to make the refresh decision
   *
   * @param text A string describing if we decided to do full or incremental refresh
   * @param millisTaken time taken in planning the refresh decision
   */
  @Override
  public void planRefreshDecision(String text, long millisTaken) {

    planPhases.add(
        PlanPhaseProfile.newBuilder()
            .setPhaseName(PlannerPhase.PLAN_REFRESH_DECISION)
            .setDurationMillis(millisTaken)
            .setPlan(text)
            .build());
  }

  @Override
  public void planRelTransform(
      final PlannerPhase phase,
      RelOptPlanner planner,
      final RelNode before,
      final RelNode after,
      final long millisTaken,
      final List<PlannerPhaseRulesStats> rulesBreakdownStats) {
    final boolean noTransform = before == after;
    final String planAsString = toStringOrEmpty(after, noTransform || phase.forceVerbose());
    final long millisTakenFinalize =
        (phase.useMaterializations)
            ? millisTaken - (findMaterializationMillis + normalizationMillis + substitutionMillis)
            : millisTaken;

    PlanPhaseProfile.Builder b =
        PlanPhaseProfile.newBuilder()
            .setPhaseName(phase.description)
            .setDurationMillis(millisTakenFinalize)
            .addAllRulesBreakdownStats(rulesBreakdownStats)
            .setPlan(planAsString);

    // dump state of volcano planner to troubleshoot costing issues (or long planning issues).
    // only add it if the plan string is empty, or it's the logical/physical planning phase, because
    // it would otherwise be redundant info.
    if ((verbose || noTransform)
        && (planAsString.isEmpty()
            || phase == PlannerPhase.LOGICAL
            || phase == PlannerPhase.PHYSICAL)) {
      final String dump = getPlanDump(planner);
      if (dump != null) {
        b.setPlannerDump(dump);
      }
    }

    planPhases.add(b.build());

    if (verbose
        && phase.useMaterializations
        && planner instanceof VolcanoPlanner
        && numSubstitutions > 0) {
      try {
        Map<String, CheapestPlanWithReflectionVisitor.RelCostPair> bestPlansWithReflections =
            new CheapestPlanWithReflectionVisitor((VolcanoPlanner) planner, ImmutableSet.of())
                .getBestPlansWithReflections();
        for (String reflection : bestPlansWithReflections.keySet()) {
          String plan =
              RelOptUtil.toString(
                  bestPlansWithReflections.get(reflection).getRel(),
                  SqlExplainLevel.ALL_ATTRIBUTES);
          LayoutMaterializedViewProfile profile = mapIdToAccelerationProfile.get(reflection);
          if (profile != null) {
            mapIdToAccelerationProfile.put(
                reflection,
                LayoutMaterializedViewProfile.newBuilder(profile)
                    .setOptimizedPlanBytes(ByteString.copyFrom(plan.getBytes()))
                    .build());
          }
        }
      } catch (Exception e) {
        logger.debug("Failed to find best plans with reflections", e);
      }
    }
  }

  private String getPlanDump(RelOptPlanner planner) {
    if (planner == null) {
      return null;
    }

    // Use VolcanoPlanner#dump to get more detailed information
    if (planner instanceof VolcanoPlanner) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      ((VolcanoPlanner) planner).dump(pw);
      pw.flush();
      return sw.toString();
    }

    // Print the current tree otherwise
    RelNode root = planner.getRoot();
    return RelOptUtil.toString(root);
  }

  @Override
  public void tablesCollected(Iterable<DremioTable> tables) {
    if (includeDatasetProfiles) {
      tables.forEach(
          t -> {
            final UserBitShared.DatasetProfile datasetProfile = buildDatasetProfile(t);
            if (datasetProfile != null) {
              datasetProfileBuilder.add(datasetProfile);
            }
          });
    }
  }

  private UserBitShared.DatasetProfile buildDatasetProfile(final DremioTable t) {
    try {
      if (t instanceof ViewTable) {
        final ViewTable view = (ViewTable) t;
        Builder builder =
            UserBitShared.DatasetProfile.newBuilder()
                .setDatasetPath(t.getPath().getSchemaPath())
                .setType(UserBitShared.DatasetType.VDS)
                .setSql(view.getView().getSql());
        if (view.getVersionContext() != null) {
          builder.setVersionContext(view.getVersionContext().toSql());
        }
        return builder.build();
      } else {
        Boolean allowApproxStats = false;

        PhysicalDataset physicalDataset = t.getDatasetConfig().getPhysicalDataset();
        if (physicalDataset != null) {
          allowApproxStats = physicalDataset.getAllowApproxStats();
        }
        Builder builder =
            UserBitShared.DatasetProfile.newBuilder()
                .setDatasetPath(t.getPath().getSchemaPath())
                .setType(UserBitShared.DatasetType.PDS)
                .setBatchSchema(ByteString.copyFrom(t.getSchema().serialize()))
                .setAllowApproxStats(allowApproxStats);
        if (t.getDataset().getVersionContext() != null) {
          builder.setVersionContext(t.getDataset().getVersionContext().toSql());
        }
        return builder.build();
      }
    } catch (Exception e) {
      logger.warn("Couldn't build dataset profile for table {}", t.getPath().getSchemaPath(), e);
    }

    return null;
  }

  @Override
  public void executorsSelected(
      long millisTaken,
      int idealNumFragments,
      int idealNumNodes,
      int numExecutors,
      String detailsText) {
    StringBuilder sb =
        new StringBuilder()
            .append("idealNumFragments: ")
            .append(idealNumFragments)
            .append("\n")
            .append("idealNumNodes    : ")
            .append(idealNumNodes)
            .append("\n")
            .append("numExecutors     : ")
            .append(numExecutors)
            .append("\n")
            .append("details          : ")
            .append(detailsText);
    planPhases.add(
        PlanPhaseProfile.newBuilder()
            .setPhaseName("Execution Plan: Executor Selection")
            .setDurationMillis(millisTaken)
            .setPlan(sb.toString())
            .build());
  }

  @Override
  public void planGenerationTime(long millisTaken) {
    planPhases.add(
        PlanPhaseProfile.newBuilder()
            .setPhaseName("Execution Plan: Plan Generation")
            .setDurationMillis(millisTaken)
            .setPlan("")
            .build());
  }

  @Override
  public void planAssignmentTime(long millisTaken) {
    planPhases.add(
        PlanPhaseProfile.newBuilder()
            .setPhaseName("Execution Plan: Fragment Assignment")
            .setDurationMillis(millisTaken)
            .setPlan("")
            .build());
  }

  @Override
  public void fragmentsStarted(long millisTaken, FragmentRpcSizeStats stats) {
    planPhases.add(
        PlanPhaseProfile.newBuilder()
            .setPhaseName("Fragment Start RPCs")
            .setDurationMillis(millisTaken)
            .setSizeStats(stats)
            .build());
  }

  @Override
  public void fragmentsActivated(long millisTaken) {
    planPhases.add(
        PlanPhaseProfile.newBuilder()
            .setPhaseName("Fragment Activate RPCs")
            .setDurationMillis(millisTaken)
            .build());
  }

  @Override
  public void updateReflectionsWithHints(
      ReflectionExplanationsAndQueryDistance reflectionExplanationsAndQueryDistance) {
    detailsPopulator.addReflectionHints(reflectionExplanationsAndQueryDistance);
  }

  @Override
  public void setNumJoinsInUserQuery(Integer joins) {
    this.numJoinsInUserQuery = joins;
  }

  @Override
  public void setNumJoinsInFinalPrel(Integer joins) {
    this.numJoinsInFinalPrel = joins;
  }

  public String getText() {
    return text;
  }

  public String getJson() {
    return json;
  }

  public String getFullSchema() {
    if (!verbose) {
      // Sometimes schema is too big especially when the schema has lot of nested columns. Enable
      // only in verbose mode.
      return null;
    }
    if (schema == null) {
      return "Query output schema is not set in plan observer";
    }

    if (schemaJSON != null) {
      return schemaJSON;
    }

    try {
      schemaJSON = schema.toJSONString();
      return schemaJSON;
    } catch (Exception e) {
      logger.warn("Failed to serialize query output schema to JSON", e);
      return "Failed to serialize query output schema to JSON: " + e.getMessage();
    }
  }

  public String toStringOrEmpty(final RelNode plan, boolean ensureDump) {
    if (!verbose && !ensureDump) {
      return "";
    }

    if (plan == null) {
      return "";
    }

    try {
      return RelOptUtil.dumpPlan(
          "",
          plan,
          SqlExplainFormat.TEXT,
          verbose ? SqlExplainLevel.ALL_ATTRIBUTES : SqlExplainLevel.EXPPLAN_ATTRIBUTES);
    } catch (UserCancellationException userCancellationException) {
      return "";
    }
  }

  public int getNumFragments() {
    return numFragments;
  }
}
