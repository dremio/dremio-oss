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


import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;

import com.dremio.common.exceptions.UserCancellationException;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.acceleration.MaterializationDescriptor;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionInfo;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionInfo.Substitution;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.observer.AbstractAttemptObserver;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.serialization.RelSerializerFactory;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.AccelerationProfile;
import com.dremio.exec.proto.UserBitShared.FragmentRpcSizeStats;
import com.dremio.exec.proto.UserBitShared.LayoutMaterializedViewProfile;
import com.dremio.exec.proto.UserBitShared.PlanPhaseProfile;
import com.dremio.exec.proto.UserBitShared.SubstitutionProfile;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.sys.accel.AccelerationDetailsPopulator;
import com.dremio.exec.work.foreman.ExecutionPlan;
import com.dremio.reflection.hints.ReflectionExplanationsAndQueryDistance;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

public class PlanCaptureAttemptObserver extends AbstractAttemptObserver {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PlanCaptureAttemptObserver.class);

  private final boolean verbose;
  private final boolean includeDatasetProfiles;
  private final FunctionImplementationRegistry funcRegistry;
  private final List<PlanPhaseProfile> planPhases = Lists.newArrayList();
  private final Map<String, LayoutMaterializedViewProfile>
    mapIdToAccelerationProfile = new LinkedHashMap<>();
  private final AccelerationDetailsPopulator detailsPopulator;
  private final ImmutableList.Builder<UserBitShared.DatasetProfile> datasetProfileBuilder = ImmutableList.builder();
  private final RelSerializerFactory relSerializerFactory;

  private Prel finalPrel;
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

  private final CachedAccelDetails accelDetails = new CachedAccelDetails();

  public PlanCaptureAttemptObserver(final boolean verbose, final boolean includeDatasetProfiles,
                                    final FunctionImplementationRegistry funcRegistry,
                                    AccelerationDetailsPopulator detailsPopulator,
                                    RelSerializerFactory relSerializerFactory) {
    this.verbose = verbose;
    this.includeDatasetProfiles = includeDatasetProfiles;
    this.funcRegistry = funcRegistry;
    this.detailsPopulator = detailsPopulator;
    this.relSerializerFactory = relSerializerFactory;
  }

  public AccelerationProfile getAccelerationProfile() {
    AccelerationProfile.Builder builder = AccelerationProfile.newBuilder()
      .setAccelerated(accelerated)
      .setNumSubstitutions(numSubstitutions)
      .setMillisTakenGettingMaterializations(findMaterializationMillis)
      .setMillisTakenNormalizing(normalizationMillis)
      .setMillisTakenSubstituting(substitutionMillis);
    if (normalizedQueryPlans != null) {
      builder.addAllNormalizedQueryPlans(normalizedQueryPlans);
    }
    if (!mapIdToAccelerationProfile.isEmpty()) {
      builder.addAllLayoutProfiles(mapIdToAccelerationProfile.values());
    }
    if (accelerationDetails != null) {
      builder.setAccelerationDetails(accelerationDetails);
    }
    return builder.build();
  }

  @Override
  public void setCachedAccelDetails(CachedPlan cachedPlan) {
    cachedPlan.setAccelDetails(accelDetails);
  }

  @Override
  public void applyAccelDetails(final CachedAccelDetails accelDetails) {
    accelerated = true;
    List<RelNode> dummy = new ArrayList<>();
    dummy.add(null);
    for (Map.Entry<DremioMaterialization, RelNode> entry : accelDetails.getMaterializationStore().entrySet()) {
      DremioMaterialization materialization = entry.getKey();
      String key = materialization.getReflectionId();
      LayoutMaterializedViewProfile profile = accelDetails.getLmvProfile(key);
      mapIdToAccelerationProfile.put(key, profile);
      detailsPopulator.planSubstituted(
        materialization, dummy,
        entry.getValue(), 0, profile.getDefaultReflection());
    }
    detailsPopulator.planAccelerated(accelDetails.getSubstitutionInfo());
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
    planPhases.add(PlanPhaseProfile.newBuilder()
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
  public void finalPrel(Prel plan) {
    this.finalPrel = plan;
  }

  public Prel getFinalPrel(){
    return finalPrel;
  }

  @Override
  public void planAccelerated(final SubstitutionInfo info) {
    accelerated = true;
    accelDetails.setSubstitutionInfo(info);
    for (Substitution sub : info.getSubstitutions()) {
      final MaterializationDescriptor descriptor = sub.getMaterialization();
      final String key = descriptor.getLayoutId();

      final LayoutMaterializedViewProfile lmvProfile = mapIdToAccelerationProfile.get(key);
      if (lmvProfile != null) {
        LayoutMaterializedViewProfile profile = LayoutMaterializedViewProfile.newBuilder(lmvProfile).setNumUsed(lmvProfile.getNumUsed() + 1).build();
        accelDetails.addLmvProfile(key, profile);
        mapIdToAccelerationProfile.put(key, profile);
      }
    }
    detailsPopulator.planAccelerated(info);
  }

  @Override
  public void planFindMaterializations(long millisTaken) {
    findMaterializationMillis = millisTaken;
    planPhases.add(PlanPhaseProfile.newBuilder()
      .setPhaseName(PlannerPhase.PLAN_FIND_MATERIALIZATIONS)
      .setDurationMillis(millisTaken)
      .setPlan("")
      .build());
  }

  @Override
  public void planNormalized(long millisTaken, List<RelNode> normalizedQueryPlans) {
    normalizationMillis = millisTaken;
    planPhases.add(PlanPhaseProfile.newBuilder()
      .setPhaseName(PlannerPhase.PLAN_NORMALIZED)
      .setDurationMillis(normalizationMillis)
      .setPlan("")
      .build());
    if (verbose) {
      this.normalizedQueryPlans = Lists.transform(normalizedQueryPlans, new Function<RelNode, String>() {
        @Nullable
        @Override
        public String apply(@Nullable RelNode plan) {
          return toStringOrEmpty(plan, false);
        }
      });
    }
  }

  @Override
  public void planSubstituted(DremioMaterialization materialization,
                              List<RelNode> substitutions,
                              RelNode target, long millisTaken, boolean defaultReflection) {
    final String key = materialization.getReflectionId();
    final LayoutMaterializedViewProfile oldProfile = mapIdToAccelerationProfile.get(key);

    final LayoutMaterializedViewProfile.Builder layoutBuilder;
    if (oldProfile == null) {
      layoutBuilder = LayoutMaterializedViewProfile.newBuilder()
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
        .addNormalizedPlans(toStringOrEmpty(target, false))
        .setSnowflake(materialization.isSnowflake())
        .setDefaultReflection(defaultReflection);
    } else {
      layoutBuilder = LayoutMaterializedViewProfile.newBuilder(oldProfile)
        .setNumSubstitutions(oldProfile.getNumSubstitutions() + substitutions.size())
        .setMillisTakenSubstituting(oldProfile.getMillisTakenSubstituting() + millisTaken);
    }
    if (verbose) {
      layoutBuilder
        .addAllSubstitutions(FluentIterable.from(substitutions).transform(new Function<RelNode, SubstitutionProfile>() {
          @Nullable
          @Override
          public SubstitutionProfile apply(@Nullable RelNode input) {
            return SubstitutionProfile.newBuilder().setPlan(toStringOrEmpty(input, false)).build();
          }
        }));
    }
    LayoutMaterializedViewProfile profile = layoutBuilder.build();
    mapIdToAccelerationProfile.put(key, profile);
    accelDetails.addLmvProfile(key, profile);
    accelDetails.addMaterialization(materialization, target);
    substitutionMillis += millisTaken;
    numSubstitutions += substitutions.size();

    detailsPopulator.planSubstituted(materialization, substitutions, target, millisTaken, defaultReflection);
  }

  @Override
  public void substitutionFailures(Iterable<String> errors) {
    detailsPopulator.substitutionFailures(errors);
  }

  @Override
  public void planCompleted(ExecutionPlan plan) {
    if (plan != null) {
      try {
        schema = RootSchemaFinder.getSchema(plan.getRootOperator());
        numFragments = plan.getFragments().size();
      } catch (Exception e) {
        logger.warn("Failed to capture query output schema", e);
      }
    }

    accelerationDetails = ByteString.copyFrom(detailsPopulator.computeAcceleration());
  }

  @Override
  public void planValidated(RelDataType rowType, SqlNode node, long millisTaken) {
    planPhases.add(PlanPhaseProfile.newBuilder()
      .setPhaseName(PlannerPhase.PLAN_VALIDATED)
      .setDurationMillis(millisTaken)
      .setPlan("")
      .build());
  }

  @Override
  public void planCacheUsed( int count) {
    planPhases.add(PlanPhaseProfile.newBuilder()
      .setPhaseName(PlannerPhase.PLAN_CACHE_USED)
      .setPlan(String.format("Cached Plan is used for the query, the cached plan entry has been used %d times", count))
      .build());
    numPlanCacheUses = count;
  }

  public int getNumPlanCacheUses() {
    return numPlanCacheUses;
  }

  // Serializes and stores plans
  private void serializeAndStoreRel(RelNode converted) throws Exception{
    PlannerSettings settings = PrelUtil.getSettings(converted.getCluster());
    if(!settings.isPlanSerializationEnabled()) {
      return;
    }
    RelNode toSerialize = converted.accept(new RelShuttleImpl() {
      @Override
      public RelNode visit(TableScan scan) {
        return LogicalTableScan.create(scan.getCluster(), scan.getTable(), ImmutableList.of());
      }
    });
    String planString = RelOptUtil.toString(toSerialize);
    if (planString != null && planString.length() < settings.getSerializationLengthLimit()) {
      this.serializedPlan = relSerializerFactory.getSerializer(converted.getCluster(), funcRegistry).serializeToBytes(toSerialize);
    }else{
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

    planPhases.add(PlanPhaseProfile.newBuilder()
      .setPhaseName(PlannerPhase.PLAN_CONVERTED_TO_REL)
      .setDurationMillis(millisTaken)
      .setPlan(convertedRelTree)
      .build());
  }

  @Override
  public void planConvertedScan(RelNode converted, long millisTaken) {
    final String convertedRelTree = toStringOrEmpty(converted, false);
    planPhases.add(PlanPhaseProfile.newBuilder()
      .setPhaseName(PlannerPhase.PLAN_CONVERTED_SCAN)
      .setDurationMillis(millisTaken)
      .setPlan(convertedRelTree)
      .build());
  }

  @Override
  public void planRelTransform(final PlannerPhase phase, RelOptPlanner planner, final RelNode before, final RelNode after, final long millisTaken) {
    final boolean noTransform = before == after;
    final String planAsString = toStringOrEmpty(after, noTransform || phase.forceVerbose());
    final long millisTakenFinalize = (phase.useMaterializations) ? millisTaken - (findMaterializationMillis + normalizationMillis + substitutionMillis) : millisTaken;
    if (phase.useMaterializations) {
      planPhases.add(PlanPhaseProfile.newBuilder()
        .setPhaseName(PlannerPhase.PLAN_REL_TRANSFORM)
        .setDurationMillis(substitutionMillis)
        .setPlan("")
        .build());
    }

    PlanPhaseProfile.Builder b = PlanPhaseProfile.newBuilder()
        .setPhaseName(phase.description)
        .setDurationMillis(millisTakenFinalize)
        .setPlan(planAsString);

    // dump state of volcano planner to troubleshoot costing issues (or long planning issues).
    if (verbose || noTransform) {
      final String dump = getPlanDump(planner);
      if (dump != null) {
        b.setPlannerDump(dump);
      }
      //System.out.println(Thread.currentThread().getName() + ":\n" + dump);
    }

    planPhases.add(b.build());

    if (verbose && phase.useMaterializations && planner instanceof VolcanoPlanner && numSubstitutions > 0) {
      try {
        Map<String, RelNode> bestPlansWithReflections = new CheapestPlanWithReflectionVisitor((VolcanoPlanner) planner).getBestPlansWithReflections();
        for (String reflection : bestPlansWithReflections.keySet()) {
          String plan = RelOptUtil.toString(bestPlansWithReflections.get(reflection), SqlExplainLevel.ALL_ATTRIBUTES);
          LayoutMaterializedViewProfile profile = mapIdToAccelerationProfile.get(reflection);
          if (profile != null) {
            mapIdToAccelerationProfile.put(
              reflection,
              LayoutMaterializedViewProfile.newBuilder(profile)
                .setOptimizedPlanBytes(ByteString.copyFrom(plan.getBytes()))
                .build()
            );
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
      tables.forEach(t -> {
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
        return UserBitShared.DatasetProfile.newBuilder()
          .setDatasetPath(t.getPath().getSchemaPath())
          .setType(UserBitShared.DatasetType.VDS)
          .setSql(view.getView().getSql())
          .build();
      } else {
        Boolean allowApproxStats = false;

        PhysicalDataset physicalDataset = t.getDatasetConfig().getPhysicalDataset();
        if (physicalDataset != null) {
          allowApproxStats = physicalDataset.getAllowApproxStats();
        }

        return UserBitShared.DatasetProfile.newBuilder()
          .setDatasetPath(t.getPath().getSchemaPath())
          .setType(UserBitShared.DatasetType.PDS)
          .setBatchSchema(ByteString.copyFrom(t.getSchema().serialize()))
          .setAllowApproxStats(allowApproxStats)
          .build();
      }
    } catch (Exception e) {
      logger.warn("Couldn't build dataset profile for table {}", t.getPath().getSchemaPath(), e);
    }

    return null;
  }

  @Override
  public void executorsSelected(long millisTaken, int idealNumFragments, int idealNumNodes, int numExecutors, String detailsText) {
    StringBuilder sb = new StringBuilder()
        .append("idealNumFragments: ").append(idealNumFragments).append("\n")
        .append("idealNumNodes    : ").append(idealNumNodes).append("\n")
        .append("numExecutors     : ").append(numExecutors).append("\n")
        .append("details          : ").append(detailsText);
    planPhases.add(PlanPhaseProfile.newBuilder()
        .setPhaseName("Execution Plan: Executor Selection")
        .setDurationMillis(millisTaken)
        .setPlan(sb.toString())
        .build());
  }

  @Override
  public void recordsProcessed(long recordCount) {
  }

  @Override
  public void planGenerationTime(long millisTaken) {
    planPhases.add(PlanPhaseProfile.newBuilder()
      .setPhaseName("Execution Plan: Plan Generation")
      .setDurationMillis(millisTaken)
      .setPlan("")
      .build());
  }

  @Override
  public void planAssignmentTime(long millisTaken) {
    planPhases.add(PlanPhaseProfile.newBuilder()
      .setPhaseName("Execution Plan: Fragment Assignment")
      .setDurationMillis(millisTaken)
      .setPlan("")
      .build());
  }

  @Override
  public void fragmentsStarted(long millisTaken, FragmentRpcSizeStats stats) {
    planPhases.add(PlanPhaseProfile.newBuilder()
      .setPhaseName("Fragment Start RPCs")
      .setDurationMillis(millisTaken)
      .setSizeStats(stats)
      .build());
  }

  @Override
  public void fragmentsActivated(long millisTaken) {
    planPhases.add(PlanPhaseProfile.newBuilder()
      .setPhaseName("Fragment Activate RPCs")
      .setDurationMillis(millisTaken)
      .build());
  }

  @Override
  public void updateReflectionsWithHints(ReflectionExplanationsAndQueryDistance reflectionExplanationsAndQueryDistance) {
    detailsPopulator.addReflectionHints(reflectionExplanationsAndQueryDistance);
  }

  public String getText() {
    return text;
  }

  public String getJson() {
    return json;
  }

  public String getFullSchema() {
    if (!verbose) {
      // Sometimes schema is too big especially when the schema has lot of nested columns. Enable only in verbose mode.
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

    if(plan == null) {
      return "";
    }

    try {
      return RelOptUtil.dumpPlan("", plan, SqlExplainFormat.TEXT,
        verbose ? SqlExplainLevel.ALL_ATTRIBUTES : SqlExplainLevel.EXPPLAN_ATTRIBUTES);
    } catch (UserCancellationException userCancellationException) {
      return "";
    }
  }

  public int getNumFragments() {
    return numFragments;
  }
}
