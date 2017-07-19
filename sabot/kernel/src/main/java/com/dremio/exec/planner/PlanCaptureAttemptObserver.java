/*
 * Copyright (C) 2017 Dremio Corporation
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


import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;

import com.dremio.exec.planner.acceleration.substitution.SubstitutionInfo;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionInfo.Substitution;
import com.dremio.exec.planner.observer.AbstractAttemptObserver;
import com.dremio.exec.planner.sql.DremioRelOptMaterialization;
import com.dremio.exec.planner.sql.MaterializationDescriptor;
import com.dremio.exec.proto.UserBitShared.AccelerationProfile;
import com.dremio.exec.proto.UserBitShared.LayoutMaterializedViewProfile;
import com.dremio.exec.proto.UserBitShared.PlanPhaseProfile;
import com.dremio.exec.proto.UserBitShared.SubstitutionProfile;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;

public class PlanCaptureAttemptObserver extends AbstractAttemptObserver {

  private final boolean verbose;
  private final List<PlanPhaseProfile> planPhases = Lists.newArrayList();
  private final Map<Pair<String, String>, LayoutMaterializedViewProfile>
    mapIdToAccelerationProfile = new LinkedHashMap<>();

  private String text;
  private String json;

  private boolean accelerated = false;
  private long findMaterializationMillis = 0;
  private long normalizationMillis = 0;
  private long substitutionMillis = 0;
  private int numSubstitutions = 0;

  public PlanCaptureAttemptObserver(boolean verbose) {
    this.verbose = verbose;
  }

  public AccelerationProfile getAccelerationProfile() {
    AccelerationProfile.Builder builder = AccelerationProfile.newBuilder()
      .setAccelerated(accelerated)
      .setNumSubstitutions(numSubstitutions)
      .setMillisTakenGettingMaterializations(findMaterializationMillis)
      .setMillisTakenNormalizing(normalizationMillis)
      .setMillisTakenSubstituting(substitutionMillis);
    if (!mapIdToAccelerationProfile.isEmpty()) {
      builder.addAllLayoutProfiles(mapIdToAccelerationProfile.values());
    }
    return builder.build();
  }

  public List<PlanPhaseProfile> getPlanPhases() {
    return planPhases;
  }

  @Override
  public void planText(String text, long millisTaken) {
    this.text = text;
    planPhases.add(PlanPhaseProfile.newBuilder()
      .setPhaseName("Final Physical Transformation")
      .setDurationMillis(millisTaken)
      .setPlan(text)
      .build());
  }

  @Override
  public void planJsonPlan(String text) {
    this.json = text;
  }

  @Override
  public void planAccelerated(final SubstitutionInfo info) {
    accelerated = true;
    for (Substitution sub : info.getSubstitutions()) {
      final MaterializationDescriptor descriptor = sub.getMaterialization();
      final Pair<String, String> key = Pair.of(descriptor.getLayoutId(), descriptor.getMaterializationId());

      final LayoutMaterializedViewProfile lmvProfile = mapIdToAccelerationProfile.get(key);
      if (lmvProfile != null) {
        mapIdToAccelerationProfile.put(key, LayoutMaterializedViewProfile.newBuilder(lmvProfile).setNumUsed(lmvProfile.getNumUsed() + 1).build());
      }
    }
  }

  @Override
  public void planFindMaterializations(long millisTaken) {
    findMaterializationMillis = millisTaken;
    planPhases.add(PlanPhaseProfile.newBuilder()
      .setPhaseName("Find Materializations")
      .setDurationMillis(millisTaken)
      .setPlan("")
      .build());
  }

  @Override
  public void planNormalized(long millisTaken) {
    normalizationMillis = millisTaken;
    planPhases.add(PlanPhaseProfile.newBuilder()
      .setPhaseName("Normalization")
      .setDurationMillis(normalizationMillis)
      .setPlan("")
      .build());
  }

  @Override
  public void planSubstituted(DremioRelOptMaterialization materialization,
                              List<RelNode> substitutions,
                              RelNode query, RelNode target, long millisTaken) {
    final Pair<String, String> key = Pair.of(materialization.getLayoutId(), materialization.getMaterializationId());
    final LayoutMaterializedViewProfile oldProfile = mapIdToAccelerationProfile.get(key);

    final LayoutMaterializedViewProfile.Builder layoutBuilder;
    if (oldProfile == null) {
      layoutBuilder = LayoutMaterializedViewProfile.newBuilder()
        .setLayoutId(materialization.getLayoutId())
        .setMaterializationId(materialization.getMaterializationId())
        .setMaterializationExpirationTimestamp(materialization.getExpirationTimestamp())
        .addAllDimensions(materialization.getLayoutInfo().getDimensions())
        .addAllMeasures(materialization.getLayoutInfo().getMeasures())
        .addAllSortedColumns(materialization.getLayoutInfo().getSortColumns())
        .addAllPartitionedColumns(materialization.getLayoutInfo().getPartitionColumns())
        .addAllDistributionColumns(materialization.getLayoutInfo().getDistributionColumns())
        .addAllDisplayColumns(materialization.getLayoutInfo().getDisplayColumns())
        .setNumSubstitutions(substitutions.size())
        .setMillisTakenSubstituting(millisTaken)
        .setPlan(asString(materialization.queryRel))
        .addNormalizedPlans(asString(target));
    } else {
      layoutBuilder = LayoutMaterializedViewProfile.newBuilder(oldProfile)
        .setNumSubstitutions(oldProfile.getNumSubstitutions() + substitutions.size())
        .setMillisTakenSubstituting(oldProfile.getMillisTakenSubstituting() + millisTaken);
    }
    if (verbose) {
      layoutBuilder
        .addNormalizedQueryPlans(asString(query))
        .addAllSubstitutions(FluentIterable.from(substitutions).transform(new Function<RelNode, SubstitutionProfile>() {
          @Nullable
          @Override
          public SubstitutionProfile apply(@Nullable RelNode input) {
            return SubstitutionProfile.newBuilder().setPlan(asString(input)).build();
          }
        }));
    }
    mapIdToAccelerationProfile.put(key, layoutBuilder.build());
    substitutionMillis += millisTaken;
    numSubstitutions += substitutions == null ? 0 : substitutions.size();
  }

  @Override
  public void planValidated(RelDataType rowType, SqlNode node, long millisTaken) {
    planPhases.add(PlanPhaseProfile.newBuilder()
      .setPhaseName("Validation")
      .setDurationMillis(millisTaken)
      .setPlan("")
      .build());
  }

  @Override
  public void planConvertedToRel(RelNode converted, long millisTaken) {
    final String convertedRelTree = asString(converted);
    planPhases.add(PlanPhaseProfile.newBuilder()
      .setPhaseName("Convert To Rel")
      .setDurationMillis(millisTaken)
      .setPlan(convertedRelTree)
      .build());
  }

  @Override
  public void planConvertedScan(RelNode converted, long millisTaken) {
    final String convertedRelTree = asString(converted);
    planPhases.add(PlanPhaseProfile.newBuilder()
      .setPhaseName("Convert Scan")
      .setDurationMillis(millisTaken)
      .setPlan(convertedRelTree)
      .build());
  }

  @Override
  public void planRelTransform(final PlannerPhase phase, final RelNode before, final RelNode after, final long millisTaken) {
    final String planAsString = asString(after);
    final long millisTakenFinalize = (phase.useMaterializations) ? millisTaken - (findMaterializationMillis + normalizationMillis + substitutionMillis) : millisTaken;
    if (phase.useMaterializations) {
      planPhases.add(PlanPhaseProfile.newBuilder()
        .setPhaseName("Substitution")
        .setDurationMillis(substitutionMillis)
        .setPlan("")
        .build());
    }
    planPhases.add(PlanPhaseProfile.newBuilder()
      .setPhaseName(phase.description)
      .setDurationMillis(millisTakenFinalize)
      .setPlan(planAsString)
      .build());
  }

  @Override
  public void planGenerationTime(long millisTaken) {
    planPhases.add(PlanPhaseProfile.newBuilder()
      .setPhaseName("Plan Generation")
      .setDurationMillis(millisTaken)
      .setPlan("")
      .build());
  }

  @Override
  public void planAssignmentTime(long millisTaken) {
    planPhases.add(PlanPhaseProfile.newBuilder()
      .setPhaseName("Fragment Assignment")
      .setDurationMillis(millisTaken)
      .setPlan("")
      .build());
  }

  @Override
  public void intermediateFragmentScheduling(long millisTaken) {
    planPhases.add(PlanPhaseProfile.newBuilder()
      .setPhaseName("Intermediate Fragments Scheduling")
      .setDurationMillis(millisTaken)
      .setPlan("")
      .build());
  }

  @Override
  public void leafFragmentScheduling(long millisTaken) {
    planPhases.add(PlanPhaseProfile.newBuilder()
      .setPhaseName("Leaf Fragments Scheduling")
      .setDurationMillis(millisTaken)
      .setPlan("")
      .build());
  }

  public String getText() {
    return text;
  }

  public String getJson() {
    return json;
  }

  public String asString(final RelNode plan) {
    if (!verbose) {
      return "";
    }
    return RelOptUtil.dumpPlan("", plan, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES);
  }

}
