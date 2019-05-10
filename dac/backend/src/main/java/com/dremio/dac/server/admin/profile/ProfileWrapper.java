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
package com.dremio.dac.server.admin.profile;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.proto.UserBitShared.MajorFragmentProfile;
import com.dremio.exec.proto.UserBitShared.MinorFragmentProfile;
import com.dremio.exec.proto.UserBitShared.NodeQueryProfile;
import com.dremio.exec.proto.UserBitShared.OperatorProfile;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.service.accelerator.AccelerationDetailsUtils;
import com.dremio.service.accelerator.proto.AccelerationDetails;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

/**
 * Wrapper class for a {@link #profile query profile}, so it to be presented through web UI.
 */
public class ProfileWrapper {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProfileWrapper.class);

  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance(Locale.US);

  private QueryProfile profile;
  private String id;
  private final List<FragmentWrapper> fragmentProfiles;
  private final List<NodeWrapper> nodeProfiles;
  private final List<OperatorWrapper> operatorProfiles;
  private final AccelerationWrapper accelerationDetails;

  public ProfileWrapper(final QueryProfile profile, boolean debug) {
    this.profile = profile;
    this.id = QueryIdHelper.getQueryId(profile.getId());

    final List<FragmentWrapper> fragmentProfiles = new ArrayList<>();

    final List<MajorFragmentProfile> majors = new ArrayList<>(profile.getFragmentProfileList());
    Collections.sort(majors, Comparators.majorId);

    for (final MajorFragmentProfile major : majors) {
      fragmentProfiles.add(new FragmentWrapper(major, profile.getStart(), debug));
    }
    this.fragmentProfiles = fragmentProfiles;

    final List<NodeWrapper> nodeProfiles = new ArrayList<>();

    final List<NodeQueryProfile> nodeQueryProfiles = new ArrayList<>(profile.getNodeProfileList());
    Collections.sort(nodeQueryProfiles, Comparators.endpoint);

    for (final NodeQueryProfile nodeQueryProfile : nodeQueryProfiles) {
      nodeProfiles.add(new NodeWrapper(nodeQueryProfile, debug));
    }
    this.nodeProfiles = nodeProfiles;

    final List<OperatorWrapper> ows = new ArrayList<>();
    // temporary map to store (major_id, operator_id) -> [(op_profile, minor_id)]
    final Map<ImmutablePair<Integer, Integer>, List<ImmutablePair<OperatorProfile, Integer>>> opmap = new HashMap<>();

    Collections.sort(majors, Comparators.majorId);
    for (final MajorFragmentProfile major : majors) {

      final List<MinorFragmentProfile> minors = new ArrayList<>(major.getMinorFragmentProfileList());
      Collections.sort(minors, Comparators.minorId);
      for (final MinorFragmentProfile minor : minors) {

        final List<OperatorProfile> ops = new ArrayList<>(minor.getOperatorProfileList());
        Collections.sort(ops, Comparators.operatorId);
        for (final OperatorProfile op : ops) {

          final ImmutablePair<Integer, Integer> ip = new ImmutablePair<>(
              major.getMajorFragmentId(), op.getOperatorId());
          if (!opmap.containsKey(ip)) {
            final List<ImmutablePair<OperatorProfile, Integer>> l = new ArrayList<>();
            opmap.put(ip, l);
          }
          opmap.get(ip).add(new ImmutablePair<>(op, minor.getMinorFragmentId()));
        }
      }
    }

    final List<ImmutablePair<Integer, Integer>> keys = new ArrayList<>(opmap.keySet());
    Collections.sort(keys);

    for (final ImmutablePair<Integer, Integer> ip : keys) {
      ows.add(new OperatorWrapper(ip.getLeft(), opmap.get(ip), profile.hasOperatorTypeMetricsMap() ? profile.getOperatorTypeMetricsMap(): null));
    }
    this.operatorProfiles = ows;

    AccelerationWrapper wrapper = null;
    try {
      AccelerationDetails details = AccelerationDetailsUtils.deserialize(profile.getAccelerationProfile().getAccelerationDetails());
      if (details != null && details.getReflectionRelationshipsList() != null) {
        wrapper = new AccelerationWrapper(details);
      }
    } catch (Exception e) {
      // do not fail if we are unable to deserialize the acceleration details
      logger.warn("Failed to deserialize acceleration details", e);
    }
    accelerationDetails = wrapper;
  }

  /**
   * @return
   */
  @SuppressWarnings("unused")
  public String getCommandPoolWaitMillis() {
    final QueryProfile profile = getProfile();
    if (!profile.hasCommandPoolWaitMillis()) {
      return "";
    }
    return NUMBER_FORMAT.format(profile.getCommandPoolWaitMillis()) + "ms";
  }

  /**
   * @return Get query planning time. If the planning hasn't started, returns "Planning not started". If planning hasn't
   * completed, returns "Still planning".
   */
  @SuppressWarnings("unused")
  public String getPlanningTime() {
    final QueryProfile profile = getProfile();
    if (!profile.hasPlanningStart() || profile.getPlanningStart() == 0) {
      return "Planning not started";
    }

    if (!profile.hasPlanningEnd() || profile.getPlanningEnd() == 0) {
      return "Still planning";
    }

    // Starting from 3.0, the planning time includes the resource queueing time. Thus, correcting for it when resource scheduling time exists
    long planningPlusSchedulingTime = profile.getPlanningEnd() - profile.getPlanningStart();

    UserBitShared.ResourceSchedulingProfile r = profile.getResourceSchedulingProfile();
    if (r == null || r.getResourceSchedulingStart() == 0 || r.getResourceSchedulingEnd() == 0) {
      return NUMBER_FORMAT.format(planningPlusSchedulingTime) + "ms";
    }
    long schedulingTime = r.getResourceSchedulingEnd() - r.getResourceSchedulingStart();

    return NUMBER_FORMAT.format(planningPlusSchedulingTime - schedulingTime) + "ms";
  }

  @SuppressWarnings("unused")
  public String getQueueTime() {
    UserBitShared.ResourceSchedulingProfile r = profile.getResourceSchedulingProfile();
    if (r == null || r.getResourceSchedulingStart() == 0 || r.getResourceSchedulingEnd() == 0) {
      return "";
    }
    return NUMBER_FORMAT.format(r.getResourceSchedulingEnd() - r.getResourceSchedulingStart()) + "ms";
  }

  @SuppressWarnings("unused")
  public String getResourceSchedulingOverview() {
    UserBitShared.ResourceSchedulingProfile r = profile.getResourceSchedulingProfile();
    if (r == null) {
      return "";
    }
    DescriptionListBuilder dlb = new DescriptionListBuilder();
    if (r.hasQueueName()) {
      dlb.addItem("Queue Name:", r.getQueueName());
    }
    if (r.hasQueueId()) {
      dlb.addItem("Queue Id:", r.getQueueId());
    }
    if (r.hasRuleName()) {
      dlb.addItem("Rule Name:", r.getRuleName());
    }
    if (r.hasRuleId()) {
      dlb.addItem("Rule Id:", r.getRuleId());
    }
    if (r.hasRuleContent()) {
      dlb.addItem("Rule Content:", r.getRuleContent());
    }
    if (r.hasRuleAction()) {
      dlb.addItem("Rule Action:", r.getRuleAction());
    }
    if (r.hasSchedulingProperties()) {
      UserBitShared.ResourceSchedulingProperties rsp = r.getSchedulingProperties();
      if (rsp.hasQueryCost()) {
        dlb.addItem("Query Cost:", String.format("%.0f", rsp.getQueryCost()));
      }
      if (rsp.hasQueryType()) {
        dlb.addItem("Query Type:", rsp.getQueryType()); // this maps to WorkloadType internally
      }
    }
    if (profile.hasCancelReason()) {
      dlb.addItem("Cancellation Reason:", profile.getCancelReason());
    }
    return dlb.build();
  }

  @SuppressWarnings("unused")
  public boolean hasError() {
    return profile.hasError() && profile.getError() != null;
  }

  @SuppressWarnings("unused")
  public String getQuerySchema() {
    final String schema = profile.getFullSchema();
    if (Strings.isNullOrEmpty(schema)) {
      return null;
    }
    return schema;
  }

  @SuppressWarnings("unused")
  public String getNonDefaultOptions() {
    final String options = profile.getNonDefaultOptionsJSON();
    if (Strings.isNullOrEmpty(options)) {
      return null;
    }

    return options;
  }

  public QueryProfile getProfile() {
    return profile;
  }

  public AccelerationWrapper getAccelerationDetails() {
    return accelerationDetails;
  }

  public String getQueryId() {
    return id;
  }

  public String getPlanText() {
    return StringEscapeUtils.escapeJson(profile.getPlan());
  }

  @SuppressWarnings("unused")
  public List<FragmentWrapper> getFragmentProfiles() {
    return fragmentProfiles;
  }

  public int getFragmentProfilesSize() {
    return fragmentProfiles.size();
  }

  @SuppressWarnings("unused")
  public String getFragmentsOverview() {
    TableBuilder tb = new TableBuilder(FragmentWrapper.FRAGMENT_OVERVIEW_COLUMNS);
    for (final FragmentWrapper fw : fragmentProfiles) {
      fw.addSummary(tb);
    }
    return tb.build();
  }

  @SuppressWarnings("unused")
  public String getNodesOverview() {
    TableBuilder tb = new TableBuilder(NodeWrapper.NODE_OVERVIEW_COLUMNS);
    for (final NodeWrapper fw : nodeProfiles) {
      fw.addSummary(tb);
    }
    return tb.build();
  }

  @SuppressWarnings("unused")
  public List<OperatorWrapper> getOperatorProfiles() {
    return operatorProfiles;
  }

  @SuppressWarnings("unused")
  public String getOperatorsOverview() {
    final TableBuilder tb = new TableBuilder(OperatorWrapper.OPERATORS_OVERVIEW_COLUMNS);
    for (final OperatorWrapper ow : operatorProfiles) {
      ow.addSummary(tb);
    }
    return tb.build();
  }

  @SuppressWarnings("unused")
  public String getOperatorsJSON() {
    final StringBuilder sb = new StringBuilder("{");
    String sep = "";
    for (final CoreOperatorType op : CoreOperatorType.values()) {
      sb.append(String.format("%s\"%d\" : \"%s\"", sep, op.ordinal(), op));
      sep = ", ";
    }
    return sb.append("}").toString();
  }

  public Map<DatasetPath, List<UserBitShared.LayoutMaterializedViewProfile>> getDatasetGroupedLayoutList() {
    Map<DatasetPath, List<UserBitShared.LayoutMaterializedViewProfile>> map = Maps.newHashMap();

    UserBitShared.AccelerationProfile accelerationProfile = profile.getAccelerationProfile();
    List<UserBitShared.LayoutMaterializedViewProfile> layoutProfilesList = accelerationProfile.getLayoutProfilesList();

    for (UserBitShared.LayoutMaterializedViewProfile viewProfile : layoutProfilesList) {
      String reflectionDatasetPath = accelerationDetails.getReflectionDatasetPath(viewProfile.getLayoutId());

      DatasetPath path;

      if ("".equals(reflectionDatasetPath)) {
        path = new DatasetPath(Arrays.asList("unknown", "missing dataset"));
      } else {
        path = new DatasetPath(reflectionDatasetPath);
      }

      if (!map.containsKey(path)) {
        map.put(path, new ArrayList<UserBitShared.LayoutMaterializedViewProfile>());
      }
      map.get(path).add(viewProfile);
    }

    return map;
  }

  public String getPerdiodFromStart(Long datetime) {
    if (datetime == null) {
      return "";
    }
    return DurationFormatUtils.formatDurationWords( this.profile.getStart() - datetime, true, true);
  }
}
