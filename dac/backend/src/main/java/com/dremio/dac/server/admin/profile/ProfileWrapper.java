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
package com.dremio.dac.server.admin.profile;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.proto.UserBitShared.MajorFragmentProfile;
import com.dremio.exec.proto.UserBitShared.MinorFragmentProfile;
import com.dremio.exec.proto.UserBitShared.OperatorProfile;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.proto.helper.QueryIdHelper;
import com.google.common.base.Strings;

/**
 * Wrapper class for a {@link #profile query profile}, so it to be presented through web UI.
 */
public class ProfileWrapper {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProfileWrapper.class);

  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance(Locale.US);

  private QueryProfile profile;
  private String id;
  private final List<FragmentWrapper> fragmentProfiles;
  private final List<OperatorWrapper> operatorProfiles;

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
      ows.add(new OperatorWrapper(ip.getLeft(), opmap.get(ip)));
    }
    this.operatorProfiles = ows;
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

    return NUMBER_FORMAT.format(profile.getPlanningEnd() - profile.getPlanningStart()) + "ms";
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

  public QueryProfile getProfile() {
    return profile;
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
}
