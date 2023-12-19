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
package com.dremio.exec.planner.fragment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * Captures parallelization parameters for a given operator/fragments. It consists of min and max width of
 * parallelization and affinity to node endpoints.
 */
public class ParallelizationInfo {
  /**
   * Constraints on the width of parallelization.
   * Constraints listed in strictly decreasing order -- the further the constraint is, the more restrictive it is
   * In particular, applying one constraint over another results in the higher-ordinal constraint.
   * For example: applying affinity-limited width to an unlimited constraint results in affinity-limited constraint
   */
  public enum WidthConstraint {
    UNLIMITED,        // Width not limited: minimum: 1, maximum: Integer.MAX_VALUE
    AFFINITY_LIMITED, // Width strictly equal to the number of affinities
    SINGLE            // Width equal to 1 (min = 1; max = 1)
  }

  private final Map<NodeEndpoint, EndpointAffinity> affinityMap;
  private final int minWidth;
  private final int maxWidth;

  private ParallelizationInfo(int minWidth, int maxWidth, Map<NodeEndpoint, EndpointAffinity> affinityMap) {
    this.minWidth = minWidth;
    this.maxWidth = maxWidth;
    this.affinityMap = ImmutableMap.copyOf(affinityMap);
  }

  int getMinWidth() {
    return minWidth;
  }

  int getMaxWidth() {
    return maxWidth;
  }

  public Map<NodeEndpoint, EndpointAffinity> getEndpointAffinityMap() {
    return affinityMap;
  }

  @Override
  public String toString() {
    return getDigest(minWidth, maxWidth, affinityMap);
  }

  private static String getDigest(int minWidth, int maxWidth, Map<NodeEndpoint, EndpointAffinity> affinityMap) {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("[minWidth = %d, maxWidth=%d, epAff=[", minWidth, maxWidth));
    sb.append(Joiner.on(",").join(affinityMap.values()));
    sb.append("]]");

    return sb.toString();
  }

  /**
   * Return the more restrictive of the two width constraints
   */
  private static WidthConstraint moreRestrictive(WidthConstraint constraint1, WidthConstraint constraint2) {
    // NB: implementation relies on the fact that the widths are defined in strictly decreasing order
    return WidthConstraint.values()[Math.max(constraint1.ordinal(), constraint2.ordinal())];
  }

  /**
   * Collects/merges information that results in a ParallelizationInfo instance
   * The information collected includes:
   * - explicit widths (typically supplied by scans, generated from the splits that belong to this scan)
   * - width constraints (typically supplied by exchanges)
   * - affinity suppliers
   * The information is lazily put together when the collector is asked for a ParallelizationInfo instance
   * (at which point further collection is disallowed):
   * - The affinity suppliers are materialized, and merged together into a single affinity map
   * - the width constraints are instantiated (may be based on the affinity map, above), then applied to the
   *   numeric width limits
   */
  public static class ParallelizationInfoCollector {
    private int minWidth = 1;
    private int maxWidth = Integer.MAX_VALUE;
    private WidthConstraint widthConstraint = WidthConstraint.UNLIMITED;
    private final Map<NodeEndpoint, EndpointAffinity> affinityMap = Maps.newHashMap();
    private final List<Supplier<Collection<EndpointAffinity>>> uninstantiatedAffinities = new ArrayList<>();
    private ParallelizationInfo pInfo = null; // Computed lazily at get()

    void addMaxWidth(int newMaxWidth) {
      Preconditions.checkState(pInfo == null, "Invalid use of ParallelizationInfoCollector past get()");
      maxWidth = Math.min(maxWidth, newMaxWidth);
    }

    void addMinWidth(int newMinWidth) {
      Preconditions.checkState(pInfo == null, "Invalid use of ParallelizationInfoCollector past get()");
      minWidth = Math.max(minWidth, newMinWidth);
    }

    void addWidthConstraint(WidthConstraint newWidthConstraint) {
      Preconditions.checkState(pInfo == null, "Invalid use of ParallelizationInfoCollector past get()");
      widthConstraint = moreRestrictive(widthConstraint, newWidthConstraint);
    }

    /**
     * Get the minimum width so far. Please note that the endpoint affinity width constraints may not be applied unless
     * the endpoint affinities were materialized
     */
    int getMinWidth() {
      if (pInfo != null) {
        return pInfo.getMinWidth();
      }
      applyWidthConstraint(false);
      return minWidth;
    }

    /**
     * Get the maximum width so far. Please note that the endpoint affinity width constraints may not be applied unless
     * the endpoint affinities were materialized
     */
    int getMaxWidth() {
      if (pInfo != null) {
        return pInfo.getMaxWidth();
      }
      applyWidthConstraint(false);
      return maxWidth;
    }

    /**
     * Add any affinities coming from endpoints that are going to be running the query
     * @param endpointAffinity
     */
    void addEndpointAffinity(Supplier<Collection<EndpointAffinity>> endpointAffinity) {
      Preconditions.checkState(pInfo == null, "Invalid use of ParallelizationInfoCollector past get()");
      uninstantiatedAffinities.add(endpointAffinity);
    }

    /**
     * Add any affinities coming from splits
     */
    void addSplitAffinities(List<EndpointAffinity> endpointAffinities) {
      Preconditions.checkState(pInfo == null, "Invalid use of ParallelizationInfoCollector past get()");
      for(EndpointAffinity epAff : endpointAffinities) {
        addEndpointAffinity(epAff);
      }
    }

    // Helper method to add the given EndpointAffinity to the global affinity map
    private void addEndpointAffinity(EndpointAffinity epAff) {
      final EndpointAffinity epAffAgg = affinityMap.get(epAff.getEndpoint());
      if (epAffAgg != null) {
        epAffAgg.addAffinity(epAff.getAffinity());
        if (epAff.isAssignmentRequired()) {
          epAffAgg.setAssignmentRequired();
        }
        epAffAgg.setMaxWidth(epAff.getMaxWidth());
        epAffAgg.setMinWidth(epAff.getMinWidth());
      } else {
        affinityMap.put(epAff.getEndpoint(), epAff);
      }
    }

    /**
     * Get a ParallelizationInfo instance based on the current state of collected info.
     */
    public ParallelizationInfo get() {
      if (pInfo != null) {
        return pInfo;
      }
      materializeEndpointAffinities();
      applyWidthConstraint(true);
      pInfo = new ParallelizationInfo(minWidth, maxWidth, affinityMap);
      return pInfo;
    }

    private void materializeEndpointAffinities() {
      for (Supplier<Collection<EndpointAffinity>> affinitySupplier : uninstantiatedAffinities) {
        for (EndpointAffinity epAff : affinitySupplier.get()) {
          addEndpointAffinity(epAff);
        }
      }
    }

    private void applyWidthConstraint(boolean affinitiesMaterialized) {
      switch (widthConstraint) {
      case UNLIMITED:
        break;
      case AFFINITY_LIMITED:
        if (affinitiesMaterialized) {
          int maxWidth = affinityMap.values().stream().mapToInt(EndpointAffinity::getMaxWidth).sum();
          int minWidth = affinityMap.values().stream().mapToInt(EndpointAffinity::getMinWidth).sum();
          addMaxWidth(maxWidth);
          addMinWidth(minWidth);
        }
        break;
      case SINGLE:
        addMaxWidth(1);
        addMinWidth(1);
        break;
      default:
        throw new IllegalStateException(String.format("Invalid width constraint %s", widthConstraint));
      }
    }

    @Override
    public String toString() {
      return getDigest(minWidth, maxWidth, affinityMap);
    }
  }
}
