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
package com.dremio.exec.util;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.planner.physical.filter.RuntimeFilterEntry;
import com.dremio.exec.planner.physical.filter.RuntimeFilterInfo;

/**
 * Groups the information within {@link com.dremio.exec.planner.physical.filter.RuntimeFilterEntry} objects indexed by
 * probe scan node.
 */
@NotThreadSafe
public class RuntimeFilterProbeTarget {
    private static final Logger logger = LoggerFactory.getLogger(RuntimeFilterInfo.class);
    private int probeScanMajorFragmentId;
    private int probeScanOperatorId;

    // partitioned at the probe side, and respective build side keys
    private List<String> partitionBuildTableKeys = new ArrayList<>();
    private List<String> partitionProbeTableKeys = new ArrayList<>();

    // non-partitioned at the probe side, and respective build side keys
    private List<String> nonPartitionBuildTableKeys = new ArrayList<>();
    private List<String> nonPartitionProbeTableKeys = new ArrayList<>();

    public RuntimeFilterProbeTarget(int probeScanMajorFragmentId, int probeScanOperatorId) {
        this.probeScanMajorFragmentId = probeScanMajorFragmentId;
        this.probeScanOperatorId = probeScanOperatorId;
    }

    public boolean isSameProbeCoordinate(int majorFragmentId, int operatorId) {
        return this.probeScanMajorFragmentId==majorFragmentId && this.probeScanOperatorId==operatorId;
    }

    private void addPartitionKey(String buildTableKey, String probeTableKey) {
        partitionBuildTableKeys.add(buildTableKey);
        partitionProbeTableKeys.add(probeTableKey);
    }

    private void addNonPartitionKey(String buildTableKey, String probeTableKey) {
        nonPartitionBuildTableKeys.add(buildTableKey);
        nonPartitionProbeTableKeys.add(probeTableKey);
    }

    public List<String> getPartitionBuildTableKeys() {
        return partitionBuildTableKeys;
    }

    public List<String> getPartitionProbeTableKeys() {
        return partitionProbeTableKeys;
    }

    public List<String> getNonPartitionBuildTableKeys() {
        return nonPartitionBuildTableKeys;
    }

    public List<String> getNonPartitionProbeTableKeys() {
        return nonPartitionProbeTableKeys;
    }

    public int getProbeScanMajorFragmentId() {
        return probeScanMajorFragmentId;
    }

    public int getProbeScanOperatorId() {
        return probeScanOperatorId;
    }

    @Override
    public String toString() {
        return "RuntimeFilterInfoProbeTarget{" +
                "probeScanMajorFragmentId=" + probeScanMajorFragmentId +
                ", probeScanOperatorId=" + probeScanOperatorId +
                ", partitionBuildTableKeys=" + partitionBuildTableKeys +
                ", partitionProbeTableKeys=" + partitionProbeTableKeys +
                ", nonPartitionBuildTableKeys=" + nonPartitionBuildTableKeys +
                ", nonPartitionProbeTableKeys=" + nonPartitionProbeTableKeys +
                '}';
    }

    public String toTargetIdString() {
        return "RuntimeFilterInfoProbeTarget{" +
                "probeScanMajorFragmentId=" + probeScanMajorFragmentId +
                ", probeScanOperatorId=" + probeScanOperatorId +
                '}';
    }

    /**
     * Returns list of probe targets, which contains mapping of probe scan node coordinates against respective join
     * table keys
     *
     * @param runtimeFilterInfo
     * @return
     */
    public static List<RuntimeFilterProbeTarget> getProbeTargets(RuntimeFilterInfo runtimeFilterInfo) {
        final List<RuntimeFilterProbeTarget> targets = new ArrayList<>();
        try {
            if (runtimeFilterInfo==null) {
                return targets;
            }

            for (RuntimeFilterEntry entry : runtimeFilterInfo.getPartitionJoinColumns()) {
                RuntimeFilterProbeTarget probeTarget = findOrCreateNew(targets, entry);
                probeTarget.addPartitionKey(entry.getBuildFieldName(), entry.getProbeFieldName());
            }

            for (RuntimeFilterEntry entry : runtimeFilterInfo.getNonPartitionJoinColumns()) {
                RuntimeFilterProbeTarget probeTarget = findOrCreateNew(targets, entry);
                probeTarget.addNonPartitionKey(entry.getBuildFieldName(), entry.getProbeFieldName());
            }
        } catch (RuntimeException e) {
            logger.error("Error while establishing probe scan targets from RuntimeFilterInfo", e);
        }
        return targets;
    }

    private static RuntimeFilterProbeTarget findOrCreateNew(final List<RuntimeFilterProbeTarget> probeTargets,
                                                            final RuntimeFilterEntry entry) {
        return probeTargets.stream()
                .filter(t -> t.isSameProbeCoordinate(entry.getProbeScanMajorFragmentId(), entry.getProbeScanOperatorId()))
                .findFirst()
                .orElseGet(() -> {
                    RuntimeFilterProbeTarget newTarget = new RuntimeFilterProbeTarget(entry.getProbeScanMajorFragmentId(), entry.getProbeScanOperatorId());
                    probeTargets.add(newTarget);
                    return newTarget;
                });
    }
}
