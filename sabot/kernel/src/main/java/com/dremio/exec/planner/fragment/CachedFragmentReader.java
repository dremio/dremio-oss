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

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.base.AbstractPhysicalVisitor;
import com.dremio.exec.physical.base.FragmentRoot;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentMajor;
import com.dremio.options.OptionList;
import com.google.common.collect.Maps;
import java.util.Map;

/**
 * This reader caches the de-serialized object from json, and avoids repeated de-serializations for
 * each minor.
 */
public class CachedFragmentReader
    extends AbstractPhysicalVisitor<PhysicalOperator, Void, ExecutionSetupException> {
  final PhysicalPlanReader reader;
  final PlanFragmentsIndex planFragmentsIndex;
  // Cached deserialized fragment_json
  final Map<Integer, FragmentRoot> majorIdToRootMap = Maps.newConcurrentMap();
  // Cached deserialized options_json
  final Map<Integer, OptionList> majorIdToOptionListMap = Maps.newConcurrentMap();

  public CachedFragmentReader(PhysicalPlanReader reader, PlanFragmentsIndex planFragmentsIndex) {
    this.reader = reader;
    this.planFragmentsIndex = planFragmentsIndex;
  }

  public PlanFragmentsIndex getPlanFragmentsIndex() {
    return planFragmentsIndex;
  }

  public OptionList readOptions(PlanFragmentFull planFragment) throws ExecutionSetupException {
    int majorId = planFragment.getMajorFragmentId();
    final PlanFragmentMajor major = planFragment.getMajor();

    // Check if already present in the cache.
    return majorIdToOptionListMap.computeIfAbsent(
        majorId,
        k -> {
          OptionList innerList = null;
          if (!major.hasOptionsJson() || major.getOptionsJson().isEmpty()) {
            innerList = new OptionList();
          } else {
            try {
              innerList = reader.readOptionList(major.getOptionsJson(), major.getFragmentCodec());
            } catch (final Exception e) {
              throw new RuntimeException("Failure while reading plan options.", e);
            }
          }
          return innerList;
        });
  }

  public FragmentRoot readFragment(PlanFragmentFull planFragment) throws ExecutionSetupException {
    int majorId = planFragment.getMajorFragmentId();
    final PlanFragmentMajor major = planFragment.getMajor();

    FragmentRoot root =
        majorIdToRootMap.computeIfAbsent(
            majorId,
            k -> {
              try {
                return reader.readFragmentOperator(
                    major.getFragmentJson(), major.getFragmentCodec());
              } catch (final Exception e) {
                throw new RuntimeException(e);
              }
            });

    // Copy the operator tree. Populate minor-specific attributes in the copy, and return the copy.
    final MinorDataSerDe minorDataSerDe =
        new MinorDataSerDe(reader, planFragment.getMajor().getFragmentCodec());
    return (FragmentRoot)
        MinorDataPopulator.populate(
            planFragment.getHandle(),
            root,
            minorDataSerDe,
            MinorAttrsMap.create(planFragment.getMinor().getAttrsList()),
            planFragmentsIndex);
  }
}
