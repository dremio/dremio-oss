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
package com.dremio.exec.planner.fragment;

import java.util.HashMap;
import java.util.Map;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.base.AbstractPhysicalVisitor;
import com.dremio.exec.physical.base.FragmentRoot;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentMajor;
import com.dremio.options.OptionList;

/**
 * This reader caches the de-serialized object from json, and avoids repeated de-serializations for
 * each minor.
 */
public class CachedFragmentReader extends AbstractPhysicalVisitor<PhysicalOperator, Void, ExecutionSetupException>{
  final PhysicalPlanReader reader;
  final PlanFragmentsIndex planFragmentsIndex;
  // Cached deserialized fragment_json
  final Map<Integer, FragmentRoot> majorIdToRootMap = new HashMap<>();
  // Cached deserialized options_json
  final Map<Integer, OptionList> majorIdToOptionListMap = new HashMap<>();

  public CachedFragmentReader(PhysicalPlanReader reader, PlanFragmentsIndex planFragmentsIndex) {
    this.reader = reader;
    this.planFragmentsIndex = planFragmentsIndex;
  }

  public PlanFragmentsIndex getPlanFragmentsIndex() {
    return planFragmentsIndex;
  }

  public OptionList readOptions(PlanFragmentFull planFragment) throws ExecutionSetupException {
    int majorId = planFragment.getMajorFragmentId();

    // Check if already present in the cache.
    OptionList list = majorIdToOptionListMap.get(majorId);

    if (list == null) {
      // deserialize and add to cache.
      final PlanFragmentMajor major = planFragment.getMajor();
      if (!major.hasOptionsJson() || major.getOptionsJson().isEmpty()) {
        list = new OptionList();
      } else {
        try {
          list = reader.readOptionList(major.getOptionsJson(), major.getFragmentCodec());
          majorIdToOptionListMap.put(majorId, list);
        } catch (final Exception e) {
          throw new ExecutionSetupException("Failure while reading plan options.", e);
        }
      }
    }
    return list;
  }

  public FragmentRoot readFragment(PlanFragmentFull planFragment) throws ExecutionSetupException {
    int majorId = planFragment.getMajorFragmentId();
    FragmentRoot root = majorIdToRootMap.get(majorId);

    // Check if already present in the cache.
    if (root == null) {
      // serialize and Cache the operator tree.
      final PlanFragmentMajor major = planFragment.getMajor();
      try {
        root = reader.readFragmentOperator(major.getFragmentJson(), major.getFragmentCodec());
        majorIdToRootMap.put(majorId, root);
      } catch (final Exception e) {
        throw new ExecutionSetupException("Failure while reading fragment json.", e);
      }
    }

    // Copy the operator tree. Populate minor-specific attributes in the copy, and return the copy.
    final MinorDataSerDe minorDataSerDe = new MinorDataSerDe(reader,
      planFragment.getMajor().getFragmentCodec());
    return (FragmentRoot)MinorDataPopulator.populate(planFragment.getHandle(), root,
      minorDataSerDe,
      MinorAttrsMap.create(planFragment.getMinor().getAttrsList()),
      planFragmentsIndex);
  }
}
