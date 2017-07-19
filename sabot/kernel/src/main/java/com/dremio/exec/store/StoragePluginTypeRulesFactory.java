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
package com.dremio.exec.store;

import java.util.Set;

import org.apache.calcite.plan.RelOptRule;

import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.service.namespace.StoragePluginType;
import com.google.common.collect.ImmutableSet;

/**
 * Interface that must be implemented by classes used to get rules associated
 * with a storage plugin. Note that implementations must have a zero-argument
 * constructor. StoragePluginTypeRulesFactory is called once per StoragePluginType.
 */
public interface StoragePluginTypeRulesFactory {

  StoragePluginTypeRulesFactory NO_OP = new StoragePluginTypeRulesFactory(){
    @Override
    public Set<RelOptRule> getRules(OptimizerRulesContext optimizerContext, PlannerPhase phase,
        StoragePluginType pluginType) {
      return ImmutableSet.of();
    }};

  Set<RelOptRule> getRules(OptimizerRulesContext optimizerContext, PlannerPhase phase, StoragePluginType pluginType);
}
