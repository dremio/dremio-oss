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
package com.dremio.exec.store;

import java.util.Set;

import org.apache.calcite.plan.RelOptRule;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.PlannerPhase;
import com.google.common.collect.ImmutableSet;

/**
 * Interface that must be implemented by classes used to get rules associated
 * with a storage plugin. Note that implementations must have a zero-argument
 * constructor.
 */
public interface StoragePluginRulesFactory {

  /**
   * Get rules for this type of plugin. We only add rules for types once across instances of each type.
   * @param optimizerContext
   * @param phase
   * @param pluginType
   * @return
   */
  Set<RelOptRule> getRules(OptimizerRulesContext optimizerContext, PlannerPhase phase, SourceType pluginType);

  /**
   * Get rules for this plugin instance. We call this for each plugin instance.
   * @param optimizerContext
   * @param phase
   * @param pluginId
   * @return
   */
  Set<RelOptRule> getRules(OptimizerRulesContext optimizerContext, PlannerPhase phase, StoragePluginId pluginId);

  public static class NoOpPluginRulesFactory implements StoragePluginRulesFactory {

    @Override
    public Set<RelOptRule> getRules(OptimizerRulesContext optimizerContext, PlannerPhase phase, SourceType pluginType) {
      return ImmutableSet.of();
    }

    @Override
    public Set<RelOptRule> getRules(OptimizerRulesContext optimizerContext, PlannerPhase phase, StoragePluginId pluginId) {
      return ImmutableSet.of();
    }

  }

  public abstract class StoragePluginTypeRulesFactory implements StoragePluginRulesFactory {
    @Override
    public Set<RelOptRule> getRules(OptimizerRulesContext optimizerContext, PlannerPhase phase, StoragePluginId pluginId) {
      return ImmutableSet.of();
    }
  }
}
