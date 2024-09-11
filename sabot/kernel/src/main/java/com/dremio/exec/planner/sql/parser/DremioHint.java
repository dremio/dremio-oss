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
package com.dremio.exec.planner.sql.parser;

import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.options.OptionValidator;
import java.util.stream.Stream;
import org.apache.calcite.rel.hint.HintStrategies;
import org.apache.calcite.rel.hint.HintStrategy;
import org.apache.calcite.rel.hint.HintStrategyTable;

public enum DremioHint {
  BROADCAST("BROADCAST", HintStrategies.TABLE_SCAN),
  CONSIDER_REFLECTIONS("CONSIDER_REFLECTIONS", PlannerSettings.CONSIDER_REFLECTIONS),
  EXCLUDE_REFLECTIONS("EXCLUDE_REFLECTIONS", PlannerSettings.EXCLUDE_REFLECTIONS),
  CHOOSE_REFLECTIONS("CHOOSE_REFLECTIONS", PlannerSettings.CHOOSE_REFLECTIONS),
  NO_REFLECTIONS("NO_REFLECTIONS", PlannerSettings.NO_REFLECTIONS),
  CURRENT_ICEBERG_DATA_ONLY("CURRENT_ICEBERG_DATA_ONLY", PlannerSettings.CURRENT_ICEBERG_DATA_ONLY),
  PREFER_CACHED_METADATA("PREFER_CACHED_METADATA", PlannerSettings.PREFER_CACHED_METADATA);

  private final String hintName;
  private final HintStrategy strategy;
  private final OptionValidator option;

  DremioHint(String hintName, HintStrategy strategy) {
    this.hintName = hintName;
    this.strategy = strategy;
    this.option = null;
  }

  /**
   * Creates a query level hint that can also be set at the session level through an option. Since
   * options have precedence, i.e. query > session > system, then setting a query level option using
   * a hint will override a session level option.
   *
   * @param hintName
   * @param option
   */
  DremioHint(String hintName, OptionValidator option) {
    this.hintName = hintName;
    this.strategy = HintStrategies.SET_VAR;
    this.option = option;
  }

  public String getHintName() {
    return hintName;
  }

  public OptionValidator getOption() {
    return option;
  }

  public static HintStrategyTable buildHintStrategyTable() {
    HintStrategyTable.Builder builder = HintStrategyTable.builder();
    Stream.of(values()).forEach(x -> builder.addHintStrategy(x.hintName, x.strategy));
    return builder.build();
  }
}
