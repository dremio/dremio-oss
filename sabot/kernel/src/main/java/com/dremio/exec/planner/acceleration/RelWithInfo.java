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
package com.dremio.exec.planner.acceleration;

import java.time.Duration;

import org.apache.calcite.rel.RelNode;

/**
 * RelWithInfo wraps a normalized user query plan, normalized target materialization plan or replacement plan.
 * It includes additional information about the source of this plan such as the matching phase and time to
 * generate the plan such as normalization times.
 */
public final class RelWithInfo {
  private final RelNode rel;
  private final String info;
  private final Duration elapsed;
  private RelWithInfo(RelNode node, String info, Duration elapsed) {
    this.rel = node;
    this.info = info;
    this.elapsed = elapsed;
  }
  public static RelWithInfo create(RelNode node, String info, Duration elapsed) {
    return new RelWithInfo(node, info, elapsed);
  }
  public RelWithInfo cloneWithRelNode(RelNode node) {
    return new RelWithInfo(node, this.getInfo(), this.getElapsed());
  }

  public RelNode getRel() {
    return rel;
  }

  public String getInfo() {
    return info;
  }

  public Duration getElapsed() {
    return elapsed;
  }
}
