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

import com.dremio.exec.planner.acceleration.descriptor.ReflectionInfo;
import com.dremio.exec.proto.UserBitShared.ReflectionType;
import com.google.common.base.Preconditions;
import java.time.Duration;
import org.apache.calcite.rel.RelNode;

/** MaterializationTarget is created from a DremioMaterialization and used for matching */
public class MaterializationTarget {

  private final DremioMaterialization materialization;
  private final RelNode matchingRel;
  private final RelNode replacementRel;
  private final String info;
  private final Duration normalizationDuration;

  public MaterializationTarget(
      DremioMaterialization materialization, RelNode matchingRel, RelNode replacementRel) {
    this.materialization = materialization;
    this.matchingRel = matchingRel;
    this.replacementRel = replacementRel;
    this.info = null;
    this.normalizationDuration = null;
  }

  public MaterializationTarget(
      MaterializationTarget target, RelNode matchingRel, String info, Duration duration) {
    this.materialization = target.materialization;
    this.matchingRel = matchingRel;
    this.replacementRel = target.replacementRel;
    this.info = info;
    this.normalizationDuration = duration;
  }

  public String getReflectionId() {
    return materialization.getReflectionId();
  }

  public ReflectionInfo getLayoutInfo() {
    return materialization.getLayoutInfo();
  }

  public RelNode getTableRel() {
    return replacementRel;
  }

  public RelNode getQueryRel() {
    return matchingRel;
  }

  public String getMaterializationId() {
    return materialization.getMaterializationId();
  }

  public String getInfo() {
    return info;
  }

  public RelWithInfo getQueryRelWithInfo() {
    return RelWithInfo.create(matchingRel, info, normalizationDuration);
  }

  public ReflectionType getReflectionType() {
    return materialization.getReflectionType();
  }

  public DremioMaterialization getMaterialization() {
    return materialization;
  }

  public MaterializationTarget toNormalizedTarget(
      RelNode matchingRel, String info, Duration duration) {
    Preconditions.checkNotNull(info);
    Preconditions.checkNotNull(duration);
    return new MaterializationTarget(this, matchingRel, info, duration);
  }

  public MaterializationTarget withExpansionQuery(RelNode query) {
    return new MaterializationTarget(this, query, this.info, this.normalizationDuration);
  }
}
