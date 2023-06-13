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
package com.dremio.exec.planner.sql.handlers;

import org.immutables.value.Value;

/**
 * Parameters for DremioFieldTrimmer.
 * This staged builder pattern enables us to simulate "named parameters" in Java.
 */
@Value.Style(stagedBuilder = true)
@Value.Immutable
public interface DremioFieldTrimmerParameters {
  boolean shouldLog();
  boolean isRelPlanning();
  boolean trimProjectedColumn();
  boolean trimJoinBranch();

  static ImmutableDremioFieldTrimmerParameters.ShouldLogBuildStage builder() {
    return ImmutableDremioFieldTrimmerParameters.builder();
  }
}
