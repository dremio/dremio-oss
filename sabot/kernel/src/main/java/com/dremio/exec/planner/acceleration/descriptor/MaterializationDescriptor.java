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

package com.dremio.exec.planner.acceleration.descriptor;

import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.acceleration.IncrementalUpdateSettings;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionUtils;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionUtils.VersionedPath;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.proto.UserBitShared.ReflectionType;
import java.util.List;
import java.util.Set;

public interface MaterializationDescriptor {
  IncrementalUpdateSettings getIncrementalUpdateSettings();

  String getLayoutId();

  ReflectionInfo getLayoutInfo();

  String getMaterializationId();

  String getVersion();

  long getExpirationTimestamp();

  List<String> getPath();

  double getOriginalCost();

  long getJobStart();

  List<String> getPartition();

  boolean isApplicable(
      Set<VersionedPath> queryTablesUsed,
      Set<SubstitutionUtils.VersionedPath> queryVdsUsed,
      Set<SubstitutionUtils.ExternalQueryDescriptor> externalQueries);

  DremioMaterialization getMaterializationFor(SqlConverter converter);

  ReflectionType getReflectionType();

  boolean isStale();
}
