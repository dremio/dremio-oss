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
package com.dremio.dac.util;

import com.dremio.common.Version;
import com.dremio.dac.proto.model.source.ClusterVersion;

/**
 * Helper methods to convert from/to {@link ClusterVersion}
 */
public final class ClusterVersionUtils {

  private ClusterVersionUtils() {}

  public static ClusterVersion toClusterVersion(Version version) {
    return new ClusterVersion()
      .setMajor(version.getMajorVersion())
      .setMinor(version.getMinorVersion())
      .setBuildNumber(version.getBuildNumber())
      .setPatch(version.getPatchVersion())
      .setQualifier(version.getQualifier());
  }

  public static Version fromClusterVersion(ClusterVersion version) {
    if (version == null) {
      return null;
    }
    return new Version(
      String.format("%d.%d.%d-%s", version.getMajor(), version.getMinor(), version.getPatch(), version.getQualifier()), // not really needed when comparing versions
      version.getMajor(),
      version.getMinor(),
      version.getPatch(),
      version.getBuildNumber(),
      version.getQualifier());
  }

}
