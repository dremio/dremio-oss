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

<@pp.dropOutputFile />

<@pp.changeOutputFile name="/com/dremio/common/util/DremioVersionInfo.java" />

<#include "/@includes/license.ftl" />

package com.dremio.common.util;

import com.dremio.common.Version;
/*
 * This file is generated with Freemarker using the template src/main/codegen/templates/DremioVersionInfo.java
 */
/**
 * Give access to Dremio version as captured during the build
 *
 * <strong>Caution</strong> don't rely on major, minor and patch versions only to compare two 
 * Dremio versions. Instead you should use the whole string, and apply the same semver algorithm 
 * as Maven (see {@code org.apache.maven.artifact.versioning.ComparableVersion}).
 *
 */
public class DremioVersionInfo {
  /**
   * The version extracted from Maven POM file at build time.
   */
  public static final Version VERSION = new Version(
      "${maven.project.version}",
      ${maven.project.artifact.selectedVersion.majorVersion},
      ${maven.project.artifact.selectedVersion.minorVersion},
      ${maven.project.artifact.selectedVersion.incrementalVersion},
      ${maven.project.artifact.selectedVersion.buildNumber},
      "${maven.project.artifact.selectedVersion.qualifier!}"
  );

  /**
   * Get the Dremio version from pom
   * @return the version number as x.y.z
   */
  public static String getVersion() {
    return VERSION.getVersion();
  }

  /**
   *  Get the Dremio major version from pom
   *  @return x if assuming the version number is x.y.z
   */
  public static int getMajorVersion() {
    return VERSION.getMajorVersion();
  }

  /**
   *  Get the Dremio minor version from pom
   *  @return y if assuming the version number is x.y.z
   */
  public static int getMinorVersion() {
    return VERSION.getMinorVersion();
  }

  /**
   *  Get the Dremio patch version from pom
   *  @return z if assuming the version number is x.y.z(-suffix)
   */
  public static int getPatchVersion() {
    return VERSION.getPatchVersion();
  }

  /**
   *  Get the Dremio build number from pom
   *  @return z if assuming the version number is x.y.z(.b)(-suffix)
   */
  public static int getBuildNumber() {
    return VERSION.getPatchVersion();
  }

  /**
   *  Get the Dremio version qualifier from pom
   *  @return suffix if assuming the version number is x.y.z(-suffix), or an empty string
   */
  public static String getQualifier() {
    return VERSION.getQualifier();
   }
}

