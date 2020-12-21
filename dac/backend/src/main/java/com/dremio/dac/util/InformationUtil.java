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
package com.dremio.dac.util;

import java.net.URL;
import java.util.Properties;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import com.dremio.common.util.DremioVersionInfo;
import com.dremio.dac.model.info.CommitInfo;
import com.dremio.dac.model.info.VersionInfo;
import com.dremio.dac.service.errors.ServerErrorException;
import com.dremio.provision.ClusterType;
import com.google.common.io.Resources;

/**
   * Information Util.
 */
public class InformationUtil {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InformationUtil.class);

  public static VersionInfo getVersionInfo(ClusterType clusterType) {
    String version = DremioVersionInfo.getVersion(); // get dremio version (x.y.z)
    long buildTime = 0;
    CommitInfo commitInfo = null;

    try {
      URL u = Resources.getResource("git.properties");

      if (u != null) {
        Properties p = new Properties();
        p.load(Resources.asByteSource(u).openStream());
        buildTime = DateTime.parse(p.getProperty("git.build.time"), DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ"))
          .getMillis();
        commitInfo = new CommitInfo(
          p.getProperty("git.commit.id"),
          p.getProperty("git.build.user.email"),
          DateTime.parse(p.getProperty("git.commit.time"), DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ"))
            .getMillis());
      }
    } catch (Exception e) {
      throw(new ServerErrorException("Could not gather commit information. Check your node setup.", e));
    }
    return new VersionInfo(version, buildTime, commitInfo, String.format("%s", clusterType));
  }
}
