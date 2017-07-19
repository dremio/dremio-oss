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
package com.dremio.dac.service.admin;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

import com.dremio.common.util.DremioVersionInfo;
import com.dremio.dac.annotations.RestResource;
import com.google.common.io.Resources;

/**
 * Returns version of server.
 */
@RestResource
@Path("/version")
public class VersionResource {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VersionResource.class);

  @GET
  public VersionInfo getVersion(){
    String version = DremioVersionInfo.getVersion(); // get dremio version (x.y.z)
    URL u = Resources.getResource("git.properties");
    String buildTime = null;
    CommitInfo commitInfo = null;
    if (u != null) {
      Properties p = new Properties();
      try {
      p.load(Resources.asByteSource(u).openStream());
      buildTime = p.getProperty("git.build.time");
      commitInfo = new CommitInfo(
          p.getProperty("git.commit.id"),
          p.getProperty("git.build.user.email"),
          p.getProperty("git.commit.time"),
          p.getProperty("git.commit.message.short"));
      } catch (IOException ex) {
        logger.warn("Failure while trying to read version information.", ex);
      }
    }

    return new VersionInfo(version, buildTime, commitInfo);
  }
}
