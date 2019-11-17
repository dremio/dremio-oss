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
package org.apache.hadoop.hbase.util;

import java.io.PrintWriter;
import org.apache.hadoop.hbase.classification.InterfaceAudience.Public;
import org.apache.hadoop.hbase.classification.InterfaceStability.Evolving;

/**
 * Overwrite the VersionInfo class from HBase with one which just hardcodes version
 * information instead of retrieving it from package-info. Package-info cannot be
 * retrieved reliably from child ClassLoaders when the parent ClassLoader defines
 * the same package.
 */
@Public
@Evolving
public class VersionInfo {
  private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(VersionInfo.class);

  public VersionInfo() {
  }

  static Package getPackage() {
    return VersionInfo.class.getPackage();
  }

  public static String getVersion() {
    return "1.1.1";
  }

  public static String getRevision() {
    return "d0a115a7267f54e01c72c603ec53e91ec418292f";
  }

  public static String getDate() {
    return "Tue Jun 23 14:56:34 PDT 2015";
  }

  public static String getUser() {
    return "ndimiduk";
  }

  public static String getUrl() {
    return "git://hw11397.local/Volumes/hbase-1.1.1RC0/hbase";
  }

  static String[] versionReport() {
    return new String[]{"HBase " + getVersion(), "Source code repository " + getUrl() + " revision=" + getRevision(), "Compiled by " + getUser() + " on " + getDate(), "From source with checksum " + getSrcChecksum()};
  }

  public static String getSrcChecksum() {
    return "6e2d8cecbd28738ad86daacb25dc467e";
  }

  public static void writeTo(PrintWriter out) {
    for (String line : versionReport()) {
      out.println(line);
    }
  }

  public static void logVersion() {
    for (String line : versionReport()) {
      LOG.info(line);
    }
  }

  public static void main(String[] args) {
    logVersion();
  }
}
