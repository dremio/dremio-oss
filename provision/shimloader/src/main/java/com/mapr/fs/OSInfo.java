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
package com.mapr.fs;

/**
 * OSInfo
 *
 * <p>Included for compiling ShimLoader
 */
public final class OSInfo {
  private OSInfo() {
    // utility class
  }

  public static void main(String[] args) {
    if (args.length >= 1) {
      if ("--os".equals(args[0])) {
        System.out.print(getOSName());
        return;
      }

      if ("--arch".equals(args[0])) {
        System.out.print(getArchName());
        return;
      }
    }

    System.out.print(getNativeLibFolderPathForCurrentOS());
  }

  public static String getNativeLibFolderPathForCurrentOS() {
    return getOSName() + "/" + getArchName();
  }

  public static String getOSName() {
    return translateOSNameToFolderName(System.getProperty("os.name"));
  }

  public static String getArchName() {
    String osArch = System.getProperty("os.arch");
    if ("Mac".equals(getOSName()) && ("universal".equals(osArch) || "amd64".equals(osArch))) {
      return "x86_64";
    }
    if ("amd64".equals(osArch)) {
      return "x86_64";
    }
    if ("x86".equals(osArch)) {
      return "i386";
    }
    return translateArchNameToFolderName(osArch);
  }

  static String translateOSNameToFolderName(String osName) {
    if (osName.contains("Windows")) {
      return "Windows";
    } else if (osName.contains("Mac")) {
      return "Mac";
    } else {
      return osName.contains("Linux") ? "Linux" : osName.replaceAll("\\W", "");
    }
  }

  static String translateArchNameToFolderName(String archName) {
    return archName.replaceAll("\\W", "");
  }
}
