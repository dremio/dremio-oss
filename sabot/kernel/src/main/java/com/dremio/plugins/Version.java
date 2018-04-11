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
package com.dremio.plugins;

import static java.lang.String.format;

/**
 * Version
 */
public class Version implements Comparable<Version> {
  private final int major;
  private final int minor;
  private final int patch;

  public Version(int major, int minor, int patch) {
    super();
    this.major = major;
    this.minor = minor;
    this.patch = patch;
  }

  public int getMajor() {
    return major;
  }

  public int getMinor() {
    return minor;
  }

  public int getPatch() {
    return patch;
  }

  public static Version parse(String s) {
    String[] components = s.split("\\.");

    int major = 0, minor = 0, patch = 0;
    switch(components.length) {
      case 3:
        patch = Integer.parseInt(components[2]);
      case 2:
        minor = Integer.parseInt(components[1]);
      case 1:
        major = Integer.parseInt(components[0]);
        break;
      default:
        throw new IllegalArgumentException(format("Cannot parse version %s", s));
    }

    return new Version(major, minor, patch);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + major;
    result = prime * result + minor;
    result = prime * result + patch;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Version other = (Version) obj;
    if (major != other.major) {
      return false;
    }
    if (minor != other.minor) {
      return false;
    }
    if (patch != other.patch) {
      return false;
    }
    return true;
  }

  @Override
  public int compareTo(Version other) {
    int result = Integer.compare(major, other.major);
    if (result == 0) {
      result = Integer.compare(minor, other.minor);
    }
    if (result == 0) {
      result = Integer.compare(patch, other.patch);
    }
    return result;
  }

  @Override
  public String toString() {
    return format("%d.%d.%d", major, minor, patch);
  }
}
