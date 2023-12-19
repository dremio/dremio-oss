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
package com.dremio.common.util;

/**
 * Expose the current loaded edition of Dremio software
 */
public enum DremioEdition {
  OSS, COMMUNITY, ENTERPRISE, MARKETPLACE;

  private static final DremioEdition CURRENT;

  static {
    DremioEdition edition = DremioEdition.OSS;

    if (is("Marketplace")) {
      edition = DremioEdition.MARKETPLACE;
    } else if (is("Enterprise")) {
      edition = DremioEdition.ENTERPRISE;
    } else if (is("Community")) {
      edition = DremioEdition.COMMUNITY;
    }

    CURRENT = edition;
  }

  public static DremioEdition get() {
    return CURRENT;
  }

  public static String getAsString() {
    switch (CURRENT) {
      case OSS:
        return "OSS";
      case COMMUNITY:
        return "CE";
      case ENTERPRISE:
        return "EE";
      case MARKETPLACE:
        return "ME";
      default:
        return CURRENT.name();
    }
  }

  private static boolean is(String name) {
    try {
      Class.forName("com.dremio.edition." + name);
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
