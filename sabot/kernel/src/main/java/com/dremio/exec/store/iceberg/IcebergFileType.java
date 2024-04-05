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
package com.dremio.exec.store.iceberg;

import java.util.HashMap;
import java.util.Map;

public enum IcebergFileType {
  DATA(0), // Data file
  POSITION_DELETES(1),
  EQUALITY_DELETES(2),
  MANIFEST(3),
  MANIFEST_LIST(4),
  PARTITION_STATS(5), // Partition Stats file or Partition Stats metadata file
  METADATA_JSON(6),
  OTHER(7);

  public final Integer id;

  IcebergFileType(Integer id) {
    this.id = id;
  }

  private static final Map<Integer, IcebergFileType> IDS = new HashMap<>();

  static {
    for (IcebergFileType type : values()) {
      IDS.put(type.id, type);
    }
  }

  private static final Map<String, IcebergFileType> NAMES = new HashMap<>();

  static {
    for (IcebergFileType type : values()) {
      NAMES.put(type.name().toLowerCase(), type);
    }
  }

  public static IcebergFileType valueById(Integer id) {
    if (IDS.containsKey(id)) {
      return IDS.get(id);
    } else {
      throw new UnsupportedOperationException(String.format("Id %d is not recognized.", id));
    }
  }

  public static IcebergFileType valueByName(String name) {
    if (NAMES.containsKey(name.toLowerCase())) {
      return NAMES.get(name.toLowerCase());
    } else {
      throw new UnsupportedOperationException(String.format("Name %s is not recognized.", name));
    }
  }
}
