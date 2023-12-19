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
package com.dremio.catalog.model;

public class CatalogEntityId {
  private final String id;
  public CatalogEntityId(String id) {
    this.id = id;
  }

  public static CatalogEntityId fromString(String id) {
    return new CatalogEntityId(id);
  }

  @Override
  public String toString() {
    return id;
  }


  @Override
  public final int hashCode() {
    return id.hashCode();
  }

  @Override
  public final boolean equals(final Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof CatalogEntityId)) {
      return false;
    }
    CatalogEntityId otherId = (CatalogEntityId) other;
    return otherId.id.equals(id);
  }
}
