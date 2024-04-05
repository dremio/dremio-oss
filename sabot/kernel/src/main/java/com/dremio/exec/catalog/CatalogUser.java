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
package com.dremio.exec.catalog;

import java.util.Objects;

/** Catalog User identity. */
public class CatalogUser implements CatalogIdentity {
  private final String userName;

  public CatalogUser(String userName) {
    this.userName = userName;
  }

  @Override
  public String getName() {
    return userName;
  }

  public static CatalogUser from(String userName) {
    return new CatalogUser(userName);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CatalogUser that = (CatalogUser) o;
    return Objects.equals(userName, that.userName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(userName);
  }
}
