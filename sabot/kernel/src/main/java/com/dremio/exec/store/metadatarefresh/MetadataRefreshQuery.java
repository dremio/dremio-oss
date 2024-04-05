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
package com.dremio.exec.store.metadatarefresh;

/** Holds objects related to refresh query. */
public class MetadataRefreshQuery {
  private final String querySQL;
  private final String user;

  public MetadataRefreshQuery(String querySQL, String user) {
    this.querySQL = querySQL;
    this.user = user;
  }

  public String getQuery() {
    return querySQL;
  }

  public String getUser() {
    return user;
  }
}
