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
package com.dremio.plugins;

import javax.annotation.Nullable;

public class NessieViewAdapter {
  private final int currentVersionId;
  private final int currentSchemaId;
  @Nullable private final String sql;

  public NessieViewAdapter(int currentVersionId, int currentSchemaId, @Nullable String sql) {
    this.currentVersionId = currentVersionId;
    this.currentSchemaId = currentSchemaId;
    this.sql = sql;
  }

  public int getCurrentVersionId() {
    return currentVersionId;
  }

  public int getCurrentSchemaId() {
    return currentSchemaId;
  }

  public String getSql() {
    return sql;
  }
}
