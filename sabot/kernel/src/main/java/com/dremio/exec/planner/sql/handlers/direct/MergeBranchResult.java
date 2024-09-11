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
package com.dremio.exec.planner.sql.handlers.direct;

public final class MergeBranchResult {
  // names of these fields will be used as column names in the MERGE BRANCH result set
  public final String message;
  public final String contentName;
  public final String status;

  public MergeBranchResult(String message, String contentName, String status) {
    this.message = message;
    this.contentName = contentName;
    this.status = status;
  }
}
