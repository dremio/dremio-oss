/*
 * Copyright (C) 2017 Dremio Corporation
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

public class SimpleCommandResult {

  public boolean ok;
  public String summary;

  public SimpleCommandResult(boolean ok, String summary) {
    super();
    this.ok = ok;
    this.summary = summary;
  }

  public static SimpleCommandResult successful(String msg, Object...objects ) {
    return successful(String.format(msg, objects));
  }

  public static SimpleCommandResult successful(String msg) {
    return new SimpleCommandResult(true, msg);
  }
}
