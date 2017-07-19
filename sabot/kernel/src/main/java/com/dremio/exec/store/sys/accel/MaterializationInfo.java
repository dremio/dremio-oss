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
package com.dremio.exec.store.sys.accel;

import java.sql.Timestamp;

public class MaterializationInfo {

  public String acceleration_id;
  public String layout_id;
  public String materialization_id;
  public Timestamp create;
  public Timestamp expiration;
  public Long bytes;

  public MaterializationInfo(String acceleration_id, String layout_id, String materialization_id, Timestamp create,
      Timestamp expiration, Long bytes) {
    super();
    this.acceleration_id = acceleration_id;
    this.layout_id = layout_id;
    this.materialization_id = materialization_id;
    this.create = create;
    this.expiration = expiration;
    this.bytes = bytes;
  }

}
