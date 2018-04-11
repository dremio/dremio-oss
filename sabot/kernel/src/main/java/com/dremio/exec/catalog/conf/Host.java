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
package com.dremio.exec.catalog.conf;

import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.NotBlank;

import io.protostuff.Tag;

public class Host {
  //  optional string hostname = 1;
  //  optional int32 port = 2;

  @NotBlank
  @Tag(1)
  public String hostname;

  @NotNull
  @Tag(2)
  public Integer port;

  public Host(String hostname, Integer port) {
    super();
    this.hostname = hostname;
    this.port = port;
  }

  public Host() { }

  public String toCompound() {
    if(port != null) {
      return hostname + ":" + port;
    }
    return hostname;
  }

  public String toString() {
    return toCompound();
  }
}
