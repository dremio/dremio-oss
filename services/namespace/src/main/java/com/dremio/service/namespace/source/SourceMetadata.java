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
package com.dremio.service.namespace.source;

import com.dremio.service.namespace.source.proto.SourceConfig;

/** Metadata of a source */
public final class SourceMetadata {
  private String name;
  private String type;

  SourceMetadata(String name, String type) {
    this.name = name;
    this.type = type;
  }

  public static SourceMetadata from(SourceConfig sourceConfig) {
    return new SourceMetadata(sourceConfig.getName(), sourceConfig.getType());
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }
}
