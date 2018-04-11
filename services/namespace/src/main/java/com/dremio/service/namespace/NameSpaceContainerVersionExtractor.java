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
package com.dremio.service.namespace;

import com.dremio.datastore.VersionExtractor;
import com.dremio.service.namespace.proto.NameSpaceContainer;

/**
 * version extractor for namespace container.
 */
final class NameSpaceContainerVersionExtractor implements VersionExtractor<NameSpaceContainer> {

  @Override
  public Long getVersion(NameSpaceContainer value) {
    switch (value.getType()) {
      case DATASET:
        return value.getDataset().getVersion();
      case FOLDER:
        return value.getFolder().getVersion();
      case HOME:
        return value.getHome().getVersion();
      case SOURCE:
        return value.getSource().getVersion();
      case SPACE:
        return value.getSpace().getVersion();
      default:
        throw new UnsupportedOperationException("Unknown type: " + value.getType());
    }
  }

  @Override
  public void setVersion(NameSpaceContainer value, Long version) {
    switch (value.getType()) {
      case DATASET:
        value.getDataset().setVersion(version);
        break;
      case FOLDER:
        value.getFolder().setVersion(version);
        break;
      case HOME:
        value.getHome().setVersion(version);
        break;

      case SOURCE:
        value.getSource().setVersion(version);
        break;
      case SPACE:
        value.getSpace().setVersion(version);
        break;
      default:
        throw new UnsupportedOperationException("Unknown type: " + value.getType());
    }
  }

  @Override
  public Long incrementVersion(NameSpaceContainer value) {
    Long version = getVersion(value);
    setVersion(value, version == null ? 0 : version + 1);
    return version;
  }
}
