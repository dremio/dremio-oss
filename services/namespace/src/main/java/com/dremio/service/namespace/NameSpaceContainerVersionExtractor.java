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
package com.dremio.service.namespace;

import com.dremio.common.AutoCloseables;
import com.dremio.datastore.VersionExtractor;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.SourceConfig;

/**
 * version extractor for namespace container.
 */
final class NameSpaceContainerVersionExtractor implements VersionExtractor<NameSpaceContainer> {
  @Override
  public String getTag(NameSpaceContainer value) {
    switch (value.getType()) {
      case DATASET:
        return value.getDataset().getTag();
      case FOLDER:
        return value.getFolder().getTag();
      case HOME:
        return value.getHome().getTag();
      case SOURCE:
        return value.getSource().getTag();
      case SPACE:
        return value.getSpace().getTag();
      case FUNCTION:
        return value.getFunction().getTag();
      default:
        throw new UnsupportedOperationException("Unknown type: " + value.getType());
    }
  }

  @Override
  public void setTag(NameSpaceContainer value, String version) {
    switch (value.getType()) {
      case DATASET:
        value.getDataset().setTag(version);
        break;
      case FOLDER:
        value.getFolder().setTag(version);
        break;
      case HOME:
        value.getHome().setTag(version);
        break;
      case SOURCE:
        value.getSource().setTag(version);
        break;
      case SPACE:
        value.getSpace().setTag(version);
        break;
      case FUNCTION:
        value.getFunction().setTag(version);
        break;
      default:
        throw new UnsupportedOperationException("Unknown type: " + value.getType());
    }
  }

  @Override
  public AutoCloseable preCommit(NameSpaceContainer value) {
    if (value.getType() != NameSpaceContainer.Type.SOURCE) {
      return AutoCloseables.noop();
    }

    // for sources, we want to maintain a numeric version so we can distinguish chronological order
    SourceConfig source = value.getSource();
    Long configOrdinal= source.getConfigOrdinal();

    value.getSource().setConfigOrdinal(configOrdinal == null ? 0 : configOrdinal + 1);

    return () -> {
      // rollback the ordinal in case the commit fails
      value.getSource().setConfigOrdinal(configOrdinal);
    };
  }

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

}
