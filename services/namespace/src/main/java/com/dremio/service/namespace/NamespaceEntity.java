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

import com.dremio.common.Any;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.function.proto.FunctionConfig;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import java.util.List;

/** Class that includes the namespace path and container. */
final class NamespaceEntity {

  private final NamespaceInternalKey pathKey;
  private final NameSpaceContainer container;

  public NamespaceEntity(NamespaceInternalKey pathKey, NameSpaceContainer container) {
    this.pathKey = pathKey;
    this.container = container;
  }

  public NamespaceInternalKey getPathKey() {
    return pathKey;
  }

  public NameSpaceContainer getContainer() {
    return container;
  }

  /** Helper method that converts the given object into a {@link NamespaceEntity} */
  static NamespaceEntity toEntity(
      Type type, NamespaceKey path, Object config, List<Any> attributes) {
    final NameSpaceContainer container = new NameSpaceContainer();
    final NamespaceInternalKey namespaceInternalKey = new NamespaceInternalKey(path);
    container.setType(type);
    switch (type) {
      case DATASET:
        container.setDataset((DatasetConfig) config);
        break;
      case FOLDER:
        container.setFolder((FolderConfig) config);
        break;
      case HOME:
        container.setHome((HomeConfig) config);
        break;
      case SOURCE:
        container.setSource((SourceConfig) config);
        break;
      case SPACE:
        container.setSpace((SpaceConfig) config);
        break;
      case FUNCTION:
        container.setFunction((FunctionConfig) config);
        break;
      default:
        throw new UnsupportedOperationException("Unknown type: " + type);
    }
    container.setFullPathList(path.getPathComponents());
    container.setAttributesList(attributes);
    return new NamespaceEntity(namespaceInternalKey, container);
  }
}
