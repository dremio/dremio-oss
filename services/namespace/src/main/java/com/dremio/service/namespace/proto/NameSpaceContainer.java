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
package com.dremio.service.namespace.proto;

import java.util.List;

import com.dremio.service.namespace.function.proto.FunctionConfig;
import com.google.protobuf.ByteString;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;

/**
 * NameSpaceContainer POJO that wraps the protostuff version
 */
public class NameSpaceContainer {
  /**
   * NameSpaceContainer type
   */
  public enum Type {
    SPACE {
      @Override
      public int getNumber() {
        return com.dremio.service.namespace.protostuff.NameSpaceContainer.Type.SPACE.getNumber();
      }
    },

    SOURCE {
      @Override
      public int getNumber() {
        return com.dremio.service.namespace.protostuff.NameSpaceContainer.Type.SOURCE.getNumber();
      }
    },

    HOME {
      @Override
      public int getNumber() {
        return com.dremio.service.namespace.protostuff.NameSpaceContainer.Type.HOME.getNumber();
      }
    },

    FOLDER {
      @Override
      public int getNumber() {
        return com.dremio.service.namespace.protostuff.NameSpaceContainer.Type.FOLDER.getNumber();
      }
    },

    DATASET {
      @Override
      public int getNumber() {
        return com.dremio.service.namespace.protostuff.NameSpaceContainer.Type.DATASET.getNumber();
      }
    },

    FUNCTION {
      @Override
      public int getNumber() {
        return com.dremio.service.namespace.protostuff.NameSpaceContainer.Type.FUNCTION.getNumber();
      }
    };

    /**
     * Returns the underlying numeric representation
     *
     * @return
     */
    public abstract int getNumber();
  }

  private final com.dremio.service.namespace.protostuff.NameSpaceContainer delegate;

  public NameSpaceContainer() {
    this(com.dremio.service.namespace.protostuff.NameSpaceContainer.getSchema().newMessage());
  }

  public NameSpaceContainer(com.dremio.service.namespace.protostuff.NameSpaceContainer delegate) {
    this.delegate = delegate;
  }

  public List<String> getFullPathList() {
    return delegate.getFullPathList();
  }

  public NameSpaceContainer setFullPathList(List<String> fullPath) {
    delegate.setFullPathList(fullPath);
    return this;
  }

  public Type getType() {
    switch (delegate.getType()) {
      case SPACE:
        return Type.SPACE;
      case SOURCE:
        return Type.SOURCE;
      case HOME:
        return Type.HOME;
      case FOLDER:
        return Type.FOLDER;
      case DATASET:
        return Type.DATASET;
      case FUNCTION:
        return Type.FUNCTION;
      default:
        throw new RuntimeException("bad type");
    }
  }

  public NameSpaceContainer setType(NameSpaceContainer.Type type) {
    switch (type) {
      case SPACE:
        delegate.setType(com.dremio.service.namespace.protostuff.NameSpaceContainer.Type.SPACE);
        break;
      case SOURCE:
        delegate.setType(com.dremio.service.namespace.protostuff.NameSpaceContainer.Type.SOURCE);
        break;
      case HOME:
        delegate.setType(com.dremio.service.namespace.protostuff.NameSpaceContainer.Type.HOME);
        break;
      case FOLDER:
        delegate.setType(com.dremio.service.namespace.protostuff.NameSpaceContainer.Type.FOLDER);
        break;
      case DATASET:
        delegate.setType(com.dremio.service.namespace.protostuff.NameSpaceContainer.Type.DATASET);
        break;
      case FUNCTION:
        delegate.setType(com.dremio.service.namespace.protostuff.NameSpaceContainer.Type.FUNCTION);
        break;
      default:
        throw new RuntimeException("bad type");
    }

    return this;
  }

  public com.dremio.service.namespace.source.proto.SourceConfig getSource() {
    return delegate.getSource();
  }

  public NameSpaceContainer setSource(com.dremio.service.namespace.source.proto.SourceConfig source) {
    delegate.setSource(source);
    return this;
  }

  public com.dremio.service.namespace.space.proto.SpaceConfig getSpace() {
    return delegate.getSpace();
  }

  public NameSpaceContainer setSpace(com.dremio.service.namespace.space.proto.SpaceConfig space) {
    delegate.setSpace(space);
    return this;
  }

  public FunctionConfig getFunction() {return delegate.getFunction(); }

  public NameSpaceContainer setFunction(FunctionConfig udf) {
    delegate.setFunction(udf);
    return this;
  }

  public com.dremio.service.namespace.space.proto.FolderConfig getFolder() {
    return delegate.getFolder();
  }

  public NameSpaceContainer setFolder(com.dremio.service.namespace.space.proto.FolderConfig folder) {
    delegate.setFolder(folder);
    return this;
  }

  public com.dremio.service.namespace.dataset.proto.DatasetConfig getDataset() {
    return delegate.getDataset();
  }

  public NameSpaceContainer setDataset(com.dremio.service.namespace.dataset.proto.DatasetConfig dataset) {
    delegate.setDataset(dataset);
    return this;
  }

  public com.dremio.service.namespace.space.proto.HomeConfig getHome() {
    return delegate.getHome();
  }

  public NameSpaceContainer setHome(com.dremio.service.namespace.space.proto.HomeConfig home) {
    delegate.setHome(home);
    return this;
  }

  public List<com.dremio.common.Any> getAttributesList() {
    return delegate.getAttributesList();
  }

  public NameSpaceContainer setAttributesList(List<com.dremio.common.Any> attributes) {
    delegate.setAttributesList(attributes);
    return this;
  }

  public int getVersion() {
    return delegate.getVersion();
  }

  public void setVersion(int version) {
    delegate.setVersion(version);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || this.getClass() != obj.getClass()) {
      return false;
    }
    final NameSpaceContainer that = (NameSpaceContainer) obj;
    return that.toProtoStuff().equals(this.toProtoStuff());
  }

  @Override
  public int hashCode() {
    return delegate.hashCode();
  }

  /**
   * Returns a protostuff representation
   *
   * @return
   */
  public com.dremio.service.namespace.protostuff.NameSpaceContainer toProtoStuff() {
    return delegate;
  }

  public ByteString clone(LinkedBuffer buffer) {
    // TODO(DX-10857): change from opaque object to protobuf; avoid unnecessary copies
    return ByteString.copyFrom(ProtobufIOUtil.toByteArray(delegate, com.dremio.service.namespace.protostuff.NameSpaceContainer.getSchema(), buffer));
  }

  /**
   * Create a NameSpaceContainer from a protostuff ByteString
   *
   * @param bytes a ByteString representation a NameSpaceContainer
   * @return
   */
  public static NameSpaceContainer from(ByteString bytes) {
    com.dremio.service.namespace.protostuff.NameSpaceContainer container = com.dremio.service.namespace.protostuff.NameSpaceContainer.getSchema().newMessage();
    ProtobufIOUtil.mergeFrom(bytes.toByteArray(), container, container.getSchema());
    return new NameSpaceContainer(container);
  }
}
