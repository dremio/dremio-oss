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
package com.dremio.service.reflection;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.reflection.proto.DependencyType;
import com.dremio.service.reflection.proto.ReflectionDependencyEntry;
import com.dremio.service.reflection.proto.ReflectionId;

/**
 * Reflection dependency entry used by the {@link DependencyManager}
 */
public abstract class DependencyEntry {

  public abstract DependencyType getType();

  public List<String> getPath() {
    return Collections.emptyList();
  }

  public abstract String getId();

  public abstract ReflectionDependencyEntry toProtobuf();

  public static DependencyEntry of(ReflectionDependencyEntry entry) {
    if (entry.getType() == DependencyType.REFLECTION) {
      return new ReflectionDependency(new ReflectionId(entry.getId()));
    } else if (entry.getType() == DependencyType.DATASET) {
      return new DatasetDependency(entry.getId(), entry.getPathList());
    } else if (entry.getType() == DependencyType.TABLEFUNCTION) {
      return new TableFunctionDependency(entry.getId(), entry.getSourceName(), entry.getQuery());
    }
    throw new IllegalStateException("Unsupported dependency type " + entry.getType());
  }

  public static ReflectionDependency of(ReflectionId rId) {
    return new ReflectionDependency(rId);
  }

  public static DatasetDependency of(String id, List<String> path) {
    return new DatasetDependency(id, path);
  }

  public static DependencyEntry of(String id, String sourceName, String query) {
    return new TableFunctionDependency(id, sourceName, query);
  }

  /**
   * Reflection dependency
   */
  public static class ReflectionDependency extends DependencyEntry {
    private final ReflectionId reflectionId;

    ReflectionDependency(ReflectionId reflectionId) {
      this.reflectionId = reflectionId;
    }

    public ReflectionId getReflectionId() {
      return reflectionId;
    }

    @Override
    public DependencyType getType() {
      return DependencyType.REFLECTION;
    }

    @Override
    public String getId() {
      return reflectionId.getId();
    }

    @Override
    public ReflectionDependencyEntry toProtobuf() {
      return new ReflectionDependencyEntry()
        .setType(DependencyType.REFLECTION)
        .setId(reflectionId.getId());
    }

    @Override
    public int hashCode() {
      return reflectionId.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj == null || !(obj instanceof ReflectionDependency)) {
        return false;
      }

      final ReflectionDependency dep = (ReflectionDependency) obj;
      return Objects.equals(reflectionId, dep.reflectionId);
    }

    @Override
    public String toString() {
      return "ReflectionId: " + reflectionId.getId();
    }
  }

  /**
   * Dataset dependency
   */
  public static class DatasetDependency extends DependencyEntry {
    private final String id;
    private final List<String> path;

    DatasetDependency(String id, List<String> path) {
      this.id = id;
      this.path = path;
    }

    @Override
    public DependencyType getType() {
      return DependencyType.DATASET;
    }

    public List<String> getPath() {
      return path;
    }

    public NamespaceKey getNamespaceKey() {
      return new NamespaceKey(path);
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public ReflectionDependencyEntry toProtobuf() {
      return new ReflectionDependencyEntry()
        .setType(DependencyType.DATASET)
        .setPathList(path)
        .setId(id);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, path);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj == null || !(obj instanceof DatasetDependency)) {
        return false;
      }

      final DatasetDependency dep = (DatasetDependency) obj;
      return Objects.equals(id, dep.id) && Objects.equals(path, dep.path);
    }

    @Override
    public String toString() {
      return "DatasetId: " + id + ", Path: " + path;
    }
  }

  /**
   * Table function dependency
   * a type of reflection dependency on external query
   */
  public static class TableFunctionDependency extends DependencyEntry {
    private final String id;
    private final String sourceName;
    private final String query;

    TableFunctionDependency(String id, String sourceName, String query) {
      this.id = id;
      this.sourceName = sourceName;
      this.query = query;
    }

    @Override
    public DependencyType getType() {
      return DependencyType.TABLEFUNCTION;
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public ReflectionDependencyEntry toProtobuf() {
      return new ReflectionDependencyEntry()
        .setType(DependencyType.TABLEFUNCTION)
        .setId(id)
        .setSourceName(sourceName)
        .setQuery(query);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, sourceName, query);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj == null || !(obj instanceof TableFunctionDependency)) {
        return false;
      }

      final TableFunctionDependency dep = (TableFunctionDependency) obj;
      return Objects.equals(id, dep.id) && Objects.equals(sourceName, dep.sourceName) && Objects.equals(query, dep.query);
    }

    @Override
    public String toString() {
        return "Table Function Id: " + id + ", Source: " + sourceName + ", Sql: " + query;
    }

    public String getSourceName() {
      return sourceName;
    }

    public String getQuery() {
      return query;
    }
  }

}
