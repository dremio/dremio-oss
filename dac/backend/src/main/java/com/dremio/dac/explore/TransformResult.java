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
package com.dremio.dac.explore;

import static com.dremio.common.utils.ProtostuffUtil.copy;
import static java.util.Collections.unmodifiableSet;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.dremio.dac.proto.model.dataset.VirtualDatasetState;
import com.google.common.collect.Lists;

/**
 * The result of a transformation
 */
class TransformResult {
  private final VirtualDatasetState newState;
  private final Set<String> addedColumns = new HashSet<>();
  private final Set<String> removedColumns = new HashSet<>();
  private final List<String> rowDeletionMarkerColumns = Lists.newArrayList();

  public TransformResult(
      VirtualDatasetState newState) {
    super();
    this.newState = newState;
  }
  private TransformResult(TransformResult other) {
    this.newState = other.newState;
    this.addedColumns.addAll(other.addedColumns);
    this.removedColumns.addAll(other.removedColumns);
  }
  public TransformResult added(String colName) {
    TransformResult newTr = new TransformResult(this);
    newTr.addedColumns.add(colName);
    return newTr;
  }
  public TransformResult added(List<String> columns) {
    TransformResult newTr = new TransformResult(this);
    newTr.addedColumns.addAll(columns);
    return newTr;
  }
  public TransformResult removed(String colName) {
    TransformResult newTr = new TransformResult(this);
    newTr.removedColumns.add(colName);
    return newTr;
  }
  public TransformResult modified(String colName) {
    return removed(colName).added(colName);
  }

  /**
   * Set the list of columns whose values in a row determine whether that row can be marked as deleted or not.
   * @param columns
   * @return
   */
  public TransformResult setRowDeletionMarkerColumns(List<String> columns) {
    TransformResult newTr = new TransformResult(this);
    newTr.rowDeletionMarkerColumns.addAll(columns);
    return newTr;
  }

  public VirtualDatasetState getNewState() {
    return copy(newState);
  }
  public Set<String> getAddedColumns() {
    return minus(addedColumns, removedColumns);
  }
  public Set<String> getRemovedColumns() {
    return minus(removedColumns, addedColumns);
  }
  public Set<String> getModifiedColumns() {
    return delta(removedColumns, addedColumns);
  }

  /**
   * Get the list of columns whose values in a row determine whether that row can be marked as deleted or not.
   * If any one of the columns contains a null value in a row, then that row is considered deleted.
   * @return
   */
  public List<String> getRowDeletionMarkerColumns() {
    return rowDeletionMarkerColumns;
  }

  private Set<String> minus(Set<String> a, Set<String> b) {
    Set<String> result = new HashSet<>(a);
    result.removeAll(b);
    return unmodifiableSet(result);
  }

  private Set<String> delta(Set<String> a, Set<String> b) {
    Set<String> result = new HashSet<>(a);
    result.retainAll(b);
    return unmodifiableSet(result);
  }
}
