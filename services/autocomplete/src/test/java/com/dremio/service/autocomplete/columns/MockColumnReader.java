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
package com.dremio.service.autocomplete.columns;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.arrow.util.Preconditions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Mocking of the schema reader.
 */
public final class MockColumnReader implements ColumnReader {
  private final ImmutableMap<ImmutableList<String>, ImmutableSet<Column>> schemas;

  public MockColumnReader(ImmutableMap<ImmutableList<String>, ImmutableSet<Column>> schemas) {
    Preconditions.checkNotNull(schemas);
    this.schemas = schemas;
  }

  @Override
  public Optional<Set<Column>> getColumnsForTableWithName(List<String> qualifiedName) {
    Set<Column> columns = schemas.get(qualifiedName);
    if(columns == null) {
      return Optional.empty();
    }

    return Optional.of(columns);
  }
}
