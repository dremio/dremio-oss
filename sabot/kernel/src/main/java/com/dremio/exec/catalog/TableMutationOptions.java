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
package com.dremio.exec.catalog;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableTableMutationOptions.class)
@JsonDeserialize(as = ImmutableTableMutationOptions.class)
public abstract class TableMutationOptions {

  @Nullable
  public abstract ResolvedVersionContext getResolvedVersionContext();

  @Value.Default
  public boolean isLayered() {
    return false;
  }

  /**
   * Create a new builder.
   *
   * @return new builder
   */
  public static ImmutableTableMutationOptions.Builder newBuilder() {
    return new ImmutableTableMutationOptions.Builder();
  }

  @Value.Default
  public boolean shouldDeleteCatalogEntry() {
    return false;
  }
}
