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
package com.dremio.dac.service.catalog;

import com.dremio.dac.api.CatalogItem;
import com.dremio.dac.api.CatalogPageToken;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/** Result of listing children under a path. */
@Value.Immutable
public interface CatalogListingResult {
  /** Children of an entity (i.e. Folder, Space, Source). */
  List<CatalogItem> children();

  /** Token that can be passed in subsequent requests. */
  Optional<CatalogPageToken> nextPageToken();

  /** Maximum number of children parameter that was used. */
  int maxChildren();

  @Nullable
  default String getApiNextPageToken() {
    return nextPageToken().isPresent() ? nextPageToken().get().toApiToken() : null;
  }

  static ImmutableCatalogListingResult.Builder builder() {
    return new ImmutableCatalogListingResult.Builder();
  }

  static CatalogListingResult empty() {
    return builder().setMaxChildren(0).build();
  }
}
