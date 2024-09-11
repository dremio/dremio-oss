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
package com.dremio.exec.store.iceberg.dremioudf.core.udf;

import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfVersion;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * A version of the UDF at a point in time.
 *
 * <p>A version consists of a UDF metadata file.
 *
 * <p>Versions are created by UDF operations, like Create and Replace.
 */
@Value.Immutable
@SuppressWarnings("ImmutablesStyle")
@Value.Style(
    typeImmutable = "ImmutableUdfVersion",
    visibilityString = "PUBLIC",
    builderVisibilityString = "PUBLIC")
interface BaseUdfVersion extends UdfVersion {

  @Override
  @Nullable
  String defaultCatalog();
}
