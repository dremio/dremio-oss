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

import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfHistoryEntry;
import org.immutables.value.Value;

/**
 * UDF history entry.
 *
 * <p>An entry contains a change to the UDF state. At the given timestamp, the current version was
 * set to the given version ID.
 */
@Value.Immutable
@SuppressWarnings("ImmutablesStyle")
@Value.Style(
    typeImmutable = "ImmutableUdfHistoryEntry",
    visibilityString = "PUBLIC",
    builderVisibilityString = "PUBLIC")
interface BaseUdfHistoryEntry extends UdfHistoryEntry {}
