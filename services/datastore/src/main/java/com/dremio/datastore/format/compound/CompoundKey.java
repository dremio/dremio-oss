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
package com.dremio.datastore.format.compound;

import java.util.List;

/**
 * Marker interface for compound keys used.
 *
 * <p>A compound key is key composed of a list of named fields, each field having its own format.
 *
 * <p>Field name and order is significant: adding/removing a field, renaming a field, or reordering
 * fields is an incompatible change which might make it impossible to read older versions of stores
 * using the key.
 *
 * <p>Compound key instances can be represented as an immutable list of objects.
 */
public interface CompoundKey extends List<Object> {}
