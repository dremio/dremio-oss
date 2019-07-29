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
package com.dremio.plugins.adl.store;

import com.dremio.options.Options;
import com.dremio.options.TypeValidators;

/**
 * System Options for Azure Storage
 */
@Options
public class AzureDataLakeOptions {

  // If enabled, use the asynchronous interface for files. If disabled, use the hadoop file interface.
  public static final TypeValidators.BooleanValidator ASYNC_READS = new TypeValidators.BooleanValidator("store.adl.async", true);
}
