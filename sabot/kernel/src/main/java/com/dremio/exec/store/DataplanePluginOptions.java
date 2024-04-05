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
package com.dremio.exec.store;

import com.dremio.options.Options;
import com.dremio.options.TypeValidators;

/** System options for Dataplane Plugin. */
@Options
public final class DataplanePluginOptions {

  public static final TypeValidators.BooleanValidator ARCTIC_PLUGIN_ENABLED =
      new TypeValidators.BooleanValidator("plugins.arctic.enabled", false);

  public static final TypeValidators.BooleanValidator NESSIE_PLUGIN_ENABLED =
      new TypeValidators.BooleanValidator("plugins.nessie.enabled", true);

  public static final TypeValidators.BooleanValidator DATAPLANE_AZURE_STORAGE_ENABLED =
      new TypeValidators.BooleanValidator("plugins.dataplane.azure_storage.enabled", false);

  public static final TypeValidators.BooleanValidator DATAPLANE_STORAGE_SELECTION_UI_ENABLED =
      new TypeValidators.BooleanValidator("plugins.dataplane.storage_selection.ui.enabled", false);

  public static final TypeValidators.BooleanValidator DATAPLANE_LOCAL_FILE_SYSTEM_ENABLED =
      new TypeValidators.BooleanValidator("plugins.dataplane.local_file_system.enabled", false);

  private DataplanePluginOptions() {}
}
