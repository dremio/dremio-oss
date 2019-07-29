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

import javax.inject.Provider;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.google.common.annotations.VisibleForTesting;

import io.protostuff.Tag;

@SourceType(value = "MISSING", configurable = false)
public class MissingPluginConf extends ConnectionConf<MissingPluginConf, MissingStoragePlugin> {

  public static final String TYPE = "MISSING";

  @Tag(1)
  public String errorMessage;

  // Mark this plugin as bad and throw exceptions from operations. This should be set to true,
  // unless you need an active MissingPluginConf for testing purposes.
  @Tag(2)
  @VisibleForTesting
  public boolean throwOnInvocation = true;

  @Override
  public MissingStoragePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    return new MissingStoragePlugin(errorMessage, throwOnInvocation);
  }
}
