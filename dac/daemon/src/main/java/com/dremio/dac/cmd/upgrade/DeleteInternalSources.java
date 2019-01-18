/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.dac.cmd.upgrade;

import java.util.List;

import com.dremio.common.Version;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.google.common.collect.ImmutableList;

/**
 * Before 1.5, config of internal sources were null.  Delete those to have Dremio recreate them.
 */
public class DeleteInternalSources extends UpgradeTask implements LegacyUpgradeTask {

  //DO NOT MODIFY
  static final String taskUUID = "69d0202a-ad32-4128-82d6-e0341b0f0151";

  public DeleteInternalSources() {
    super("Deleting internal sources", ImmutableList.of(MoveFromAccelerationsToReflections.taskUUID));
  }

  @Override
  public Version getMaxVersion() {
    return VERSION_150;
  }

  @Override
  public String getTaskUUID() {
    return taskUUID;
  }

  @Override
  public void upgrade(UpgradeContext context) throws Exception {
    final NamespaceService namespaceService = new NamespaceServiceImpl(context.getKVStoreProvider());

    List<SourceConfig> sources = namespaceService.getSources();

    for (SourceConfig sourceConfig : sources) {
      // Pre-1.5, the config object for internal sources was null.  Deleting the internal sources without a config will
      // ensure that they get recreated on startup.
      if (sourceConfig.getConfig() == null) {
        System.out.printf("  deleting '%s'%n", sourceConfig.getName());
        // following may throw an exception, we let it propagate to fail the upgrade
        namespaceService.deleteSource(sourceConfig.getKey(), sourceConfig.getTag());
      }
    }
  }

  @Override
  public String toString() {
    return String.format("'%s' up to %s)", getDescription(), getMaxVersion());
  }
}
