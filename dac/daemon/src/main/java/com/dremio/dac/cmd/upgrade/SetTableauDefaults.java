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
package com.dremio.dac.cmd.upgrade;

import java.util.Optional;
import java.util.function.Supplier;

import com.dremio.common.Version;
import com.dremio.dac.proto.model.source.ClusterIdentity;
import com.dremio.dac.resource.TableauResource;
import com.dremio.dac.support.BasicSupportService;
import com.dremio.exec.server.options.OptionValidatorListingImpl;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.options.OptionValue;
import com.dremio.service.DirectProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

/**
 * Task to set a valid default for Tableau that varies based on version.
 */
public class SetTableauDefaults extends UpgradeTask {
  //DO NOT MODIFY
  static final String taskUUID = "67780ff3-80e1-4d40-88f1-cb7139b5c3de";

  private static final Version VERSION_460 = new Version("4.6.0", 4, 6, 0, 0, "");

  public SetTableauDefaults() {
    super("Set Tableau client defaults", ImmutableList.of(DeleteSysMaterializationsMetadata.taskUUID));
  }

  @Override
  public String getTaskUUID() {
    return taskUUID;
  }

  @Override
  public void upgrade(UpgradeContext context) throws Exception {
    final Optional<ClusterIdentity> identity =
      BasicSupportService.getClusterIdentity(context.getKVStoreProvider());

    final Version previousVersion = Upgrade.retrieveStoreVersion(identity.get());

    final Supplier<SystemOptionManager> optionManagerSupplier = Suppliers.memoize(() ->
      new SystemOptionManager(new OptionValidatorListingImpl(context.getScanResult()),
        context.getLpPersistence(),
        DirectProvider.wrap(context.getKVStoreProvider()),
        false));

    updateOptionsIfNeeded(previousVersion, optionManagerSupplier, false);
  }

  /**
   * Add the Tableau option to the given OptionManager. Note that if the option manager is
   * not already opened, it will lazily be initialized, opened, and closed.
   *
   * @param previousVersion         The version of the KV store to check.
   * @param optionManagerSupplier   The OptionManager to update. This is assumed to not be started and will be closed.
   * @param isOptionManagerOpen     Indicates if the supplied option manager is already opened.
   * @return true if the Tableau option was overridden.
   */
  @VisibleForTesting
  static boolean updateOptionsIfNeeded(Version previousVersion, Supplier<SystemOptionManager> optionManagerSupplier,
                                       boolean isOptionManagerOpen) throws Exception {
    // We write a default only when upgrading from versions older than 4.6.0.
    // The default is to enable Tableau for these older versions.
    // For versions newer, use the normal default for the option.
    if (Upgrade.UPGRADE_VERSION_ORDERING.compare(previousVersion, VERSION_460) >= 0) {
      return false;
    }

    final SystemOptionManager optionManager = optionManagerSupplier.get();
    try {
      if (!isOptionManagerOpen) {
        optionManager.start();
      }

      final OptionValue regularTableauDefault = TableauResource.CLIENT_TOOLS_TABLEAU.getDefault();
      final OptionValue tableauEnable = OptionValue.createBoolean(regularTableauDefault.getType(),
        regularTableauDefault.getName(), false);

      if (!optionManager.isSet(TableauResource.CLIENT_TOOLS_TABLEAU.getOptionName())) {
        optionManager.setOption(tableauEnable);
        return true;
      }
      return false;
    } finally {
      if (!isOptionManagerOpen) {
        optionManager.close();
      }
    }
  }
}
