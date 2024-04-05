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

import com.dremio.common.Version;
import com.dremio.dac.explore.bi.TableauMessageBodyGenerator;
import com.dremio.dac.proto.model.source.ClusterIdentity;
import com.dremio.dac.support.BasicSupportService;
import com.dremio.exec.server.options.OptionValidatorListingImpl;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.options.OptionValue;
import com.dremio.service.DirectProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Supplier;

/** Task to set a valid default for Tableau export that retains values set on upgrade. */
public class SetExportType extends UpgradeTask {
  // DO NOT MODIFY
  static final String taskUUID = "17bbcdda-68a1-40ee-8451-4db5958f5317";

  private static final Version VERSION_1600 = new Version("16.0.0", 16, 0, 0, 0, "");

  public SetExportType() {
    super("Set Tableau export defaults", ImmutableList.of(SetTableauDefaults.taskUUID));
  }

  @Override
  public String getTaskUUID() {
    return taskUUID;
  }

  @Override
  public void upgrade(UpgradeContext context) throws Exception {
    final Optional<ClusterIdentity> identity =
        BasicSupportService.getClusterIdentity(context.getLegacyKVStoreProvider());

    final Version previousVersion = Upgrade.retrieveStoreVersion(identity.get());

    final Supplier<SystemOptionManager> optionManagerSupplier =
        Suppliers.memoize(
            () ->
                new SystemOptionManager(
                    new OptionValidatorListingImpl(context.getScanResult()),
                    context.getLpPersistence(),
                    DirectProvider.wrap(context.getLegacyKVStoreProvider()),
                    false));

    updateOptionsIfNeeded(previousVersion, optionManagerSupplier, false);
  }

  /**
   * Add the Tableau export option to the given OptionManager. Note that if the option manager is
   * not already opened, it will lazily be initialized, opened, and closed.
   *
   * @param previousVersion The version of the KV store to check.
   * @param optionManagerSupplier The OptionManager to update. This is assumed to not be started and
   *     will be closed.
   * @param isOptionManagerOpen Indicates if the supplied option manager is already opened.
   * @return true if the Tableau export option was overridden.
   */
  @VisibleForTesting
  static boolean updateOptionsIfNeeded(
      Version previousVersion,
      Supplier<SystemOptionManager> optionManagerSupplier,
      boolean isOptionManagerOpen)
      throws Exception {
    // We write a default only when upgrading from versions older than 16.0.0.
    // The default is to keep the current setting for Tableau export for these older versions.
    // For versions newer, use the normal default for the option.
    if (Upgrade.UPGRADE_VERSION_ORDERING.compare(previousVersion, VERSION_1600) >= 0) {
      return false;
    }

    final SystemOptionManager optionManager = optionManagerSupplier.get();
    try {
      if (!isOptionManagerOpen) {
        optionManager.start();
      }

      if (!optionManager.isSet(TableauMessageBodyGenerator.TABLEAU_EXPORT_TYPE.getOptionName())) {
        final OptionValue regularTableauDefault =
            TableauMessageBodyGenerator.TABLEAU_EXPORT_TYPE.getDefault();
        final OptionValue tableauExport =
            OptionValue.createString(
                regularTableauDefault.getType(),
                regularTableauDefault.getName(),
                TableauMessageBodyGenerator.TableauExportType.ODBC
                    .toString()
                    .toLowerCase(Locale.ROOT));
        optionManager.setOption(tableauExport);
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
