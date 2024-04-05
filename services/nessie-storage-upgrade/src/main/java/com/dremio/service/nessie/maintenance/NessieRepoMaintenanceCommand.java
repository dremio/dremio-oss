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
package com.dremio.service.nessie.maintenance;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.dremio.dac.cmd.AdminCommand;
import com.dremio.dac.cmd.AdminLogger;
import com.dremio.dac.cmd.CmdUtils;
import com.dremio.dac.server.DACConfig;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.io.file.Path;
import com.dremio.service.embedded.catalog.EmbeddedPointerStore;
import com.dremio.service.namespace.NamespaceServiceImpl.NamespaceStoreCreator;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.google.common.annotations.VisibleForTesting;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.projectnessie.model.ContentKey;

/**
 * Admin CLI tool to invoking Embedded Nessie maintenance operations and performing basic repository
 * sanity checks.
 */
@AdminCommand(
    value = "embedded-catalog-maintenance",
    description = "Runs Embedded Catalog maintenance tasks.")
public class NessieRepoMaintenanceCommand {
  @Parameters(separators = "=")
  static final class Options {
    @Parameter(
        names = {"-h", "--help"},
        description = "Show usage and exit.",
        help = true)
    private boolean help;

    @Parameter(
        names = {"--list-keys"},
        description = "List live keys without performing any maintenance.")
    private boolean listKeys;

    @Parameter(
        names = {"--list-obsolete-internal-keys"},
        description =
            "List keys in the dremio.internal namespace, that do not have associated datasets.")
    private boolean listObsoleteInternalKeys;

    public static Options parse(String[] cliArgs) {
      Options args = new Options();
      JCommander jc = JCommander.newBuilder().addObject(args).build();
      jc.setProgramName("dremio-admin nessie-maintenance");

      try {
        jc.parse(cliArgs);
      } catch (ParameterException p) {
        AdminLogger.log(p.getMessage());
        jc.usage();
        System.exit(1);
      }

      if (args.help) {
        jc.usage();
      }
      return args;
    }
  }

  public static void main(String[] args) throws Exception {
    Options options = Options.parse(args);

    if (options.help) {
      return; // the help message is printed by high-level code
    }

    final DACConfig dacConfig = DACConfig.newConfig();

    if (!dacConfig.isMaster) {
      throw new UnsupportedOperationException("Nessie maintenance should be run on master nodes");
    }

    Optional<LocalKVStoreProvider> providerOptional =
        CmdUtils.getKVStoreProvider(dacConfig.getConfig());
    if (!providerOptional.isPresent()) {
      AdminLogger.log("No KVStore detected.");
      return;
    }

    try (LocalKVStoreProvider kvStore = providerOptional.get()) {
      kvStore.start();

      execute(kvStore, options);
    }
  }

  @VisibleForTesting
  static void execute(LocalKVStoreProvider kvStore, Options options) throws Exception {
    execute(kvStore, options, AdminLogger::log);
  }

  @VisibleForTesting
  static void execute(LocalKVStoreProvider kvStore, Options options, LocalLogger logger)
      throws Exception {
    EmbeddedPointerStore store = new EmbeddedPointerStore(kvStore);

    if (options.listKeys) {
      listKeys(store, logger);
    } else if (options.listObsoleteInternalKeys) {
      listObsoleteMetadataKeys(kvStore, store, logger);
    }
  }

  private static void listKeys(EmbeddedPointerStore store, LocalLogger logger) {
    store
        .findAll()
        .forEach(
            doc -> {
              logger.log("{}", doc.getKey());
            });
  }

  private static void listObsoleteMetadataKeys(
      LocalKVStoreProvider kvStore, EmbeddedPointerStore store, LocalLogger logger) {
    IndexedStore<String, NameSpaceContainer> namespace =
        kvStore.getStore(NamespaceStoreCreator.class);
    Iterable<Document<String, NameSpaceContainer>> docs = namespace.find();
    Set<UUID> liveTableIds = new HashSet<>();
    for (Document<String, NameSpaceContainer> doc : docs) {
      NameSpaceContainer container = doc.getValue();
      if (container.getType() != NameSpaceContainer.Type.DATASET) {
        continue;
      }

      DatasetConfig ds = container.getDataset();
      PhysicalDataset pds = ds.getPhysicalDataset();
      if (pds == null) {
        continue;
      }

      IcebergMetadata icebergMetadata = pds.getIcebergMetadata();
      if (icebergMetadata == null || icebergMetadata.getTableUuid() == null) {
        continue;
      }

      UUID uuid = UUID.fromString(icebergMetadata.getTableUuid());
      liveTableIds.add(uuid);
    }

    // Process only Infinite Splits keys, which have two components.
    // For example (the dot in "dremio.internal" is not a separator):
    // dremio.internal./data/pdfs/metadata/11c3b76c-e355-4dee-8b96-5e704361f49f
    store
        .findAll()
        .forEach(
            doc -> {
              ContentKey key = doc.getKey();
              if (key.getElements().size() == 2
                  && "dremio.internal".equals(key.getElements().get(0))) {
                String pathName = key.getElements().get(1);
                String id = Path.of(pathName).getName(); // the last path element is the table UUID
                if (!liveTableIds.remove(UUID.fromString(id))) {
                  // Note: Avoid zero bytes that can be produced by key.toPathString()
                  // Note: The '|' char is not used in dremio.internal keys
                  logger.log("{}", String.join("|", key.getElements()));
                }
              }
            });

    for (UUID id : liveTableIds) {
      logger.log("Live metadata table ID: {} does not have a corresponding Nessie key", id);
    }

    if (!liveTableIds.isEmpty()) {
      throw new IllegalStateException("Keys for some table IDs were not found.");
    }
  }

  @FunctionalInterface
  interface LocalLogger {
    void log(String msg, Object... args);
  }
}
