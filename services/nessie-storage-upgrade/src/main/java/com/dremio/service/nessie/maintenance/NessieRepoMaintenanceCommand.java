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

import java.time.Instant;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;
import org.projectnessie.versioned.persist.nontx.ImmutableAdjustableNonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapterConfig;

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
import com.dremio.service.namespace.NamespaceServiceImpl.NamespaceStoreCreator;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.nessie.DatastoreDatabaseAdapterFactory;
import com.dremio.service.nessie.EmbeddedRepoMaintenanceParams;
import com.dremio.service.nessie.EmbeddedRepoPurgeParams;
import com.dremio.service.nessie.ImmutableDatastoreDbConfig;
import com.dremio.service.nessie.ImmutableEmbeddedRepoMaintenanceParams;
import com.dremio.service.nessie.NessieDatastoreInstance;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;

/**
 * Admin CLI tool to invoking Embedded Nessie maintenance operations and performing basic repository sanity checks.
 */
@AdminCommand(value = "nessie-maintenance", description = "Runs Embedded Nessie repository maintenance tasks.")
public class NessieRepoMaintenanceCommand {
  @Parameters(separators = "=")
  static final class Options {
    @Parameter(names = {"-h", "--help"}, description = "Show usage and exit.", help = true)
    private boolean help;

    @Parameter(names = {"--purge-key-lists"},
      description = "Purge obsolete key lists.")
    private boolean purgeKeyLists;

    @Parameter(names = {"--compact-global-log"},
      description = "Compact Global Log (legacy data).")
    private boolean compactGlobalLog;

    @Parameter(names = {"--dry-run"},
      description = "Do not make any changes during repository maintenance activities. " +
        "This is useful for estimating the amount of maintenance work that would be performed.")
    private boolean dryRun;

    @Parameter(names = {"--progress"},
      description = "Report process every Nth maintenance event, such as deleting an entity (ignored if not positive).")
    private int progress;

    @Parameter(names = {"--list-keys"},
      description = "List live keys without performing any maintenance.")
    private boolean listKeys;

    @Parameter(names = {"--list-live-commits"},
      description = "List live keys with associated commit hashes and timestamps without performing any maintenance.")
    private boolean listLiveCommits;

    @Parameter(names = {"--list-obsolete-internal-keys"},
      description = "List keys in the dremio.internal namespace, that do not have associated datasets.")
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

    Optional<LocalKVStoreProvider> providerOptional = CmdUtils.getKVStoreProvider(dacConfig.getConfig());
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
  static String execute(LocalKVStoreProvider kvStore, Options options) throws Exception {
    return execute(kvStore, options, AdminLogger::log);
  }

  @VisibleForTesting
  static String execute(LocalKVStoreProvider kvStore, Options options, LocalLogger logger) throws Exception {
    NonTransactionalDatabaseAdapterConfig adapterCfg = ImmutableAdjustableNonTransactionalDatabaseAdapterConfig
      .builder()
      .validateNamespaces(false)
      .build();
    NessieDatastoreInstance store = new NessieDatastoreInstance();
    store.configure(new ImmutableDatastoreDbConfig.Builder().setStoreProvider(() -> kvStore).build());
    store.initialize();
    DatabaseAdapter adapter = new DatastoreDatabaseAdapterFactory().newBuilder()
      .withConnector(store)
      .withConfig(adapterCfg)
      .build();

    if (options.listKeys) {
      listKeys(adapter, logger);
      return "";
    } else if (options.listLiveCommits) {
      listLiveCommits(adapter, logger);
      return "";
    } else if (options.listObsoleteInternalKeys) {
      listObsoleteMetadataKeys(kvStore, adapter, logger);
      return "";
    } else if (options.purgeKeyLists || options.compactGlobalLog) {
      return executeMaintenance(adapter, options);
    }

    return null;
  }

  private static String executeMaintenance(DatabaseAdapter adapter, Options options) throws Exception {
    ImmutableEmbeddedRepoMaintenanceParams.Builder params = EmbeddedRepoMaintenanceParams.builder();

    if (options.purgeKeyLists) {
      params.setEmbeddedRepoPurgeParams(EmbeddedRepoPurgeParams.builder()
        .setDryRun(options.dryRun)
        .setProgressReporter(new ProgressReporter(options.progress))
        .build());
    }

    Map<String, Map<String, String>> result = adapter.repoMaintenance(params.build());

    // Pretty print maintenance output to the log as JSON
    ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();
    String stringResult = writer.writeValueAsString(result);

    AdminLogger.log("Completed Nessie repository maintenance with output: {}", stringResult);
    return stringResult;
  }

  private static void listKeys(DatabaseAdapter adapter, LocalLogger logger) throws Exception {
    ReferenceInfo<ByteString> main = adapter.namedRef("main", GetNamedRefsParams.DEFAULT);
    try (Stream<KeyListEntry> keys = adapter.keys(main.getHash(), KeyFilterPredicate.ALLOW_ALL)) {
      keys.forEach(keyWithType -> logger.log("{}", keyWithType.getKey()));
    }
  }

  private static void listLiveCommits(DatabaseAdapter adapter, LocalLogger logger) throws Exception {
    ReferenceInfo<ByteString> main = adapter.namedRef("main", GetNamedRefsParams.DEFAULT);
    try (Stream<KeyListEntry> keys = adapter.keys(main.getHash(), KeyFilterPredicate.ALLOW_ALL)) {
      for (Iterator<KeyListEntry> it = keys.iterator(); it.hasNext(); ) {
        KeyListEntry entry = it.next();
        try (Stream<CommitLogEntry> logStream = adapter.commitLog(entry.getCommitId())) {
          logStream.findFirst().ifPresent(logEntry -> {
            Instant ts = Instant.ofEpochMilli(TimeUnit.MICROSECONDS.toMillis(logEntry.getCreatedTime()));
            logger.log("{}: {}: {}", logEntry.getHash().asString(), ts, entry.getKey());
          });
        }
      }
    }
  }

  private static void listObsoleteMetadataKeys(LocalKVStoreProvider kvStore, DatabaseAdapter adapter,
                                               LocalLogger logger) throws Exception {
    IndexedStore<String, NameSpaceContainer> namespace = kvStore.getStore(NamespaceStoreCreator.class);
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
      if (icebergMetadata == null) {
        continue;
      }

      UUID uuid = UUID.fromString(icebergMetadata.getTableUuid());
      liveTableIds.add(uuid);
    }

    ReferenceInfo<ByteString> main = adapter.namedRef("main", GetNamedRefsParams.DEFAULT);
    // Process only Infinite Splits keys, which have two components.
    // For example (the dot in "dremio.internal" is not a separator):
    // dremio.internal./data/pdfs/metadata/11c3b76c-e355-4dee-8b96-5e704361f49f
    try (Stream<KeyListEntry> keys = adapter.keys(main.getHash(),
      (key, id, type) -> key.getElements().size() == 2 && "dremio.internal".equals(key.getElements().get(0)))) {
      keys.forEach(keyWithType -> {
        String pathName = keyWithType.getKey().getElements().get(1);
        String id = Path.of(pathName).getName(); // the last path element is the table UUID
        if (!liveTableIds.remove(UUID.fromString(id))) {
          // Note: Avoid zero bytes that can be produced by key.toPathString()
          // Note: The '|' char is not used in dremio.internal keys
          logger.log("{}", String.join("|", keyWithType.getKey().getElements()));
        }
      });
    }

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
