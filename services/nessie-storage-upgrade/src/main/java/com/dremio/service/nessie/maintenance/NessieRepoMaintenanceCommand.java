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

import java.util.Map;
import java.util.Optional;

import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.GlobalLogCompactionParams;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
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
import com.dremio.service.nessie.DatastoreDatabaseAdapterFactory;
import com.dremio.service.nessie.EmbeddedRepoMaintenanceParams;
import com.dremio.service.nessie.EmbeddedRepoPurgeParams;
import com.dremio.service.nessie.ImmutableDatastoreDbConfig;
import com.dremio.service.nessie.ImmutableEmbeddedRepoMaintenanceParams;
import com.dremio.service.nessie.NessieDatastoreInstance;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;

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
      description = "Only list live keys without performing any maintenance.")
    private boolean listKeys;

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
    NonTransactionalDatabaseAdapterConfig adapterCfg = ImmutableAdjustableNonTransactionalDatabaseAdapterConfig
      .builder()
      .build();
    NessieDatastoreInstance store = new NessieDatastoreInstance();
    store.configure(new ImmutableDatastoreDbConfig.Builder().setStoreProvider(() -> kvStore).build());
    store.initialize();
    TableCommitMetaStoreWorker worker = new TableCommitMetaStoreWorker();
    DatabaseAdapter adapter = new DatastoreDatabaseAdapterFactory().newBuilder()
      .withConnector(store)
      .withConfig(adapterCfg)
      .build(worker);

    if (options.listKeys) {
      listKeys(adapter);
      return "";
    } else if (options.purgeKeyLists || options.compactGlobalLog) {
      return executeMaintenance(adapter, options);
    }

    return null;
  }

  private static String executeMaintenance(DatabaseAdapter adapter, Options options) throws Exception {
    ImmutableEmbeddedRepoMaintenanceParams.Builder params = EmbeddedRepoMaintenanceParams.builder();
    params.setGlobalLogCompactionParams(
      GlobalLogCompactionParams.builder().isEnabled(options.compactGlobalLog).build());

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

  private static void listKeys(DatabaseAdapter adapter) throws Exception {
    ReferenceInfo<ByteString> main = adapter.namedRef("main", GetNamedRefsParams.DEFAULT);
    adapter.keys(main.getHash(), KeyFilterPredicate.ALLOW_ALL).forEach(keyWithType -> {
      AdminLogger.log("{}", keyWithType.getKey());
    });
  }
}
