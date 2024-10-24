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
package com.dremio.plugins.dataplane.exec;

import static com.dremio.exec.store.IcebergExpiryMetric.NUM_ACCESS_DENIED;
import static com.dremio.exec.store.IcebergExpiryMetric.NUM_NOT_FOUND;
import static com.dremio.exec.store.IcebergExpiryMetric.NUM_PARTIAL_FAILURES;
import static com.dremio.exec.store.iceberg.logging.VacuumLogProto.ErrorType.NOT_FOUND_EXCEPTION;
import static com.dremio.exec.store.iceberg.logging.VacuumLogProto.ErrorType.PERMISSION_EXCEPTION;
import static com.dremio.exec.store.iceberg.logging.VacuumLogProto.ErrorType.UNKNOWN;
import static com.dremio.exec.store.iceberg.logging.VacuumLoggingUtil.createNessieExpireSnapshotLog;
import static com.dremio.exec.store.iceberg.logging.VacuumLoggingUtil.getVacuumLogger;
import static com.dremio.plugins.dataplane.exec.NessieCommitsRecordReader.readTableMetadata;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.logging.StructuredLogger;
import com.dremio.common.util.S3ConnectionConstants;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.context.JobIdContext;
import com.dremio.context.RequestContext;
import com.dremio.exec.catalog.VacuumOptions;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.iceberg.IcebergExpiryAction;
import com.dremio.exec.store.iceberg.IcebergExpirySnapshotsReader;
import com.dremio.exec.store.iceberg.SnapshotsScanOptions;
import com.dremio.exec.store.iceberg.SupportsIcebergMutablePlugin;
import com.dremio.plugins.dataplane.store.DataplanePlugin;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Stopwatch;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.nessie.NessieIcebergClient;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Reference.ReferenceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Nessie version of {@link IcebergExpirySnapshotsReader} which goes over all the Nessie tables across all the refs
 * for the expiry actions.
 * The identification logic is handled by an Async producer, which produces these actions in the queue. Parent class
 * is given next {@link IcebergExpiryAction} by the queue.
 * A semaphore is put in place to ensure that the parallelism of table expiry actions do not exceed the FileSystem limits.
 */
public class NessieIcebergExpirySnapshotsReader extends IcebergExpirySnapshotsReader {

  public static final String FS_EXPIRY_PARALLELISM_CONF_KEY = "vacuum.expiry_action.parallelism";
  private static final Logger LOGGER =
      LoggerFactory.getLogger(NessieIcebergExpirySnapshotsReader.class);
  private static final StructuredLogger vacuumLogger = getVacuumLogger();

  private final NessieApiV2 nessieApi;
  private CompletableFuture<?> producer;
  private final Queue<CompletableFuture<Optional<IcebergExpiryAction>>> expiryActionsQueue;

  private final Semaphore slots;
  private final String schemeVariate;
  private final String fsScheme;
  private final String queryId;

  public NessieIcebergExpirySnapshotsReader(
      OperatorContext context,
      SupportsIcebergMutablePlugin icebergMutablePlugin,
      OpProps props,
      SnapshotsScanOptions snapshotsScanOptions,
      String schemeVariate,
      String fsScheme) {
    super(context, icebergMutablePlugin, props, snapshotsScanOptions);
    DataplanePlugin plugin = (DataplanePlugin) icebergMutablePlugin;
    this.nessieApi = plugin.getNessieApi();

    // Limit the parallel expiry threads based on filesystem's limits. Take minimum of all supported
    // filesystem implementations.
    // Since S3 is the only supported FS, using that value.
    int maxParallelism =
        plugin
            .getProperty(FS_EXPIRY_PARALLELISM_CONF_KEY)
            .map(Integer::parseInt)
            .orElse(S3ConnectionConstants.DEFAULT_MAX_THREADS / 2);
    this.slots = new Semaphore(maxParallelism);
    this.expiryActionsQueue = new ConcurrentLinkedQueue<>();
    this.schemeVariate = schemeVariate;
    this.fsScheme = fsScheme;
    this.queryId = QueryIdHelper.getQueryId(context.getFragmentHandle().getQueryId());
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    super.setup(output);
    initializeProducer();
  }

  private void initializeProducer() {
    producer =
        CompletableFuture.runAsync(
            () -> {
              try {
                RequestContext requestContext =
                    RequestContext.current()
                        .with(
                            JobIdContext.CTX_KEY,
                            new JobIdContext(
                                QueryIdHelper.getQueryId(
                                    context.getFragmentHandle().getQueryId())));

                requestContext
                    .callStream(
                        () ->
                            nessieApi.getAllReferences().stream()
                                .filter(r -> ReferenceType.BRANCH.equals(r.getType()))
                                .flatMap(this::listTables))
                    .forEach(
                        tableHolder -> {
                          try {
                            slots.acquire();
                            expiryActionsQueue.offer(
                                CompletableFuture.supplyAsync(
                                    () -> prepareExpiryAction(tableHolder), context.getExecutor()));
                          } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                          }
                        });

                // Ensure at-least 1 element in the queue so consumer doesn't wait indefinitely
                expiryActionsQueue.offer(CompletableFuture.completedFuture(Optional.empty()));
              } catch (Exception e) {
                throw UserException.dataReadError(e)
                    .message("Error while expiring snapshots.")
                    .build();
              }
            },
            context.getExecutor());
  }

  private Optional<IcebergExpiryAction> prepareExpiryAction(IcebergTableInfo tableHolder) {
    String tableId =
        String.format(
            "%s AT %s",
            tableHolder.getTableId().name(), tableHolder.getVersionContext().getRefName());
    Stopwatch tracker = Stopwatch.createStarted();
    boolean status = false;
    IcebergTable table = tableHolder.getTable();
    String metadataLocation = table.getMetadataLocation();
    String tableName = tableHolder.getTableId().name();

    try {
      String namespace =
          Optional.ofNullable(tableHolder.getTableId().namespace())
              .map(Namespace::toString)
              .orElse(null);
      ResolvedVersionContext tableVersionContext = tableHolder.getVersionContext();

      super.setupFsIfNecessary(metadataLocation);
      Optional<TableMetadata> tableMetadata =
          readTableMetadata(io, metadataLocation, tableId, context);
      if (tableMetadata.isEmpty()) {
        return Optional.empty();
      }
      VacuumOptions options =
          new VacuumOptions(
              true,
              false,
              snapshotsScanOptions.getOlderThanInMillis(),
              snapshotsScanOptions.getRetainLast(),
              null,
              null);

      Optional<IcebergExpiryAction> ret =
          Optional.of(
              new NessieIcebergExpiryAction(
                  icebergMutablePlugin,
                  props,
                  context,
                  options,
                  tableMetadata.get(),
                  tableName,
                  namespace,
                  tableVersionContext,
                  io,
                  true,
                  schemeVariate,
                  fsScheme));
      status = true;
      vacuumLogger.info(
          createNessieExpireSnapshotLog(
              queryId,
              tableId,
              metadataLocation,
              snapshotsScanOptions,
              ret.get().getExpiredSnapshots().stream()
                  .map(snapshotEntry -> String.valueOf(snapshotEntry.getSnapshotId()))
                  .collect(Collectors.toList())),
          "");
      return ret;
    } catch (NotFoundException nfe) {
      String message = "Skipping since table metadata is missing for the table " + tableId;
      LOGGER.warn(message, nfe);
      vacuumLogger.warn(
          createNessieExpireSnapshotLog(
              queryId,
              tableId,
              metadataLocation,
              snapshotsScanOptions,
              NOT_FOUND_EXCEPTION,
              message + "./n" + nfe.toString()),
          "");
      context.getStats().addLongStat(NUM_PARTIAL_FAILURES, 1L);
      context.getStats().addLongStat(NUM_NOT_FOUND, 1L);
      return Optional.empty();
    } catch (UserException e) {
      vacuumLogger.warn(
          createNessieExpireSnapshotLog(
              queryId, tableId, metadataLocation, snapshotsScanOptions, UNKNOWN, e.toString()),
          "");
      if (UserBitShared.DremioPBError.ErrorType.PERMISSION.equals(e.getErrorType())) {
        String message = "Skipping since access is denied on the table ";
        vacuumLogger.warn(
            createNessieExpireSnapshotLog(
                queryId,
                tableId,
                metadataLocation,
                snapshotsScanOptions,
                PERMISSION_EXCEPTION,
                message),
            "");
        LOGGER.warn(message + tableId, e);
        context.getStats().addLongStat(NUM_PARTIAL_FAILURES, 1L);
        context.getStats().addLongStat(NUM_ACCESS_DENIED, 1L);
        return Optional.empty();
      } else {
        throw e;
      }
    } finally {
      LOGGER.info(
          "Table expiry {} status {}, time taken was {}ms",
          tableId,
          status,
          tracker.elapsed(TimeUnit.MILLISECONDS));
      slots.release();
    }
  }

  @Override
  protected void setupNextExpiryAction() {
    try {
      currentExpiryAction = null;
      while (!expiryActionsQueue.isEmpty() || !producer.isDone()) {
        CompletableFuture<Optional<IcebergExpiryAction>> optionalNextExpiryActionFuture =
            expiryActionsQueue.poll();
        if (optionalNextExpiryActionFuture == null) {
          // Queue is empty, producer is lagging
          continue;
        }
        Optional<IcebergExpiryAction> optionalNextExpiryAction =
            optionalNextExpiryActionFuture.get();
        if (optionalNextExpiryAction.isPresent()) {
          currentExpiryAction = optionalNextExpiryAction.get();
          return;
        }
      }
      noMoreActions = true;
    } catch (InterruptedException | ExecutionException e) {
      throw UserException.dataReadError(e).message("Error while expiring snapshots").build();
    }
  }

  @Override
  protected void processNextTable() {
    checkProducerState();
    super.processNextTable();
  }

  @Override
  public void close() {
    if (!producer.isDone()) {
      producer.cancel(true);
    }
  }

  private void checkProducerState() {
    if (producer.isCompletedExceptionally()) {
      try {
        producer.get();
      } catch (ExecutionException | InterruptedException e) {
        throw UserException.dataWriteError(e).message("Error while expiring snapshots").build();
      }
    }
  }

  private Stream<IcebergTableInfo> listTables(Reference branch) {
    NessieIcebergClient nessieIcebergClient =
        new NessieIcebergClient(
            nessieApi, branch.getName(), branch.getHash(), Collections.emptyMap());
    try {
      return nessieApi.getEntries().reference(branch).stream()
          .filter(e -> Content.Type.ICEBERG_TABLE == e.getType())
          .map(this::toIdentifier)
          .map(id -> new IcebergTableInfo(branch, id, nessieIcebergClient.table(id)));
    } catch (NessieNotFoundException e) {
      throw UserException.dataReadError(e)
          .message(
              "Nessie reference %s not found. Please re-run the query; avoid conflicting Nessie operations while the job is in progress.",
              branch.getName())
          .build();
    }
  }

  private TableIdentifier toIdentifier(EntriesResponse.Entry entry) {
    List<String> elements = entry.getName().getElements();
    return TableIdentifier.of(elements.toArray(new String[elements.size()]));
  }

  private static final class IcebergTableInfo {

    private final ResolvedVersionContext versionContext;
    private final IcebergTable table;
    private final TableIdentifier tableId;

    private IcebergTableInfo(Reference reference, TableIdentifier tableId, IcebergTable table) {
      this.versionContext =
          ResolvedVersionContext.ofBranch(reference.getName(), reference.getHash());
      this.tableId = tableId;
      this.table = table;
    }

    public ResolvedVersionContext getVersionContext() {
      return versionContext;
    }

    public IcebergTable getTable() {
      return table;
    }

    public TableIdentifier getTableId() {
      return tableId;
    }
  }
}
