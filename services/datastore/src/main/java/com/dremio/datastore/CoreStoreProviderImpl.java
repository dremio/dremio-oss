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
package com.dremio.datastore;

import static java.lang.String.format;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.StreamSupport;

import com.dremio.common.AutoCloseables;
import com.dremio.common.perf.Timer;
import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.common.utils.ProtostuffUtil;
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.indexed.CommitWrapper;
import com.dremio.datastore.indexed.CoreIndexedStoreImpl;
import com.dremio.datastore.indexed.LuceneSearchIndex;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

/**
 * Service that manages creation and management of various store types.
 */
public class CoreStoreProviderImpl implements CoreStoreProviderRpcService, Iterable<CoreStoreProviderImpl.StoreWithId<?, ?>>  {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CoreStoreProviderImpl.class);

  private static final boolean REINDEX_ON_CRASH_DISABLED = Boolean.getBoolean("dremio.catalog.disable_reindex_on_crash");

  public static final String MEMORY_MODE_OPTION = "dremio.debug.kv.force";

  enum ForcedMemoryMode {DEFAULT, DISK, MEMORY}
  static final ForcedMemoryMode MODE;

  static {
    ForcedMemoryMode mode = ForcedMemoryMode.DEFAULT;
    final String value = System.getProperty(MEMORY_MODE_OPTION, "DEFAULT").toUpperCase();
    try{
      mode = ForcedMemoryMode.valueOf(value);
    }catch(IllegalArgumentException ex){
      logger.warn("Failure parsing value of [{}] for property {}. Valid values include 'DEFAULT', 'DISK' and 'MEMORY'.", value, MEMORY_MODE_OPTION);
    }
    MODE = mode;
    if (mode != ForcedMemoryMode.DEFAULT) {
      logger.info(String.format("Persistence mode overridden. Setting resolved: -D%s=%s", MEMORY_MODE_OPTION,
        mode.name()));
      System.out.println("===============================================================================");
      System.out.println(String.format("Persistence mode overridden. Setting resolved: -D%s=%s", MEMORY_MODE_OPTION,
        mode.name()));
      System.out.println("===============================================================================");
    }
  }

  private static final String METADATA_FILE_SUFFIX = "_metadata.json";
  private static final String METADATA_FILES_DIR = "metadata";
  private static final DirectoryStream.Filter<Path> METADATA_FILES_GLOB =
    p -> p.toString().endsWith(METADATA_FILE_SUFFIX);
  private static final String ALARM = ".indexing";

  private final ConcurrentMap<String, StoreWithId<?, ?>> idToStore = new ConcurrentHashMap<>();

  private final boolean timed;
  private final boolean inMemory;
  private final boolean indicesViaPutOption;
  private final IndexManager indexManager;
  private final ByteStoreManager byteManager;
  private final String baseDirectory;
  private final File metaDataFilesDir;

  private final AtomicBoolean closed = new AtomicBoolean(false);

  private File alarmFile;

  @VisibleForTesting
  CoreStoreProviderImpl(String baseDirectory, boolean inMemory, boolean timed) {
    this(baseDirectory, inMemory, timed, false, false, false);
  }

  public CoreStoreProviderImpl(String baseDirectory, boolean inMemory, boolean timed, boolean noDBOpenRetry,
    boolean indicesViaPutOption, boolean noDBLogMessages) {
    this(baseDirectory, inMemory, timed, noDBOpenRetry, indicesViaPutOption, noDBLogMessages, false);
  }

  public CoreStoreProviderImpl(String baseDirectory, boolean inMemory, boolean timed, boolean noDBOpenRetry,
    boolean indicesViaPutOption, boolean noDBLogMessages, boolean readOnly) {
    super();
    switch (MODE) {
      case DISK:
        inMemory = false;
        break;
      case MEMORY:
        inMemory = true;
        break;
      default:
        // noop
    }

    this.timed = timed;
    this.inMemory = inMemory;
    this.indicesViaPutOption = indicesViaPutOption;

    this.byteManager = new ByteStoreManager(baseDirectory, inMemory, noDBOpenRetry, noDBLogMessages, readOnly);
    this.indexManager = new IndexManager(
      baseDirectory,
      inMemory,
      readOnly,
      storeName -> {
        // 1. get the transaction number on #open
        final long transactionNumber = byteManager.getMetadataManager()
          .getLatestTransactionNumber();
        // 2. then the op (commit) goes here
        return new CommitWrapper.CommitCloser() {
          @Override
          protected void onClose() {
            // 3. set the transaction number on #close
            byteManager.getMetadataManager()
              .setLatestTransactionNumber(storeName, transactionNumber);
          }
        };
      }
    );

    this.baseDirectory = baseDirectory;
    this.metaDataFilesDir = new File(baseDirectory, METADATA_FILES_DIR);
  }

  @Override
  public void start() throws Exception {
    metaDataFilesDir.mkdirs();

    byteManager.start();
    indexManager.start();
  }

  void recoverIfPreviouslyCrashed() throws Exception {
    if (inMemory) {
      // No recovery for in-memory
      return;
    }

    alarmFile = new File(baseDirectory, ALARM);

    if (!REINDEX_ON_CRASH_DISABLED && alarmFile.exists()) {
      logger.info("Dremio was not stopped properly, so the indexes need to be synced with the stores. This may take a while..");

      try (TimedBlock ignored = Timer.time("reindexing stores on crash")) {
        if (!reIndexDelta()) {
          logger.info("Unable to reindex partially, so reindexing fully. This may take even longer..");
          reIndexFull();
        }
      }

      logger.info("Finished syncing indexes with stores.");
    } else {
      Files.createFile(alarmFile.toPath());
    }

    byteManager.getMetadataManager()
        .allowUpdates();
  }

  /**
   * Reindex store with the given id.
   *
   * @param id store id
   * @return number of re-indexed entries
   */
  int reIndex(String id) {
    CoreKVStore<?, ?> kvStore = getStore(id);
    if (kvStore instanceof CoreIndexedStore<?, ?>) {
      return ((CoreIndexedStore<?, ?>)kvStore).reindex();
    } else {
      throw new IllegalArgumentException("ReIndexed should be called on a Indexed Store. " +
          id + " is not indexed.");
    }
  }

  /**
   * For stores with indexes, perform a full reindex.
   */
  private void reIndexFull() {
    StreamSupport.stream(spliterator(), false)
        .filter(storeWithId -> storeWithId.getStore() instanceof CoreIndexedStore)
        .map(StoreWithId::getId)
        .forEach(this::reIndex);
  }

  /**
   * For stores with indexes, replay updates, starting from the most recent persisted transaction number, from
   * the {@link CoreIndexedStore key-value stores} on to their respective indexes.
   *
   * @return true iff partial reindexing was successful
   */
  private boolean reIndexDelta() {
    final ReIndexer reIndexer = new ReIndexer(indexManager, idToStore);
    try {
      final boolean status = byteManager.replayDelta(
          reIndexer,
          s -> idToStore.containsKey(s) && // to skip removed and auxiliary indexes
              idToStore.get(s).getStoreBuilderHelper().hasDocumentConverter()
      );
      logger.info("Partial re-indexing status: {}, metrics:\n{}", status, reIndexer.getMetrics());
      return status;
    } catch (DatastoreException e) {
      logger.warn("Partial reindexing failed", e);
      return false;
    }
  }

  @Override
  public Iterator<StoreWithId<?, ?>> iterator() {
    return Iterators.unmodifiableIterator(idToStore.values().iterator());
  }

  @Override
  public synchronized void close() throws Exception {
    if(closed.compareAndSet(false, true)) {
      AutoCloseables.close(indexManager, byteManager);
      if (alarmFile != null && !alarmFile.delete()) {
        logger.warn("Failed to remove alarm file. Dremio will reindex internal stores on next start up.");
      } else {
        logger.trace("Deleted alarm file.");
      }
    }
  }

  @Override
  public <K, V> CoreStoreBuilder<K, V> newStore() {
    return new CoreStoreBuilderImpl<>();
  }

  @SuppressWarnings("unchecked")
  @Override
  public CoreKVStore<Object, Object> getStore(String storeId) {
    StoreWithId storeWithId = idToStore.get(storeId);
    if(storeWithId != null){
      return storeWithId.store;
    }

    throw new DatastoreException("Invalid store id " + storeId);
  }

  @Override
  public String getStoreID(String name) {
    StoreWithId<?, ?> storeWithId = idToStore.get(name);
    if (storeWithId == null) {
      throw new DatastoreFatalException("Cannot find store " + name);
    }
    return storeWithId.id;
  }

  /**
   * Takes a core store and wraps it in whatever unique decorators are necessary.
   * CoreKvStores and CoreIndexedStores share a lot of setup code. This makes
   * it easier to see how they are setup differently/similarly compared
   * to deeply nested helper functions with if statements.
   * @param <K> Key type.
   * @param <V> Value type.
   * @param <STORE> Store type. Either indexed or CoreKVStore.
   */
  @FunctionalInterface
  private interface StoreAssembler<K, V, STORE extends CoreKVStore<K, V>> {

    STORE run(CoreKVStore<K, V> coreKVStore) throws Exception;
  }

  final class CoreStoreBuilderImpl<K, V> implements CoreStoreBuilder<K, V> {

    private StoreBuilderHelper<K, V> helper;

    public CoreStoreBuilderImpl() {
    }

    @Override
    public CoreKVStore<K, V> build(StoreBuilderHelper<K, V> helper) {
      this.helper = helper;
      return this.build(this::assembler);
    }

    @Override
    public CoreIndexedStore<K, V> buildIndexed(StoreBuilderHelper<K, V> helper) {
      this.helper = helper;
      return this.build(this::indexedAssembler);
    }

    private CoreIndexedStore<K, V> indexedAssembler(CoreKVStore<K, V> coreKVStore) throws Exception {
      final DocumentConverter<K, V> documentConverter = helper.getDocumentConverter();
      final String name = helper.getName();
      CoreIndexedStore<K, V> store = new CoreIndexedStoreImpl<>(name, coreKVStore, indexManager.getIndex(name), documentConverter, indicesViaPutOption);
      if (timed) {
        store = new CoreBaseTimedStore.TimedIndexedStoreImplCore<>(name, store);
      }
      return store;
    }

    private CoreKVStore<K, V> assembler(CoreKVStore<K, V> coreKVStore) {

      if (timed) {
        coreKVStore = new CoreBaseTimedStore.TimedStoreImplCore<>(helper.getName(), coreKVStore);
      }

      return coreKVStore;
    }

    /**
     * Build a common base CoreKVStore then delegates the final build steps to the StoreAssembler.
     * Also, adds the final store to the CoreStore provider's registry.
     * @param assembler Converts the base CoreKvStore into its final form.
     * @param <STORE> Final CoreKVStore type. Could be indexed or not indexed.
     * @return STORE returned by assembler.
     */
    private <STORE extends CoreKVStore<K, V>> STORE build(StoreAssembler<K, V, STORE> assembler) {
      try{

        final KVStoreInfo kvStoreInfo = helper.getKVStoreInfo();

        // Stores are created sequentially.
        // If the store is not present at the start of load, there is no duplication.
        if (idToStore.containsKey(kvStoreInfo.getTablename())) {
          throw new DatastoreFatalException("Duplicate datastore " + kvStoreInfo.toString());
        }

        final Serializer<K, byte[]> keySerializer = (Serializer<K, byte[]>) helper.getKeyFormat().apply(ByteSerializerFactory.INSTANCE);
        final Serializer<V, byte[]> valueSerializer = (Serializer<V, byte[]>) helper.getValueFormat().apply(ByteSerializerFactory.INSTANCE);

        // write table's configuration to a file first
        if (!inMemory) {
          createMetaDataFile(kvStoreInfo);
        }

        final ByteStore rawStore = byteManager.getStore(kvStoreInfo.getTablename());

        CoreKVStore<K, V> coreKVStore =
          new CoreKVStoreImpl<>(
            rawStore,
            keySerializer,
            valueSerializer);

        final STORE store = assembler.run(coreKVStore);
        final StoreWithId<K, V> storeWithId = new StoreWithId<>(helper, store);

        idToStore.put(storeWithId.id, storeWithId);
        return store;
      } catch (Exception ex) {
        Throwables.propagateIfInstanceOf(ex.getCause(), DatastoreException.class);
        throw new DatastoreException(ex);
      }
    }
  }

  public Map<KVStoreInfo, CoreKVStore<?, ?>> getStores() {
    final Map<KVStoreInfo, CoreKVStore<?, ?>> coreKVStores = Maps.newHashMap();
    idToStore.values().forEach((v) -> coreKVStores.put(v.getStoreBuilderHelper().getKVStoreInfo(), v.store));
    return coreKVStores;
  }

  /**
   * A description of a Store along with an associated setName.
   */
  public static class StoreWithId<K, V> {
    private final String id;
    private final StoreBuilderHelper<K, V> helper;
    private final CoreKVStore<K, V> store;

    public StoreWithId(StoreBuilderHelper<K, V> helper, CoreKVStore<K, V> store) {
      this.helper = helper;
      this.id = helper.getName();
      this.store = store;
    }

    public String getId() {
      return id;
    }

    public StoreBuilderHelper<K, V> getStoreBuilderHelper() {
      return helper;
    }

    public CoreKVStore<?, ?> getStore() {
      return store;
    }
  }

  private void createMetaDataFile(KVStoreInfo kvStoreInfo) throws IOException {
    final Path metadataFile = Paths.get(metaDataFilesDir.getAbsolutePath(), format("%s%s", kvStoreInfo.getTablename(), METADATA_FILE_SUFFIX));
    try (OutputStream metaDataOut = Files.newOutputStream(metadataFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
      ProtostuffUtil.toJSON(metaDataOut, kvStoreInfo, KVStoreInfo.getSchema(), false);
    }
  }

  /**
   * Scan dbDirectory to read kvstore definitions and load all stores in memory.
   */
  public void scan() throws Exception {
    try (DirectoryStream<Path> metaDataFiles = Files.newDirectoryStream(metaDataFilesDir.toPath(), METADATA_FILES_GLOB)) {
      for (Path metadataFile : metaDataFiles) {
        final byte[] headerBytes = Files.readAllBytes(metadataFile);
        final KVStoreInfo metadata = new KVStoreInfo();
        ProtostuffUtil.fromJSON(headerBytes, metadata, KVStoreInfo.getSchema(), false);

        getStore(metadata.getTablename());
      }
    }
  }

  /**
   * **DESTRUCTIVE** For testing only
   *
   * Deletes all data associate with this store provider.
   * @throws IOException
   */
  public void deleteEverything(String... skipNamesArray) throws IOException{
    Set<String> skipNames = new HashSet<>();
    for(String skipName : skipNamesArray){
      skipNames.add(skipName);
    }
    byteManager.deleteEverything(skipNames);
    indexManager.deleteEverything(skipNames);

  }

  public LuceneSearchIndex getIndex(String name) {
    return indexManager.getIndex(name);
  }

  public CheckpointInfo newCheckpoint(Path backupDir) {
    return byteManager.newCheckpoint(backupDir);
  }
}
