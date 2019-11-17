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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.StreamSupport;

import com.dremio.common.AutoCloseables;
import com.dremio.common.perf.Timer;
import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.common.utils.ProtostuffUtil;
import com.dremio.datastore.KVStoreProvider.DocumentConverter;
import com.dremio.datastore.indexed.CommitWrapper;
import com.dremio.datastore.indexed.CoreIndexedStoreImpl;
import com.dremio.datastore.indexed.LuceneSearchIndex;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Service that manages creation and management of various store types.
 */
public class CoreStoreProviderImpl implements CoreStoreProviderRpcService, Iterable<CoreStoreProviderImpl.StoreWithId>  {
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
    if(mode != ForcedMemoryMode.DEFAULT){
      logger.info(String.format("Persistence mode overridden. Setting resolved: -D%s=%s", MEMORY_MODE_OPTION, mode.name()));
      System.out.println("===============================================================================");
      System.out.println(String.format("Persistence mode overridden. Setting resolved: -D%s=%s", MEMORY_MODE_OPTION, mode.name()));
      System.out.println("===============================================================================");
    }
  }

  private static final String METADATA_FILE_SUFFIX = "_metadata.json";
  private static final String METADATA_FILES_DIR = "metadata";
  private static final DirectoryStream.Filter<Path> METADATA_FILES_GLOB = p -> p.toString().endsWith(METADATA_FILE_SUFFIX);
  private static final String ALARM = ".indexing";

  private final ConcurrentMap<String, StoreWithId> idToStore = new ConcurrentHashMap<>();

  private final boolean timed;
  private final boolean inMemory;
  private final boolean validateOCC;
  private final boolean disableOCC;
  private final IndexManager indexManager;
  private final ByteStoreManager byteManager;
  private final String baseDirectory;
  private final File metaDataFilesDir;

  private final AtomicBoolean closed = new AtomicBoolean(false);

  private final Set<String> storeNames = Sets.newConcurrentHashSet();

  private final LoadingCache<StoreBuilderConfig, StoreWithId> stores = CacheBuilder.newBuilder()
      .removalListener(new RemovalListener<StoreBuilderConfig, StoreWithId>() {
        @Override
        public void onRemoval(RemovalNotification<StoreBuilderConfig, StoreWithId> notification) {
        }
      })
      .build(new CacheLoader<StoreBuilderConfig, StoreWithId>() {
        @Override
        public StoreWithId load(StoreBuilderConfig builderConfig) throws Exception {
          // check if some other service has created this table name earlier under different configuration
          if (!storeNames.add(builderConfig.getName())) {
            throw new DatastoreFatalException("Duplicate datastore " + builderConfig.toString());
          }

          final FinalStoreBuilderConfig resolvedConfig = new FinalStoreBuilderConfig(builderConfig);
          final StoreWithId store;
          // write table's configuration to a file first
          if (!inMemory) {
            createMetaDataFile(builderConfig);
          }

          if(resolvedConfig.hasDocumentConverter()){
            store = new StoreWithId(builderConfig, buildIndexed(resolvedConfig));
          }else{
            store = new StoreWithId(builderConfig, build(resolvedConfig));
          }

          idToStore.put(store.id, store);
          return store;
        }
      });

  private File alarmFile;

  @VisibleForTesting
  CoreStoreProviderImpl(String baseDirectory, boolean inMemory, boolean timed) {
    this(baseDirectory, inMemory, timed, true, false, false);
  }

  CoreStoreProviderImpl(String baseDirectory, boolean inMemory, boolean timed, boolean validateOCC, boolean disableOCC, boolean noDBOpenRetry) {
    super();
    switch(MODE){
    case DISK:
      inMemory = false;
      break;
    case MEMORY:
      inMemory = true;
    default:
      // noop
    }

    this.timed = timed;
    this.validateOCC = validateOCC; // occ store is created but validations are skipped
    this.disableOCC = disableOCC; // occ store is not created.
    this.inMemory = inMemory;

    this.byteManager = new ByteStoreManager(baseDirectory, inMemory, noDBOpenRetry);
    this.indexManager = new IndexManager(
        baseDirectory,
        inMemory,
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
              idToStore.get(s)
                  .getStoreBuilderConfig()
                  .getDocumentConverterClassName() != null
      );
      logger.info("Partial re-indexing status: {}, metrics:\n{}", status, reIndexer.getMetrics());
      return status;
    } catch (DatastoreException e) {
      logger.warn("Partial reindexing failed", e);
      return false;
    }
  }

  @VisibleForTesting
  KVStore<byte[], byte[]> getDB(String name) {
    return byteManager.getStore(name);
  }

  @Override
  public Iterator<StoreWithId> iterator() {
    return Iterators.unmodifiableIterator(stores.asMap().values().iterator());
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

  @Override
  public CoreKVStore<Object, Object> getStore(String storeId) {
    StoreWithId storeWithId = idToStore.get(storeId);
    if(storeWithId != null){
      return storeWithId.store;
    }

    throw new DatastoreException("Invalid store id " + storeId);
  }

  @Override
  public String getOrCreateStore(StoreBuilderConfig config) {
    return checkedGet(config).id;
  }

  final class CoreStoreBuilderImpl<K, V> implements CoreStoreBuilder<K, V> {
    private String name;
    private final StoreBuilderConfig builderConfig;

    public CoreStoreBuilderImpl() {
      builderConfig = new StoreBuilderConfig();
    }

    @Override
    public CoreStoreBuilder<K, V> name(String name) {
      this.name = name;
      builderConfig.setName(name);
      return this;
    }

    @Override
    public CoreStoreBuilder<K, V> keySerializer(Class<? extends Serializer<K>> keySerializerClass) {
      Preconditions.checkNotNull(keySerializerClass);
      builderConfig.setKeySerializerClassName(keySerializerClass.getName());
      return this;
    }

    @Override
    public CoreStoreBuilder<K, V> valueSerializer(Class<? extends Serializer<V>> valueSerializerClass) {
      Preconditions.checkNotNull(valueSerializerClass);
      builderConfig.setValueSerializerClassName(valueSerializerClass.getName());
      return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CoreKVStore<K, V> build() {
      return (CoreKVStore<K, V>) checkedGet(builderConfig).store;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CoreIndexedStore<K, V> buildIndexed(Class<? extends DocumentConverter<K, V>> documentConverterClass) {
      Preconditions.checkNotNull(documentConverterClass);
      builderConfig.setDocumentConverterClassName(documentConverterClass.getName());
      return (CoreIndexedStore<K, V>) checkedGet(builderConfig).store;
    }

    @Override
    public CoreStoreBuilder<K, V> versionExtractor(Class<? extends VersionExtractor<V>> versionExtractorClass) {
      Preconditions.checkNotNull(versionExtractorClass);
      builderConfig.setVersionExtractorClassName(versionExtractorClass.getName());
      return this;
    }

  }

  public Map<StoreBuilderConfig, CoreKVStore<?, ?>> getStores() {
    final Map<StoreBuilderConfig, CoreKVStore<?, ?>> coreKVStores = Maps.newHashMap();
    for (StoreWithId store : stores.asMap().values()) {
      coreKVStores.put(store.storeBuilderConfig, store.store);
    }
    return coreKVStores;
  }

  private StoreWithId checkedGet(StoreBuilderConfig key) throws DatastoreException {
    try{
      return stores.get(key);
    } catch (ExecutionException ex){
      Throwables.propagateIfInstanceOf(ex.getCause(), DatastoreException.class);
      throw new DatastoreException(ex);
    }
  }

  /**
   * Class used to resolve class names to instances.
   */
  private class FinalStoreBuilderConfig {
    private final String name;
    private final Serializer<Object> keySerializer;
    private final Serializer<Object> valueSerializer;
    private final DocumentConverter<Object,Object> documentConverter;
    private final VersionExtractor<Object> versionExtractor;

    @SuppressWarnings("unchecked")
    public FinalStoreBuilderConfig(StoreBuilderConfig config) {
      name = Preconditions.checkNotNull(config.getName(), "Name must be defined");
      keySerializer = DataStoreUtils.getInstance(config.getKeySerializerClassName(), Serializer.class, true);
      valueSerializer = DataStoreUtils.getInstance(config.getValueSerializerClassName(), Serializer.class, true);
      documentConverter = DataStoreUtils.getInstance(config.getDocumentConverterClassName(), DocumentConverter.class, false);
      versionExtractor = DataStoreUtils.getInstance(config.getVersionExtractorClassName(), VersionExtractor.class, false);
    }

    public boolean hasDocumentConverter() {
      return documentConverter != null;
    }

    public boolean hasVersionExtractor() {
      return versionExtractor != null;

    }
  }

  private CoreKVStore<Object, Object> getCoreStore(FinalStoreBuilderConfig builderConfig) {

    final ByteStore rawStore = byteManager.getStore(builderConfig.name);

    final CoreKVStore<Object, Object> coreKVStore =
        new CoreKVStoreImpl<>(
            rawStore,
            builderConfig.keySerializer,
            builderConfig.valueSerializer,
            builderConfig.versionExtractor);
    if (!disableOCC && builderConfig.hasVersionExtractor()) {
      return new OCCStore<>(coreKVStore, !validateOCC);
    } else {
      return coreKVStore;
    }
  }

  private CoreKVStore<Object, Object> build(FinalStoreBuilderConfig config){
    final CoreKVStore<Object, Object> coreStore = getCoreStore(config);
    if (timed) {
      final CoreKVStore<Object, Object> timedStore = new CoreBaseTimedStore.TimedStoreImplCore<>(config.name, coreStore);
      return timedStore;
    } else {
      return coreStore;
    }
  }

  private CoreIndexedStore<Object, Object> buildIndexed(FinalStoreBuilderConfig config){
    final String name = config.name;
    CoreIndexedStore<Object, Object> store = new CoreIndexedStoreImpl<>(name, getCoreStore(config), indexManager.getIndex(name), config.documentConverter);
    if (timed) {
      return new CoreBaseTimedStore.TimedIndexedStoreImplCore<>(name, store);
    } else {
      return store;
    }
  }

  /**
   * A description of a Store along with an associated name.
   */
  public static class StoreWithId {
    private final String id;
    private final StoreBuilderConfig storeBuilderConfig;
    private final CoreKVStore<Object, Object> store;

    public StoreWithId(StoreBuilderConfig storeBuilderConfig, CoreKVStore<Object, Object> store) {
      super();
      this.storeBuilderConfig = storeBuilderConfig;
      this.id = storeBuilderConfig.getName();
      this.store = store;
    }

    public String getId() {
      return id;
    }

    public StoreBuilderConfig getStoreBuilderConfig() {
      return storeBuilderConfig;
    }

    public CoreKVStore<Object, Object> getStore() {
      return store;
    }
  }

  private void createMetaDataFile(StoreBuilderConfig builderConfig) throws IOException {
    final KVStoreInfo kvStoreInfo = DataStoreUtils.toInfo(builderConfig);
    final Path metadataFile = Paths.get(metaDataFilesDir.getAbsolutePath(), format("%s%s", builderConfig.getName(), METADATA_FILE_SUFFIX));
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

        final StoreBuilderConfig storeBuilderConfig = DataStoreUtils.toBuilderConfig(metadata);
        getOrCreateStore(storeBuilderConfig);
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
}
