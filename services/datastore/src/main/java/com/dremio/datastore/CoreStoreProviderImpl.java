/*
 * Copyright (C) 2017 Dremio Corporation
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
import java.lang.reflect.Constructor;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.rocksdb.ColumnFamilyHandle;

import com.dremio.common.AutoCloseables;
import com.dremio.common.utils.ProtostuffUtil;
import com.dremio.datastore.KVStoreProvider.DocumentConverter;
import com.dremio.datastore.indexed.CoreIndexedStoreImpl;
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
  public static final String MEMORY_MODE_OPTION = "dremio.debug.kv.force";

  static enum ForcedMemoryMode {DEFAULT, DISK, MEMORY};
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
  private static final GlobFilter METADATA_FILES_GLOB = DataStoreUtils.getGlobFilter(METADATA_FILE_SUFFIX);

  private final ConcurrentMap<String, StoreWithId> idToStore = new ConcurrentHashMap<>();

  private final ConcurrentMap<byte[], ColumnFamilyHandle> handles = new ConcurrentHashMap<>();

  private final boolean timed;
  private final boolean inMemory;
  private final boolean validateOCC;
  private final boolean disableOCC;
  private final IndexManager indexManager;
  private final ByteStoreManager byteManager;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final File metaDataFilesDir;

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


  public CoreStoreProviderImpl(String baseDirectory, boolean inMemory, boolean timed) {
    this(baseDirectory, inMemory, timed, true, false);
  }

  public CoreStoreProviderImpl(String baseDirectory, boolean inMemory, boolean timed, boolean validateOCC, boolean disableOCC) {
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

    this.byteManager = new ByteStoreManager(baseDirectory, inMemory);
    this.indexManager = new IndexManager(baseDirectory, inMemory);

    this.metaDataFilesDir = new File(baseDirectory, METADATA_FILES_DIR);
  }

  @Override
  public void start() throws Exception {
    metaDataFilesDir.mkdirs();
    byteManager.start();
    indexManager.start();
  }

  /**
   * reIndex a specific store
   * @param id
   */
  public int reIndex(String id) {
    CoreKVStore<?, ?> kvStore = getStore(id);
    if (kvStore instanceof CoreIndexedStore<?, ?>) {
      return ((CoreIndexedStore<?, ?>)kvStore).reindex();
    } else {
      throw new IllegalArgumentException("ReIndexed should be called on a Indexed Store. " +
          id + " is not indexed.");
    }
  }

  @VisibleForTesting
  KVStore<byte[], byte[]> getDB(String name) {
    return byteManager.getStore(name);
  }

  public Iterator<StoreWithId> iterator(){
    return Iterators.unmodifiableIterator(stores.asMap().values().iterator());
  }

  @Override
  public synchronized void close() throws Exception {
    if(closed.compareAndSet(false, true)){
      AutoCloseables.close(indexManager, byteManager);
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
      keySerializer = getInstance(config.getKeySerializerClassName(), Serializer.class, true);
      valueSerializer = getInstance(config.getValueSerializerClassName(), Serializer.class, true);
      documentConverter = getInstance(config.getDocumentConverterClassName(), DocumentConverter.class, false);
      versionExtractor = getInstance(config.getVersionExtractorClassName(), VersionExtractor.class, false);
    }

    public boolean hasDocumentConverter() {
      return documentConverter != null;
    }

    public boolean hasVersionExtractor() {
      return versionExtractor != null;

    }
  }

  /**
   * Convert a class name to an instance.
   * @param className The class name to load.
   * @param clazz The expected class type.
   * @param failOnEmpty Whether to fail or return null if no value is given.
   * @return The newly created instance (or null if !failOnEmpty and emptry string used).
   */
  @SuppressWarnings("unchecked")
  private static <T> T getInstance(String className, Class<T> clazz, boolean failOnEmpty){
    if(className == null || className.isEmpty()) {
      if(failOnEmpty){
        throw new DatastoreException(String.format("Failure trying to resolve class for expected type of %s. The provided class name was either empty or null.", clazz.getName()));
      }
      return null;
    } else {
      try{
        final Class<?> outcome = Class.forName(Preconditions.checkNotNull(className));
        Preconditions.checkArgument(clazz.isAssignableFrom(outcome));
        Constructor<?> constructor = outcome.getDeclaredConstructor();
        constructor.setAccessible(true);
        return (T) constructor.newInstance();
      } catch(Exception ex) {
        throw new DatastoreException(String.format("Failure while trying to load class named %s which should be a subclass of %s. ", className, clazz.getName()));
      }
    }
  }

  private CoreKVStore<Object, Object> getCoreStore(FinalStoreBuilderConfig builderConfig) {

    final ByteStore rawStore = byteManager.getStore(builderConfig.name);

    final CoreKVStore<Object, Object> coreKVStore = new CoreKVStoreImpl<>(rawStore, builderConfig.keySerializer, builderConfig.valueSerializer, builderConfig.versionExtractor);
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
    final Path metadataFile = new Path(metaDataFilesDir.getAbsolutePath(), format("%s%s", builderConfig.getName(), METADATA_FILE_SUFFIX));
    final FileSystem fs = FileSystem.getLocal(new Configuration());
    try (FSDataOutputStream metaDataOut = fs.create(metadataFile, true)) {
      ProtostuffUtil.toJSON(metaDataOut, kvStoreInfo, KVStoreInfo.getSchema(), false);
    }
  }

  /**
   * Scan dbDirectory to read kvstore definitions and load all stores in memory.
   */
  public void scan() throws Exception {
    final FileSystem fs = FileSystem.getLocal(new Configuration());
    final FileStatus[] metaDataFiles = fs.listStatus(new Path(metaDataFilesDir.getPath()), METADATA_FILES_GLOB);
    for (FileStatus fileStatus : metaDataFiles) {
      final byte[] headerBytes = new byte[(int) fileStatus.getLen()];
      IOUtils.readFully(fs.open(fileStatus.getPath()), headerBytes, 0, headerBytes.length);
      final KVStoreInfo metadata = new KVStoreInfo();
      ProtostuffUtil.fromJSON(headerBytes, metadata, KVStoreInfo.getSchema(), false);

      final StoreBuilderConfig storeBuilderConfig = DataStoreUtils.toBuilderConfig(metadata);
      getOrCreateStore(storeBuilderConfig);
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
}
