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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import com.dremio.common.DeferredException;
import com.dremio.datastore.CoreStoreProviderImpl.ForcedMemoryMode;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

/**
 * Manages the underlying byte storage supporting a kvstore.
 */
class ByteStoreManager implements AutoCloseable {

  private static final int STRIPE_COUNT = 16;
  private static final String CATALOG_STORE_NAME = "catalog";

  private final boolean inMemory;
  private final int stripeCount;
  private final String baseDirectory;
  private RocksDB db;
  private ColumnFamilyHandle defaultHandle;

  private final DeferredException closeException = new DeferredException();

  private final LoadingCache<String, ByteStore> maps = CacheBuilder.newBuilder()
      .removalListener(new RemovalListener<String, ByteStore>() {
        @Override
        public void onRemoval(RemovalNotification<String, ByteStore> notification) {
          try {
            notification.getValue().close();
          } catch (Exception ex) {
            closeException.addException(ex);
          }
        }
      })
      .build(new CacheLoader<String, ByteStore>() {
        @Override
        public ByteStore load(String name) throws RocksDBException {
          return newDB(name);
        }
      });

  public ByteStoreManager(String baseDirectory, boolean inMemory) {
    this.stripeCount = STRIPE_COUNT;
    this.baseDirectory = baseDirectory;
    this.inMemory = inMemory;
  }

  private ByteStore newDB(String name) throws RocksDBException {
    if(inMemory){
      return new MapStore(name);
    }else{
      final ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(name.getBytes(UTF_8));
      ColumnFamilyHandle handle = db.createColumnFamily(columnFamilyDescriptor);
      return new RocksDBStore(name, columnFamilyDescriptor, handle, db, stripeCount);
    }
  }

  public void start() throws Exception {
    if(inMemory){
      return;
    }

    final String baseDirectory =
        CoreStoreProviderImpl.MODE == ForcedMemoryMode.DISK && this.baseDirectory == null
        ? Files.createTempDir().toString() : this.baseDirectory.toString();

    final File dbDirectory = new File(baseDirectory, CATALOG_STORE_NAME);
    if (dbDirectory.exists()) {
      if (!dbDirectory.isDirectory()) {
        throw new DatastoreException(
            String.format("Invalid path %s for local catalog db, not a directory.", dbDirectory.getAbsolutePath()));
      }
    } else {
      if (!dbDirectory.mkdirs()) {
        throw new DatastoreException(
            String.format("Failed to create directory %s for local catalog db.", dbDirectory.getAbsolutePath()));
      }
    }

    RocksDB.loadLibrary();
    final String path = dbDirectory.toString();

    final List<byte[]> families;
    try(final Options options = new Options()) {
      options.setCreateIfMissing(true);
      // get a list of existing families.
      families = new ArrayList<>(RocksDB.listColumnFamilies(options, path));
    }


    // add the default family (we don't use this)
    families.add(RocksDB.DEFAULT_COLUMN_FAMILY);
    final Function<byte[], ColumnFamilyDescriptor> func = new Function<byte[], ColumnFamilyDescriptor>(){
      @Override
      public ColumnFamilyDescriptor apply(byte[] input) {
        return new ColumnFamilyDescriptor(input);
      }
    };

    List<ColumnFamilyHandle> familyHandles = new ArrayList<>();
    try(final DBOptions dboptions = new DBOptions()) {
      dboptions.setCreateIfMissing(true);
      db = RocksDB.open(dboptions, path.toString(), Lists.transform(families, func), familyHandles);
    }
    // create an output list to be populated when we open the db.

    // populate the local cache with the existing tables.
    for(int i =0; i < families.size(); i++){
      byte[] family = families.get(i);
      if(Arrays.equals(family, RocksDB.DEFAULT_COLUMN_FAMILY)){
        // we don't allow use of the default handle.
        defaultHandle = familyHandles.get(i);
      } else {
        String name = new String(family, UTF_8);
        RocksDBStore store = new RocksDBStore(name, new ColumnFamilyDescriptor(family), familyHandles.get(i), db, stripeCount);
        maps.put(name, store);
      }
    }
  }

  void deleteEverything(Set<String> skipNames) throws IOException{
    for(Entry<String, ByteStore> entry : maps.asMap().entrySet()){
      if (!skipNames.contains(entry.getKey())) {
        entry.getValue().deleteAllValues();
      }
    }
  }

  public ByteStore getStore(String name){
    Preconditions.checkNotNull(name);
    Preconditions.checkArgument(!"default".equals(name), "The store name 'default' is reserved and cannot be used.");
    try {
      return maps.get(name);
    } catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void close() throws Exception {
    maps.invalidateAll();
    closeException.suppressingClose(defaultHandle);
    closeException.suppressingClose(db);
    closeException.close();
  }
}
