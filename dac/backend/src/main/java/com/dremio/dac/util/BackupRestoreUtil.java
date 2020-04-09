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
package com.dremio.dac.util;

import static java.lang.String.format;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.file.AccessMode;
import java.nio.file.DirectoryStream;
import java.nio.file.attribute.PosixFilePermission;
import java.security.AccessControlException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.ws.rs.core.UriBuilder;

import org.apache.commons.io.IOUtils;

import com.dremio.common.concurrent.CloseableSchedulerThreadPool;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.common.utils.ProtostuffUtil;
import com.dremio.config.DremioConfig;
import com.dremio.dac.homefiles.HomeFileConf;
import com.dremio.dac.proto.model.backup.BackupFileInfo;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.server.tokens.TokenUtils;
import com.dremio.datastore.CoreKVStore;
import com.dremio.datastore.KVStoreInfo;
import com.dremio.datastore.KVStoreTuple;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.KVStore.PutOption;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.rpc.CloseableThreadPool;
import com.dremio.exec.store.dfs.PseudoDistributedFileSystem;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.FileSystemUtils;
import com.dremio.io.file.Path;
import com.dremio.io.file.PathFilters;
import com.dremio.service.jobtelemetry.server.store.LocalProfileStore;
import com.dremio.service.namespace.NamespaceException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Backup Service running only on master.
 */
public final class BackupRestoreUtil {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BackupRestoreUtil.class);

  private static final Set<PosixFilePermission> DEFAULT_PERMISSIONS = Sets.immutableEnumSet(
      PosixFilePermission.OWNER_READ,
      PosixFilePermission.OWNER_WRITE,
      PosixFilePermission.OWNER_EXECUTE
      );

  private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd_HH.mm");
  private static final String BACKUP_DIR_PREFIX = "dremio_backup_";
  private static final String BACKUP_FILE_SUFFIX_JSON = "_backup.json";
  private static final String BACKUP_FILE_SUFFIX_BINARY = "_backup.pb";
  private static final String BACKUP_INFO_FILE_SUFFIX = "_info.json";

  private static final Predicate<Path> BACKUP_FILES_FILTER_JSON = PathFilters.endsWith(BACKUP_FILE_SUFFIX_JSON);
  private static final Predicate<Path> BACKUP_FILES_FILTER_BINARY = PathFilters.endsWith(BACKUP_FILE_SUFFIX_BINARY);
  private static final Predicate<Path> BACKUP_INFO_FILES_FILTER = PathFilters.endsWith(BACKUP_INFO_FILE_SUFFIX);

  private static String getTableName(String fileName, String suffix) {
    return fileName.substring(0, fileName.length() - suffix.length());
  }

  private static final class BackupRecord {
    private final String key;
    private final String value;

    @JsonCreator
    public BackupRecord(
      @JsonProperty("key") String key,
      @JsonProperty("value") String value) {
      this.key = key;
      this.value = value;
    }

    public String getKey() {
      return key;
    }

    public String getValue() {
      return value;
    }
  }

  private static <K, V> void dumpTable(FileSystem fs, Path backupRootDir, BackupFileInfo backupFileInfo, CoreKVStore<K, V> coreKVStore, boolean binary) throws IOException {
    final Path backupFile = backupRootDir.resolve(format("%s%s", backupFileInfo.getKvstoreInfo().getTablename(), binary ? BACKUP_FILE_SUFFIX_BINARY : BACKUP_FILE_SUFFIX_JSON));
    final Iterator<Document<KVStoreTuple<K>, KVStoreTuple<V>>> iterator = coreKVStore.find().iterator();
    long records = 0;

    if(binary) {
      try (
          final OutputStream fsout = fs.create(backupFile, true);
          final DataOutputStream bos = new DataOutputStream(fsout);
          ){
        while (iterator.hasNext()) {
          Document<KVStoreTuple<K>, KVStoreTuple<V>> keyval = iterator.next();
          {
            byte[] key = keyval.getKey().getSerializedBytes();
            bos.writeInt(key.length);
            bos.write(key);
          }
          {
            byte[] value = keyval.getValue().getSerializedBytes();
            bos.writeInt(value.length);
            bos.write(value);
          }
          ++records;
        }
        backupFileInfo.setChecksum(0L);
        backupFileInfo.setRecords(records);
        backupFileInfo.setBinary(true);
      }
    } else {
      final ObjectMapper objectMapper = new ObjectMapper();
      try (
          final OutputStream fsout = fs.create(backupFile, true);
          final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fsout));){
        while (iterator.hasNext()) {
          Document<KVStoreTuple<K>, KVStoreTuple<V>> keyval = iterator.next();
          writer.write(objectMapper.writeValueAsString(new BackupRecord(keyval.getKey().toJson(), keyval.getValue().toJson())));
          writer.newLine();
          ++records;
        }
        backupFileInfo.setChecksum(0L);
        backupFileInfo.setRecords(records);
        backupFileInfo.setBinary(false);
      }

    }

    // write info file after backup file was successfully created and closed.
    final Path backupInfoFile = backupRootDir.resolve(format("%s%s", backupFileInfo.getKvstoreInfo().getTablename(), BACKUP_INFO_FILE_SUFFIX));
    try (OutputStream backupInfoOut = fs.create(backupInfoFile, true)) {
      ProtostuffUtil.toJSON(backupInfoOut, backupFileInfo, BackupFileInfo.getSchema(), false);
    }
  }

  private static <K, V> void restoreTable(FileSystem fs, CoreKVStore<K, V> coreKVStore, Path filePath, boolean binary, long records) throws IOException {
    if (binary) {
      try(DataInputStream dis = new DataInputStream(fs.open(filePath))) {
        for(long i =0; i < records; i++) {
          final KVStoreTuple<K> key = coreKVStore.newKey();
          {
            int keyLength = dis.readInt();
            byte[] keyBytes = new byte[keyLength];
            dis.readFully(keyBytes);
            key.setSerializedBytes(keyBytes);
          }
          final KVStoreTuple<V> value = coreKVStore.newValue();
          {
            int valueLength = dis.readInt();
            byte[] valueBytes = new byte[valueLength];
            dis.readFully(valueBytes);
            value.setSerializedBytes(valueBytes);
          }
          // Use the create flag to ensure OCC-enabled KVStore tables can retrieve an initial version.
          // For non-OCC tables, this start version will get ignored and overwritten.
          coreKVStore.put(key, value, PutOption.CREATE);
        }
      }
      return;
    }

    try(final BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)));) {
      final ObjectMapper objectMapper = new ObjectMapper();
      String line;
      while ((line = reader.readLine()) != null) {
        final KVStoreTuple<K> key = coreKVStore.newKey();
        final BackupRecord record = objectMapper.readValue(line, BackupRecord.class);
        key.setObject(key.fromJson(record.getKey()));
        final KVStoreTuple<V> value = coreKVStore.newValue();
        value.setObject(value.fromJson(record.getValue()));
        // Use the create flag to ensure OCC-enabled KVStore tables can retrieve an initial version.
        // For non-OCC tables, this start version will get ignored and overwritten.
        coreKVStore.put(key, value, PutOption.CREATE);
      }
    }
  }

  private static Map<String, BackupFileInfo> scanInfoFiles(FileSystem fs, Path backupDir) throws IOException {
    final Map<String, BackupFileInfo> tableToInfo = Maps.newHashMap();
    try (final DirectoryStream<FileAttributes> backupFiles = fs.list(backupDir, BACKUP_INFO_FILES_FILTER)) {
      for (FileAttributes backupFile : backupFiles) {
        final String tableName = getTableName(backupFile.getPath().getName(), BACKUP_INFO_FILE_SUFFIX);
        // read backup info file
        final byte[] headerBytes = new byte[(int) backupFile.size()];
        IOUtils.readFully(fs.open(backupFile.getPath()), headerBytes, 0, headerBytes.length);
        final BackupFileInfo backupFileInfo = new BackupFileInfo();
        ProtostuffUtil.fromJSON(headerBytes, backupFileInfo, BackupFileInfo.getSchema(), false);
        tableToInfo.put(tableName, backupFileInfo);
      }
    }
    return tableToInfo;
  }

  private static Map<String, Path> scanBackupFiles(FileSystem fs, Path backupDir, Map<String, BackupFileInfo> tableToInfo) throws IOException {
    final Map<String, Path> tableToBackupData = Maps.newHashMap();
    boolean binary = tableToInfo.values().stream().findFirst().get().getBinary();
    try (final DirectoryStream<FileAttributes> backupDataFiles = fs.list(backupDir, binary ? BACKUP_FILES_FILTER_BINARY : BACKUP_FILES_FILTER_JSON)) {
      for (FileAttributes backupDataFile : backupDataFiles) {
        final String tableName = getTableName(backupDataFile.getPath().getName(), binary ? BACKUP_FILE_SUFFIX_BINARY : BACKUP_FILE_SUFFIX_JSON);
        if (tableToInfo.containsKey(tableName)) {
          tableToBackupData.put(tableName, backupDataFile.getPath());
        } else {
          throw new IOException("Missing metadata file for table " + tableName);
        }
      }
    }

    // make sure we found backup files for all metadata files
    final List<String> missing = Lists.newArrayList();
    for (String tableName : tableToInfo.keySet()) {
      if (!tableToBackupData.containsKey(tableName)) {
        missing.add(tableName);
      }
    }
    if (!missing.isEmpty()) {
      throw new IOException("Backup files missing " + missing);
    }
    return tableToBackupData;
  }

  public static void backupUploadedFiles(FileSystem fs,
                                         Path backupDir,
                                         HomeFileConf homeFileStore,
                                         BackupStats backupStats) throws IOException, NamespaceException {
    final Path uploadsBackupDir = Path.withoutSchemeAndAuthority(backupDir).resolve("uploads");
    fs.mkdirs(uploadsBackupDir);
    final Path uploadsDir = homeFileStore.getInnerUploads();
    copyFiles(homeFileStore.getFilesystemAndCreatePaths(null), uploadsDir, fs, uploadsBackupDir, homeFileStore.isPdfsBased(), backupStats);
  }

  private static void copyFiles(FileSystem srcFs, Path srcPath, FileSystem dstFs, Path dstPath, boolean isPdfs, BackupStats backupStats) throws IOException {
    for (FileAttributes fileAttributes : srcFs.list(srcPath)) {
      if (fileAttributes.isDirectory()) {
        final Path dstDir = dstPath.resolve(fileAttributes.getPath().getName());
        dstFs.mkdirs(dstPath);
        copyFiles(srcFs, fileAttributes.getPath(), dstFs, dstDir, isPdfs, backupStats);
      } else {
        final Path dstFile;
        if (!isPdfs) {
          dstFile = dstPath.resolve(fileAttributes.getPath().getName());
        } else {
          // strip off {host}@ from file name
          dstFile = dstPath.resolve(PseudoDistributedFileSystem.getRemoteFileName(fileAttributes.getPath().getName()));
        }
        FileSystemUtils.copy(srcFs, fileAttributes.getPath(), dstFs, dstFile, false);
        backupStats.incrementFiles();
      }
    }
  }


  public static void restoreUploadedFiles(FileSystem fs, Path backupDir, HomeFileConf homeFileStore, BackupStats backupStats, String hostname) throws IOException {
    // restore uploaded files
    final Path uploadsBackupDir = Path.withoutSchemeAndAuthority(backupDir).resolve("uploads");
    FileSystem fs2 = homeFileStore.getFilesystemAndCreatePaths(hostname);
    fs2.delete(homeFileStore.getPath(), true);
    FileSystemUtils.copy(fs, uploadsBackupDir, fs2, homeFileStore.getInnerUploads(), false, false);
    try (final DirectoryStream<FileAttributes> directoryStream = FileSystemUtils.listRecursive(fs, uploadsBackupDir, PathFilters.ALL_FILES)) {
      for (FileAttributes attributes: directoryStream) {
        if (attributes.isRegularFile()) {
          backupStats.incrementFiles();
        }
      }
    }
  }


  /**
   * Options for doing backup/restore.
   */
  public static class BackupOptions {
    private final String backupDir;
    private final boolean binary;
    private final boolean includeProfiles;

    @JsonCreator
    public BackupOptions(@JsonProperty("backupDir") String backupDir, @JsonProperty("binary") boolean binary, @JsonProperty("includeProfiles") boolean includeProfiles) {
      super();
      this.backupDir = backupDir;
      this.binary = binary;
      this.includeProfiles = includeProfiles;
    }

    public String getBackupDir() {
      return backupDir;
    }

    @JsonIgnore
    public Path getBackupDirAsPath() {
      return com.dremio.io.file.Path.of(backupDir);
    }

    public boolean isBinary() {
      return binary;
    }

    public boolean isIncludeProfiles() {
      return includeProfiles;
    }
  }

  public static BackupStats createBackup(FileSystem fs, BackupOptions options, LocalKVStoreProvider localKVStoreProvider, HomeFileConf homeFileStore) throws IOException, NamespaceException {
    final Date now = new Date();
    final BackupStats backupStats = new BackupStats();

    final Path backupDir = options.getBackupDirAsPath().resolve(format("%s%s", BACKUP_DIR_PREFIX, DATE_FORMAT.format(now)));
    fs.mkdirs(backupDir, DEFAULT_PERMISSIONS);
    backupStats.backupPath = backupDir.toURI().getPath();
    ExecutorService svc = Executors.newFixedThreadPool(Math.max(1, Runtime.getRuntime().availableProcessors()/2));
    try {

      List<CompletableFuture<Void>> futures = new ArrayList<>();
      futures.addAll(localKVStoreProvider.getStores().entrySet().stream().map((entry) -> asFuture(svc, entry, fs, backupDir, options, backupStats)).collect(Collectors.toList()));
      futures.add(CompletableFuture.runAsync(() -> {
        try {
          backupUploadedFiles(fs, backupDir, homeFileStore, backupStats);
        } catch (IOException | NamespaceException ex) {
          throw new CompletionException(ex);
        }
      }, svc));
      checkFutures(futures);
    } finally {
      CloseableSchedulerThreadPool.close(svc, logger);
    }
    return backupStats;
  }

  private static void checkFutures(List<CompletableFuture<Void>> futures) throws IOException, NamespaceException {
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
    } catch (InterruptedException e) {
      new RuntimeException(e);
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class, NamespaceException.class);
      throw new RuntimeException(e.getCause());
    }
  }

  private static CompletableFuture<Void> asFuture(Executor e, Map.Entry<KVStoreInfo, CoreKVStore<?,?>> entry, FileSystem fs, Path backupDir, BackupOptions options, BackupStats backupStats) {
    return CompletableFuture.runAsync(() -> {
      try {
        final KVStoreInfo kvstoreInfo = entry.getKey();
        if (TokenUtils.TOKENS_TABLE_NAME.equals(kvstoreInfo.getTablename())) {
          // Skip creating a backup of tokens table
          // TODO: In the future, if there are other tables that should not be backed up, this could be part of
          // StoreBuilderConfig interface
          return;
        }

        if (LocalProfileStore.PROFILES_NAME.equals(kvstoreInfo.getTablename()) && !options.isIncludeProfiles()){
          return;
        }

        final BackupFileInfo backupFileInfo = new BackupFileInfo().setKvstoreInfo(kvstoreInfo);
        dumpTable(fs, backupDir, backupFileInfo, entry.getValue(), options.isBinary());
        backupStats.incrementTables();
      } catch(IOException ex) {
        throw new CompletionException(ex);
      }
    }, e);
  }

  public static BackupStats restore(FileSystem fs, Path backupDir, DACConfig dacConfig) throws Exception {
    final String dbDir = dacConfig.getConfig().getString(DremioConfig.DB_PATH_STRING);
    URI uploads = dacConfig.getConfig().getURI(DremioConfig.UPLOADS_PATH_STRING);
    File dbPath = new File(dbDir);

    if (!dbPath.isDirectory() || dbPath.list().length > 0) {
      throw new IllegalArgumentException(format("Path %s must be an empty directory.", dbDir));
    }

    final ScanResult scan = ClassPathScanner.fromPrescan(dacConfig.getConfig().getSabotConfig());
    final LocalKVStoreProvider localKVStoreProvider = new LocalKVStoreProvider(scan, dbDir, false, true);

    // We need to initialize the adapter so that we can instantiate any legacy KV store tables.
    // Note that closing the adapter implicitly closes the wrapped provider.
    try (final LegacyKVStoreProvider legacyStub = new LegacyKVStoreProviderAdapter(localKVStoreProvider, scan)) {
      legacyStub.start();

      // TODO after we add home file store type to configuration make sure we change homefile store construction.
      if (uploads.getScheme().equals("pdfs")) {
        uploads = UriBuilder.fromUri(uploads).scheme("file").build();
      }

      final HomeFileConf homeFileConf = new HomeFileConf(uploads.toString());
      homeFileConf.getFilesystemAndCreatePaths(null);
      Map<String, BackupFileInfo> tableToInfo = scanInfoFiles(fs, backupDir);
      Map<String, Path> tableToBackupFiles = scanBackupFiles(fs, backupDir, tableToInfo);
      final BackupStats backupStats = new BackupStats();
      backupStats.backupPath = backupDir.toURI().getPath();

      try (CloseableThreadPool ctp = new CloseableThreadPool("restore")) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        futures.addAll(tableToInfo.keySet().stream().map(table -> {
          return CompletableFuture.runAsync(() -> {
            BackupFileInfo info = tableToInfo.get(table);
            final CoreKVStore<?, ?> store = localKVStoreProvider.getStore(info.getKvstoreInfo());
            try {
              restoreTable(fs, store, tableToBackupFiles.get(table), info.getBinary(), info.getRecords());
            } catch (IOException e) {
              throw new CompletionException(e);
            }
            backupStats.incrementTables();
          }, ctp);
        }).collect(Collectors.toList()));

        futures.add(CompletableFuture.runAsync(() -> {
          try {
            restoreUploadedFiles(fs, backupDir, homeFileConf, backupStats, dacConfig.getConfig().getThisNode());
          } catch (IOException e) {
            throw new CompletionException(e);
          }
        }, ctp));

        checkFutures(futures);

      }

      return backupStats;
    }
  }

  /**
   * Checks that directory exists and write permission is granted.
   * @param fs - file system
   * @param directory - directory to check
   * @throws IOException
   */
  public static void checkOrCreateDirectory(FileSystem fs, Path directory)
    throws IOException {
    // Checking if directory already exists and that the daemon can access it
    if (!fs.exists(directory)) {
      // Checking if parent already exists and has the right permissions
      Path parent = directory.getParent();
      if (!fs.exists(parent)) {
        throw new IllegalArgumentException(format("Parent directory %s does not exist.", parent));
      }
      if (!fs.isDirectory(parent)) {
        throw new IllegalArgumentException(format("Path %s is not a directory.", parent));
      }
      try {
        fs.access(parent, EnumSet.of(AccessMode.WRITE, AccessMode.EXECUTE));
      } catch(AccessControlException e) {
        throw new IllegalArgumentException(format("Cannot create directory %s: check parent directory permissions.", directory), e);
      }
      fs.mkdirs(directory);
    }
    try {
      fs.access(directory, EnumSet.allOf(AccessMode.class));
    } catch(org.apache.hadoop.security.AccessControlException e) {
      throw new IllegalArgumentException(format("Path %s is not accessible/writeable.", directory), e);
    }
  }


  /**
   * Stats for backup/restore.
   */
  public static final class BackupStats {
    private String backupPath = null;
    private AtomicLong tables = new AtomicLong(0);
    private AtomicLong files = new AtomicLong(0);

    public BackupStats() {
    }

    @JsonCreator
    public BackupStats(
      @JsonProperty("backupPath") String backupPath,
      @JsonProperty("tables") long tables,
      @JsonProperty("files") long files) {
      this.backupPath = backupPath;
      this.tables = new AtomicLong(tables);
      this.files = new AtomicLong(files);
    }

    public String getBackupPath() {
      return backupPath;
    }

    public long getTables() {
      return tables.get();
    }

    public void incrementFiles() {
      files.incrementAndGet();
    }

    public void incrementTables() {
      tables.incrementAndGet();
    }
    public long getFiles() {
      return files.get();
    }
  }

}
