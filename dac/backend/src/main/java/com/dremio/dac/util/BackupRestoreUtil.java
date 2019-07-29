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
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

import javax.ws.rs.core.UriBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.AccessControlException;

import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.utils.ProtostuffUtil;
import com.dremio.config.DremioConfig;
import com.dremio.dac.homefiles.HomeFileConf;
import com.dremio.dac.proto.model.backup.BackupFileInfo;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.server.tokens.TokenUtils;
import com.dremio.datastore.CoreKVStore;
import com.dremio.datastore.DataStoreUtils;
import com.dremio.datastore.KVStoreTuple;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.StoreBuilderConfig;
import com.dremio.exec.store.dfs.PseudoDistributedFileSystem;
import com.dremio.service.namespace.NamespaceException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Backup Service running only on master.
 */
public final class BackupRestoreUtil {
  private static final FsPermission DEFAULT_PERMISSIONS = new FsPermission(FsAction.ALL, FsAction.READ, FsAction.EXECUTE);

  private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd_HH.mm");
  private static final String BACKUP_DIR_PREFIX = "dremio_backup_";
  private static final String BACKUP_FILE_SUFFIX = "_backup.json";
  private static final String BACKUP_INFO_FILE_SUFFIX = "_info.json";

  private static final GlobFilter BACKUP_FILES_GLOB = DataStoreUtils.getGlobFilter(BACKUP_FILE_SUFFIX);
  private static final GlobFilter BACKUP_INFO_FILES_GLOB = DataStoreUtils.getGlobFilter(BACKUP_INFO_FILE_SUFFIX);

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

  private static <K, V> void dumpTable(FileSystem fs, Path backupRootDir, BackupFileInfo backupFileInfo, CoreKVStore<K, V> coreKVStore) throws IOException {
    final Path backupFile = new Path(backupRootDir, format("%s%s", backupFileInfo.getKvstoreInfo().getTablename(), BACKUP_FILE_SUFFIX));
    final ObjectMapper objectMapper = new ObjectMapper();
    final FSDataOutputStream fsout = fs.create(backupFile, true);
    final CheckedOutputStream checkedOutputStream = new CheckedOutputStream(fsout, new CRC32());
    final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(checkedOutputStream));
    final Iterator<Map.Entry<KVStoreTuple<K>, KVStoreTuple<V>>> iterator = coreKVStore.find().iterator();
    long records = 0;
    try {
      while (iterator.hasNext()) {
        Map.Entry<KVStoreTuple<K>, KVStoreTuple<V>> keyval = iterator.next();
        writer.write(objectMapper.writeValueAsString(new BackupRecord(keyval.getKey().toJson(), keyval.getValue().toJson())));
        writer.newLine();
        ++records;
      }
    } catch (IOException ioe) {
      throw ioe;
    } finally {
      try {
        writer.close();
        checkedOutputStream.close();
        fsout.close();
      } catch (IOException ioe) {
        throw ioe;
      }
    }
    // write info file after backup file was successfully created and closed.
    backupFileInfo.setChecksum(checkedOutputStream.getChecksum().getValue());
    backupFileInfo.setRecords(records);
    final Path backupInfoFile = new Path(backupRootDir, format("%s%s", backupFileInfo.getKvstoreInfo().getTablename(), BACKUP_INFO_FILE_SUFFIX));
    try (FSDataOutputStream backupInfoOut = fs.create(backupInfoFile, true)) {
      ProtostuffUtil.toJSON(backupInfoOut, backupFileInfo, BackupFileInfo.getSchema(), false);
    }
  }

  private static void validateFileWithChecksum(FileSystem fs, Path filePath, BackupFileInfo backupFileInfo) throws IOException {
    final CheckedInputStream cin = new CheckedInputStream(fs.open(filePath), new CRC32());
    final BufferedReader reader = new BufferedReader(new InputStreamReader(cin));
    final ObjectMapper objectMapper = new ObjectMapper();
    String line;
    long records = 0;
    // parse records just to make sure formatting is correct
    while ((line = reader.readLine()) != null) {
      objectMapper.readValue(line, BackupRecord.class);
      ++records;
    }
    cin.close();
    long found = cin.getChecksum().getValue();
    if (backupFileInfo.getChecksum() != found) {
      throw new IOException(format("Corrupt backup data file %s. Expected checksum %x, found %x", filePath, backupFileInfo.getChecksum(), found));
    }
    if (backupFileInfo.getRecords() != records) {
      throw new IOException(format("Corrupt backup data file %s. Expected records %x, found %x", filePath, backupFileInfo.getRecords(), records));
    }
  }

  private static <K, V> void restoreTable(FileSystem fs, CoreKVStore<K, V> coreKVStore, Path filePath) throws IOException {
    final BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)));
    final ObjectMapper objectMapper = new ObjectMapper();
    String line;
    try {
      while ((line = reader.readLine()) != null) {
        final BackupRecord record = objectMapper.readValue(line, BackupRecord.class);
        final KVStoreTuple<K> key = coreKVStore.newKey();
        key.setObject(key.fromJson(record.getKey()));

        final KVStoreTuple<V> value = coreKVStore.newValue();
        value.setObject(value.fromJson(record.getValue()));

        coreKVStore.put(key, value);
      }
    } finally {
      try {
        reader.close();
      } catch (IOException ioe) {
      }
    }
  }

  private static Map<String, BackupFileInfo> scanInfoFiles(FileSystem fs, Path backupDir) throws IOException {
    final Map<String, BackupFileInfo> tableToInfo = Maps.newHashMap();
    final FileStatus[] backupFiles = fs.listStatus(backupDir, BACKUP_INFO_FILES_GLOB);
    for (FileStatus backupFile : backupFiles) {
      final String tableName = getTableName(backupFile.getPath().getName(), BACKUP_INFO_FILE_SUFFIX);
      // read backup info file
      final byte[] headerBytes = new byte[(int) backupFile.getLen()];
      IOUtils.readFully(fs.open(backupFile.getPath()), headerBytes, 0, headerBytes.length);
      final BackupFileInfo backupFileInfo = new BackupFileInfo();
      ProtostuffUtil.fromJSON(headerBytes, backupFileInfo, BackupFileInfo.getSchema(), false);
      tableToInfo.put(tableName, backupFileInfo);
    }
    return tableToInfo;
  }

  private static Map<String, Path> scanBackupFiles(FileSystem fs, Path backupDir, Map<String, BackupFileInfo> tableToInfo) throws IOException {
    final FileStatus[] backupDataFiles = fs.listStatus(backupDir, BACKUP_FILES_GLOB);
    final Map<String, Path> tableToBackupData = Maps.newHashMap();
    for (FileStatus backupDataFile : backupDataFiles) {
      final String tableName = getTableName(backupDataFile.getPath().getName(), BACKUP_FILE_SUFFIX);
      if (tableToInfo.containsKey(tableName)) {
        tableToBackupData.put(tableName, backupDataFile.getPath());
      } else {
        throw new IOException("Missing metadata file for table " + tableName);
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

  @VisibleForTesting
  public static void validateBackupDir(FileSystem fs, Path backupDir) throws IOException {
    Map<String, BackupFileInfo> tableToInfo = scanInfoFiles(fs, backupDir);
    Map<String, Path> tableToBackupFiles = scanBackupFiles(fs, backupDir, tableToInfo);
    for (String table : tableToBackupFiles.keySet()) {
      validateFileWithChecksum(fs, tableToBackupFiles.get(table), tableToInfo.get(table));
    }
  }

  public static void backupUploadedFiles(FileSystem fs,
                                         Path backupDir,
                                         HomeFileConf homeFileStore,
                                         BackupStats backupStats) throws IOException, NamespaceException {
    final Path uploadsBackupDir = new Path(backupDir.toUri().getPath(), "uploads");
    fs.mkdirs(uploadsBackupDir);
    final Path uploadsDir = homeFileStore.getInnerUploads();
    copyFiles(homeFileStore.getFilesystemAndCreatePaths(null), uploadsDir, fs, uploadsBackupDir, homeFileStore.isPdfsBased(), new Configuration(), backupStats);
  }

  private static void copyFiles(FileSystem srcFs, Path srcPath, FileSystem dstFs, Path dstPath, boolean isPdfs, Configuration conf, BackupStats backupStats) throws IOException {
    for (FileStatus fileStatus : srcFs.listStatus(srcPath)) {

      if (fileStatus.isDirectory()) {
        final Path dstDir = new Path(dstPath, fileStatus.getPath().getName());
        dstFs.mkdirs(dstPath);
        copyFiles(srcFs, fileStatus.getPath(), dstFs, dstDir, isPdfs, conf, backupStats);
      } else {
        final Path dstFile;
        if (!isPdfs) {
          dstFile = new Path(dstPath, fileStatus.getPath().getName());
        } else {
          // strip off {host}@ from file name
          dstFile = new Path(dstPath, PseudoDistributedFileSystem.getRemoteFileName(fileStatus.getPath().getName()));
        }
        FileUtil.copy(srcFs, fileStatus.getPath(), dstFs, dstFile, false, conf);
        backupStats.files++;
      }
    }
  }


  public static void restoreUploadedFiles(FileSystem fs, Path backupDir, HomeFileConf homeFileStore, BackupStats backupStats, String hostname) throws IOException {
    // restore uploaded files
    final Path uploadsBackupDir = new Path(backupDir.toUri().getPath(), "uploads");
    FileSystem fs2 = homeFileStore.getFilesystemAndCreatePaths(hostname);
    fs2.delete(homeFileStore.getPath(), true);
    FileUtil.copy(fs, uploadsBackupDir, fs2, homeFileStore.getInnerUploads(), false, false, new Configuration());
    backupStats.files = fs.getContentSummary(backupDir).getFileCount();
  }

  public static BackupStats createBackup(FileSystem fs, Path backupRootDir, LocalKVStoreProvider localKVStoreProvider, HomeFileConf homeFileStore) throws IOException, NamespaceException {
    final Date now = new Date();
    final BackupStats backupStats = new BackupStats();

    final Path backupDir = new Path(backupRootDir, format("%s%s", BACKUP_DIR_PREFIX, DATE_FORMAT.format(now)));
    fs.mkdirs(backupDir, DEFAULT_PERMISSIONS);
    backupStats.backupPath = backupDir.toUri().getPath();

    for (Map.Entry<StoreBuilderConfig, CoreKVStore<?, ?>> entry : localKVStoreProvider.getStores().entrySet()) {
      final StoreBuilderConfig storeBuilderConfig = entry.getKey();
      if (TokenUtils.TOKENS_TABLE_NAME.equals(storeBuilderConfig.getName())) {
        // Skip creating a backup of tokens table
        // TODO: In the future, if there are other tables that should not be backed up, this could be part of
        // StoreBuilderConfig interface
        continue;
      }
      final BackupFileInfo backupFileInfo = new BackupFileInfo().setKvstoreInfo(DataStoreUtils.toInfo(storeBuilderConfig));
      dumpTable(fs, backupDir, backupFileInfo, entry.getValue());
      ++backupStats.tables;
    }
    backupUploadedFiles(fs, backupDir, homeFileStore, backupStats);
    return backupStats;
  }

  public static BackupStats restore(FileSystem fs, Path backupDir, DACConfig dacConfig) throws Exception {
    final String dbDir = dacConfig.getConfig().getString(DremioConfig.DB_PATH_STRING);
    URI uploads = dacConfig.getConfig().getURI(DremioConfig.UPLOADS_PATH_STRING);
    File dbPath = new File(dbDir);

    if (!dbPath.isDirectory() || dbPath.list().length > 0) {
      throw new IllegalArgumentException(format("Path %s must be an empty directory.", dbDir));
    }
    final LocalKVStoreProvider localKVStoreProvider = new LocalKVStoreProvider(ClassPathScanner.fromPrescan(dacConfig.getConfig().getSabotConfig()), dbDir, false, true, false, true);
    localKVStoreProvider.start();
    // TODO after we add home file store type to configuration make sure we change homefile store construction.
    if(uploads.getScheme().equals("pdfs")){
      uploads = UriBuilder.fromUri(uploads).scheme("file").build();
    }

    final HomeFileConf homeFileConf = new HomeFileConf(uploads.toString());
    homeFileConf.getFilesystemAndCreatePaths(null);
    Map<String, BackupFileInfo> tableToInfo = scanInfoFiles(fs, backupDir);
    Map<String, Path> tableToBackupFiles = scanBackupFiles(fs, backupDir, tableToInfo);
    final BackupStats backupStats = new BackupStats();
    backupStats.backupPath = backupDir.toUri().getPath();

    for (String table : tableToInfo.keySet()) {
      final StoreBuilderConfig storeBuilderConfig = DataStoreUtils.toBuilderConfig(tableToInfo.get(table).getKvstoreInfo());
      final CoreKVStore<?, ?> store = localKVStoreProvider.getOrCreateStore(storeBuilderConfig);
      restoreTable(fs, store, tableToBackupFiles.get(table));
      ++backupStats.tables;
    }

    restoreUploadedFiles(fs, backupDir, homeFileConf, backupStats, dacConfig.getConfig().getThisNode());
    localKVStoreProvider.close();

    return backupStats;
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
        fs.access(parent, FsAction.WRITE_EXECUTE);
      } catch(AccessControlException e) {
        throw new IllegalArgumentException(format("Cannot create directory %s: check parent directory permissions.", directory), e);
      }
      fs.mkdirs(directory);
    }
    try {
      fs.access(directory, FsAction.ALL);
    } catch(org.apache.hadoop.security.AccessControlException e) {
      throw new IllegalArgumentException(format("Path %s is not accessible/writeable.", directory), e);
    }
  }


  /**
   * Stats for backup/restore.
   */
  public static final class BackupStats {
    private String backupPath = null;
    private long tables = 0;
    private long files = 0;

    public BackupStats() {
    }

    @JsonCreator
    public BackupStats(
      @JsonProperty("backupPath") String backupPath,
      @JsonProperty("tables") long tables,
      @JsonProperty("files") long files) {
      this.backupPath = backupPath;
      this.tables = tables;
      this.files = files;
    }

    public String getBackupPath() {
      return backupPath;
    }

    public long getTables() {
      return tables;
    }

    public long getFiles() {
      return files;
    }
  }

  private static void checkExists(String name, File dir) {
    if (!dir.exists()) {
      throw new IllegalArgumentException(format("Directory %s for %s does not exist", dir.getAbsolutePath(), name));
    }
    if (!dir.isDirectory()) {
      throw new IllegalArgumentException(format("%s for %s is not a directory", dir.getAbsolutePath(), name));
    }
    if (!dir.canRead()) {
      throw new IllegalArgumentException(format("User does not have valid permissions for directory %s for %s", dir.getAbsolutePath(), name));
    }
  }

  private static void checkEmpty(String name, File dir) {
    checkExists(name, dir);
    if (dir.list().length > 0) {
      throw new IllegalArgumentException(format("Directory %s for %s must be empty", dir.getAbsolutePath(), name));
    }
    if (!dir.canWrite()) {
      throw new IllegalArgumentException(format("User does not have valid permissions for directory %s for %s", dir.getAbsolutePath(), name));
    }
  }

}
