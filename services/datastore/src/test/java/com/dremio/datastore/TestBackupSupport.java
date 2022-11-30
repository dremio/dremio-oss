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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributeView;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDB;

import com.dremio.test.DremioTest;
import com.google.common.collect.ImmutableList;

@RunWith(Enclosed.class)
public class TestBackupSupport extends DremioTest {
  private static final String DIR_PREFIX = "dremio_backup_";

  @RunWith(Parameterized.class)
  public static class BackupDirTests {
    private final Param<LocalDateTime, String, Class<? extends Throwable>> param;

    @Parameterized.Parameters(name = "Date {0}")
    public static List<Param<LocalDateTime, String, Class<? extends Throwable>>> params() {
      return ImmutableList.of(
        new Param<LocalDateTime, String, Class<? extends Throwable>>(null, NullPointerException.class),
        new Param<LocalDateTime, String, Class<? extends Throwable>>(LocalDate.of(2022, 6, 8).atStartOfDay(),
          formatDir("2022-06-08_00.00")),
        new Param<LocalDateTime, String, Class<? extends Throwable>>(LocalDateTime.of(2022, 7, 9, 10, 20), formatDir(
          "2022-07-09_10.20"))
      );
    }

    public BackupDirTests(Param<LocalDateTime, String, Class<? extends Throwable>> param) {
      this.param = param;
    }

    @Test
    public void test() {
      if (param.expectedValue != null) {
        assertEquals(param.expectedValue, BackupSupport.backupDestinationDirName(param.value));
      } else {
        Assert.assertThrows(param.expectedException,
          () -> BackupSupport.backupDestinationDirName(param.value));
      }
    }

  }

  @RunWith(Parameterized.class)
  public static class ExtractDateTimeFromSuffixTests {

    private final Param<String, LocalDateTime, Class<? extends Throwable>> param;

    @Parameterized.Parameters(name = "directory name = {0}")
    public static List<Param<String, LocalDateTime, Class<? extends Throwable>>> params() {
      return ImmutableList.of(
        new Param<String, LocalDateTime, Class<? extends Throwable>>(null, DateTimeParseException.class),
        new Param<String, LocalDateTime, Class<? extends Throwable>>("", DateTimeParseException.class),
        new Param<String, LocalDateTime, Class<? extends Throwable>>("invalidValue", DateTimeParseException.class),
        new Param<String, LocalDateTime, Class<? extends Throwable>>("invalidPrefix2022-06-08_00.00",
          DateTimeParseException.class),
        new Param<String, LocalDateTime, Class<? extends Throwable>>(formatDir("invalidSuffix"),
          DateTimeParseException.class),
        new Param<String, LocalDateTime, Class<? extends Throwable>>(formatDir("2022-06-08_00.00"),
          LocalDateTime.of(2022, 6, 8, 0, 0)),
        new Param<String, LocalDateTime, Class<? extends Throwable>>(formatDir("2022-07-09_10.20"),
          LocalDateTime.of(2022, 7, 9, 10, 20))
      );
    }

    public ExtractDateTimeFromSuffixTests(
      Param<String, LocalDateTime, Class<? extends Throwable>> param) {
      this.param = param;
    }

    @Test
    public void test() {
      if (param.expectedValue != null) {
        assertEquals(param.expectedValue, BackupSupport.extractDateTimeFromSuffix(param.value));
      } else {
        Assert.assertThrows(param.expectedException,
          () -> BackupSupport.extractDateTimeFromSuffix(param.value));
      }
    }

  }

  public static class NewCheckpointTests extends DremioTest {

    @ClassRule
    public static TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    private static final String BLOB_PATH = "blob";
    private static final String CATALOG_STORE_NAME = "catalog";

    @Test
    public void createValidCheckpoint() throws IOException {
      File backupRootDir = TEMP_FOLDER.newFolder();
      File baseDir = TEMP_FOLDER.newFolder();

      LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));
      String bkpName = BackupSupport.backupDestinationDirName(now);
      Path backupDir = backupRootDir.toPath().resolve(bkpName);
      String baseDirectory = baseDir.getAbsolutePath();
      Path origBlobPath = prepareBlob(baseDir, "dir1", "dir2");
      RocksDB db = mock(RocksDB.class);
      when(db.isOwningHandle()).thenReturn(true);
      // let's assume RocksDB tested its own Checkpoint feature
      BackupSupport.checkpointCreator = rocksDB -> mock(Checkpoint.class);

      CheckpointInfo checkpointInfo = BackupSupport.newCheckpoint(backupDir, baseDirectory, CATALOG_STORE_NAME,
        BLOB_PATH, db);

      assertEquals(baseDirectory + "/checkpoints/" + bkpName + "/", checkpointInfo.getCheckpointPath());
      Path checkpointBlobPath = Paths.get(checkpointInfo.getCheckpointPath()).resolve(BLOB_PATH);
      assertBlobHardLinks(origBlobPath, checkpointBlobPath);
    }

    @Test
    public void danglingCheckpointsRemoved() throws IOException {
      File backupRootDir = TEMP_FOLDER.newFolder();
      File baseDir = TEMP_FOLDER.newFolder();

      LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));
      LocalDateTime dayBefore = now.minusDays(1);
      LocalDateTime withinDay = now.minusHours(1);
      String danglingDirName = BackupSupport.backupDestinationDirName(dayBefore);
      String dirWithinDayName = BackupSupport.backupDestinationDirName(withinDay);
      Path danglingDirPath = createDanglingDir(danglingDirName, baseDir);
      assertTrue("Dangling dir should exist", Files.exists(danglingDirPath));

      String bkpName = BackupSupport.backupDestinationDirName(now);
      Path backupDir = backupRootDir.toPath().resolve(bkpName);
      String baseDirectory = baseDir.getAbsolutePath();
      Path origBlobPath = prepareBlob(baseDir, "dir1", "dir2");
      RocksDB db = mock(RocksDB.class);
      when(db.isOwningHandle()).thenReturn(true);
      // let's assume RocksDB tested its own Checkpoint feature
      BackupSupport.checkpointCreator = rocksDB -> mock(Checkpoint.class);

      CheckpointInfo checkpointInfo = BackupSupport.newCheckpoint(backupDir, baseDirectory, CATALOG_STORE_NAME,
        BLOB_PATH, db);

      Path checkpointsDirPath = Paths.get(checkpointInfo.getCheckpointPath()).getParent();
      assertFalse("Dangling dir should not exist", Files.exists(checkpointsDirPath.resolve(danglingDirName)));
    }

    @Test
    public void alienCheckpointPresentShouldNotDeleteDir() throws IOException {
      File backupRootDir = TEMP_FOLDER.newFolder();
      File baseDir = TEMP_FOLDER.newFolder();
      Path alienDir = createDanglingDir("dremio_backup_abc", baseDir);

      LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));
      String bkpName = BackupSupport.backupDestinationDirName(now);
      Path backupDir = backupRootDir.toPath().resolve(bkpName);
      String baseDirectory = baseDir.getAbsolutePath();
      Path origBlobPath = prepareBlob(baseDir, "dir1", "dir2");
      RocksDB db = mock(RocksDB.class);
      when(db.isOwningHandle()).thenReturn(true);
      // let's assume RocksDB tested its own Checkpoint feature
      BackupSupport.checkpointCreator = rocksDB -> mock(Checkpoint.class);

      BackupSupport.newCheckpoint(backupDir, baseDirectory, CATALOG_STORE_NAME,
        BLOB_PATH, db);

      assertTrue("Alien directory must be present", Files.exists(alienDir));
    }

    private Path createDanglingDir(String dirName, File baseDir) throws IOException {
      Path checkpointsDir = Paths.get(baseDir.getAbsolutePath()).resolve("checkpoints").resolve(dirName);
      return Files.createDirectories(checkpointsDir);
    }

    private void assertBlobHardLinks(Path origBlobPath, Path checkpointBlobPath) throws IOException {
      try (DirectoryStream<Path> paths = Files.newDirectoryStream(origBlobPath)) {
        for (Path path : paths) {
          try (DirectoryStream<Path> blobs = Files.newDirectoryStream(path)) {
            for (Path blob : blobs) {
              Path blobGroupDir = blob.getParent().getFileName();
              Path blobOrigFileName = blob.getFileName();
              Path checkpointBlob = checkpointBlobPath.resolve(blobGroupDir).resolve(blobOrigFileName);
              assertFalse("Blob should not be a symbolic link", Files.getFileAttributeView(checkpointBlob,
                PosixFileAttributeView.class).readAttributes().isSymbolicLink());
              assertTrue("File content must mach", Arrays.equals(Files.readAllBytes(blob),
                Files.readAllBytes(checkpointBlob)));
            }
          }
        }
      }
    }

    private Path prepareBlob(File baseDir, String... blobGroups) throws IOException {
      Path basePath = baseDir.toPath();
      Path blobPath = basePath.resolve("blob");
      Files.createDirectories(blobPath);

      int size = 5 * 1024;
      for (String blobGroup : blobGroups) {
        Path blobGroupPath = blobPath.resolve(blobGroup);
        Files.createDirectory(blobGroupPath);
        createFile(blobGroupPath, size);
      }
      return blobPath;
    }

    private void createFile(Path path, int size) throws IOException {
      UUID uuid = UUID.randomUUID();
      Path filePath = Paths.get(path.toUri().getPath(), uuid + ".blob");
      Files.createFile(filePath);
      Random random = ThreadLocalRandom.current();
      try (OutputStream fileOS = Files.newOutputStream(filePath)) {
        byte[] data = new byte[size];
        random.nextBytes(data);
        fileOS.write(data);
      }
    }

  }

  private static String formatDir(String date) {
    return format("%s%s", DIR_PREFIX, date);
  }

  private static class Param<T, A, E extends Class<? extends Throwable>> {
    private final T value;
    private final A expectedValue;
    private final E expectedException;

    Param(T value, A expectedValue) {
      this(value, expectedValue, null);
    }

    Param(T value, E expectedException) {
      this(value, null, expectedException);
    }

    private Param(T value, A expectedValue, E expectedException) {
      this.value = value;
      this.expectedValue = expectedValue;
      this.expectedException = expectedException;
    }

    @Override
    public String toString() {
      return value != null ? value.toString() : "null";
    }

  }

}
