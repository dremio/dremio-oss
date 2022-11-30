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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nonnull;

import org.apache.commons.io.FileUtils;
import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Operations and constant definitions to support Backups of datastores.
 */
public final class BackupSupport {

  private static final Logger LOGGER = LoggerFactory.getLogger(BackupSupport.class);

  private static final Set<PosixFilePermission> DEFAULT_CHECKPOINT_DIR_PERMISSIONS = EnumSet.of(
    PosixFilePermission.OWNER_READ,
    PosixFilePermission.OWNER_WRITE,
    PosixFilePermission.OWNER_EXECUTE
  );

  private static final String DATE_FORMAT_PATTERN = "yyyy-MM-dd_HH.mm";

  private static final DateTimeFormatter BACKUP_DIR_SUFFIX_DATE_FORMAT =
    DateTimeFormatter.ofPattern(DATE_FORMAT_PATTERN);

  private static final String BACKUP_DIR_PREFIX = "dremio_backup_";

  @VisibleForTesting
  static Function<RocksDB, Checkpoint> checkpointCreator = Checkpoint::create;

  private BackupSupport() {
  }

  /**
   * Creates a string representing the name of a valid backup directory using
   * the {@link LocalDateTime} provided as parameter.
   *
   * <p><br/>The output string is in the format:</p>
   * <ul>
   *   <li>Prefix "dremio_backup_" (see {@link BackupSupport#BACKUP_DIR_PREFIX})</li>
   *   <li>Suffix with the date/time formatted like "yyyy-MM-dd_HH.mm" (see
   *       {@link BackupSupport#DATE_FORMAT_PATTERN})</li>
   * </ul>
   *
   * <p>The date/time suffix is in minutes scale. Different {@link LocalDateTime}s
   * instances provided that differ in a unit lesser than or igual seconds will
   * produce the same string as result.</p>
   *
   * @param dateTime Date/time used to format the suffix of the backup
   *                 destination directory name.
   * @return
   */
  public static String backupDestinationDirName(final LocalDateTime dateTime) {
    return format("%s%s", BACKUP_DIR_PREFIX, BACKUP_DIR_SUFFIX_DATE_FORMAT.format(Objects.requireNonNull(dateTime)));
  }

  /**
   * Utility method to, given a string, extract a {@link LocalDateTime} from it.
   *
   * <p>This method is useful to extract the date/time from string that was
   * created using {@link BackupSupport#backupDestinationDirName(LocalDateTime)}</p>
   *
   * @param directoryName String following the pattern returned by
   *                      {@link BackupSupport#backupDestinationDirName(LocalDateTime)}
   * @return a {@link LocalDateTime} object with precision up to minutes. Any
   * scale below minus (i.e. seconds) are set to 0.
   */
  public static LocalDateTime extractDateTimeFromSuffix(final String directoryName) {
    if (directoryName == null) {
      throw new DateTimeParseException("Directory name cannot be null", "", 0);
    }
    boolean matchPrefix = directoryName.startsWith(BACKUP_DIR_PREFIX);

    if (matchPrefix) {
      int prefixIndex = BACKUP_DIR_PREFIX.length();
      String dirDate = directoryName.substring(prefixIndex);
      try {
        return LocalDateTime.from(BACKUP_DIR_SUFFIX_DATE_FORMAT.parse(dirDate));
      } catch (DateTimeParseException e) {
        throw new DateTimeParseException(format("Directory name '%s' does not have a date suffix", directoryName),
          directoryName, BACKUP_DIR_PREFIX.length() + e.getErrorIndex());
      }
    }

    int prefixNotMatchIndex = 0;
    throw new DateTimeParseException(format("Directory name '%s' does not match a backup directory name prefix",
      directoryName), directoryName, prefixNotMatchIndex);
  }

  /**
   * Creates a new RocksDB and Blob store checkpoint (snapshot).
   *
   * <p>The checkpoint is a copy of all SSTables and the current Manifest file
   * stored in a different filesystem path. It will also create a checkpoint
   * of the blob stores associated to the RocksDB data.</p>
   *
   * <p>For the blob store, the checkpoint is a collection of hard links.</p>
   *
   * <p>The destination directory path of the checkpoint is created into the
   * <{@code dataBaseDirectory}>/checkpoints directory.
   * <p>
   * from the method call into {@link CheckpointInfo}. The caller can use the
   * directory path.</p>
   *
   * @param backupDir           Directory path where the backup will be created.
   * @param dataBaseDirectory   Database base directory.
   * @param catalogStoreDirName The name of the directory with the catalog
   *                            database within the base directory.
   * @param blobDirName         The name of the folder with the blobs within the
   *                            base directory.
   * @param db                  RocksDB instance from where the checkpoint will
   *                            be created.
   * @return a {@link CheckpointInfo} with information of the checkpoint.
   */
  public static synchronized CheckpointInfo newCheckpoint(@Nonnull Path backupDir, @Nonnull String dataBaseDirectory,
    @Nonnull String catalogStoreDirName, @Nonnull String blobDirName, @Nonnull RocksDB db) {
    final Path basePath = Paths.get(dataBaseDirectory);

    final Path checkpointRootDir = createCheckpointRelatedDir(basePath, Paths.get("checkpoints"));
    final Path newCheckpointDir = createCheckpointRelatedDir(checkpointRootDir, backupDir.getFileName());
    deleteDanglingCheckpoints(checkpointRootDir, newCheckpointDir);

    CheckpointInfo checkpointInfo = new ImmutableCheckpointInfo.Builder()
      .setBackupDestinationDir(backupDir.toUri().getPath())
      .setCheckpointPath(newCheckpointDir.toUri().getPath())
      .build();

    checkpointBlobStore(checkpointInfo, dataBaseDirectory, blobDirName);
    checkpointRocksDB(checkpointInfo, catalogStoreDirName, db, checkpointCreator);

    return checkpointInfo;
  }

  private static @Nonnull Path createCheckpointRelatedDir(@Nonnull final Path rootPath, final Path newDirName) {
    final Path newDirPath = rootPath.resolve(newDirName);

    if (Files.exists(newDirPath)) {
      return newDirPath;
    }

    try {
      return Files.createDirectory(newDirPath,
        PosixFilePermissions.asFileAttribute(DEFAULT_CHECKPOINT_DIR_PERMISSIONS));
    } catch (IOException e) {
      throw new RuntimeException(format("Checkpoint directory '%s' could not be created",
        newDirPath.toUri().getPath()), e);
    }
  }

  /**
   * Deletes dangling checkpoint directories.
   *
   * <p>A checkpoint could be in a dangling state if an error in a previous
   * checkpoint attempt happen.</p>
   *
   * <p>Calling this method implies that a new checkpoint is being executed.</p>
   *
   * @param checkpointRootDir    The root directory where all checkpoints are
   *                             stored.
   * @param currentCheckpointDir The current checkpoint directory being taken.
   */
  private static void deleteDanglingCheckpoints(@Nonnull final Path checkpointRootDir,
    @Nonnull final Path currentCheckpointDir) {
    if (!Files.exists(checkpointRootDir)) {
      return;
    }

    LOGGER.debug("Dangling checkpoints deletion started");
    final LocalDateTime now = LocalDateTime.now(ZoneId.systemDefault());
    try (final DirectoryStream<Path> paths = Files.newDirectoryStream(checkpointRootDir, Files::isDirectory)) {
      for (Path path : paths) {
        final String directoryName = path.getFileName().toString();
        if (currentCheckpointDir.equals(path)) {
          // skip current checkpoint
          continue;
        }
        if (isDanglingCheckpoint(now, directoryName)) {
          try {
            FileUtils.deleteDirectory(path.toFile());
          } catch (IOException e) {
            //Let's not fail the checkpoint if it is not possible to remove
            //an older checkpoint
            LOGGER.warn("Dangling checkpoint directory '{}' not deleted", path.toUri().getPath(), e);
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(format("Could not create directory stream for path '%s'",
        checkpointRootDir.toUri().getPath()), e);
    } finally {
      LOGGER.debug("Dangling checkpoints deletion finished");
    }
  }

  private static boolean isDanglingCheckpoint(@Nonnull LocalDateTime baseDateTime, @Nonnull String directoryName) {
    LocalDateTime dirDateSuffix = null;
    try {
      dirDateSuffix = extractDateTimeFromSuffix(directoryName);
    } catch (DateTimeParseException e) {
      LOGGER.debug("Directory '{}' into checkpoint directory does not have a valid checkpoint name. It will be " +
        "skipped" +
        " and not delete as a dangling checkpoint. Checkpoint directory names must end with a suffix in the format " +
        "'{}'", directoryName, DATE_FORMAT_PATTERN);
      return false;
    }

    /*
     * Default number of days that will be subtracted from the current day and
     * compared to backups directory names. Used during dangling checkpoints check.
     */
    long defaultPreviousDays = 1L;
    long dateShiftNanos = 1L;

    return baseDateTime.truncatedTo(ChronoUnit.MINUTES)
      .minusDays(defaultPreviousDays)
      .minusNanos(dateShiftNanos)
      .isBefore(dirDateSuffix);
  }

  private static void checkpointBlobStore(CheckpointInfo info, String baseDirectory, String blobPath) {
    final Path baseDir = Paths.get(baseDirectory);
    final Path blobStorePath = baseDir.resolve(blobPath);
    final Path checkpointDir = Paths.get(info.getCheckpointPath());
    final Path destinationDir = createCheckpointRelatedDir(checkpointDir, Paths.get(blobPath));

    LOGGER.debug("Blob store checkpoint started");
    createHardLinkForDir(blobStorePath, destinationDir);
    LOGGER.debug("Blob store checkpoint finished");
  }

  private static void createHardLinkForDir(Path sourceDir, Path destinationDir) {
    if (!Files.isDirectory(sourceDir)) {
      throw new RuntimeException(format("Cannot create hard links from the source '%s' as it is not a directory",
        sourceDir));
    }
    if (!Files.isDirectory(destinationDir)) {
      throw new RuntimeException(format("Cannot create hard links to the destination '%s' as it is not a directory",
        destinationDir));
    }

    try (DirectoryStream<Path> stream = Files.newDirectoryStream(sourceDir)) {
      for (Path path : stream) {
        final Path filename = path.getFileName();
        final Path newFile = destinationDir.resolve(filename);
        if (!Files.isDirectory(path)) {
          Files.createLink(newFile, path);
        } else {
          final Path directory = Files.createDirectory(newFile,
            PosixFilePermissions.asFileAttribute(DEFAULT_CHECKPOINT_DIR_PERMISSIONS));
          createHardLinkForDir(path, directory);
        }
      }
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Path not found while traversing directory", e);
    } catch (IOException e) {
      throw new RuntimeException(format("Could not create hard links from '%s' to '%s", sourceDir, destinationDir), e);
    }

  }

  private static void checkpointRocksDB(CheckpointInfo info, String catalogStoreName, RocksDB db,
    Function<RocksDB, Checkpoint> checkpointCreator) {
    LOGGER.debug("RocksDB checkpoint started");
    final String catalogPath = info.getCheckpointPath() + "/" + catalogStoreName;
    try (final Checkpoint checkpoint = checkpointCreator.apply(db)) {
      checkpoint.createCheckpoint(catalogPath);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      LOGGER.debug("RocksDB checkpoint finished");
    }
  }
}
