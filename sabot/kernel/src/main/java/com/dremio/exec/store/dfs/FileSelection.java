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
package com.dremio.exec.store.dfs;

import static com.dremio.common.utils.PathUtils.removeLeadingSlash;
import static com.dremio.io.file.PathFilters.NO_HIDDEN_FILES;

import com.dremio.common.utils.PathUtils;
import com.dremio.exec.util.Utilities;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.FileSystemUtils;
import com.dremio.io.file.Path;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

/** Jackson serializable description of a file selection. */
public class FileSelection {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(FileSelection.class);
  public static final String PATH_SEPARATOR = System.getProperty("file.separator");
  private static final String WILD_CARD = "*";

  private ImmutableList<FileAttributes> fileAttributesList;
  private final String selectionRoot;
  private final Path originalRootPath;
  private final boolean
      isRootPathDirectory; // true if root path is a directory, false if it's a file

  private enum StatusType {
    NO_DIRS, // no directories in this selection
    EXPANDED, // whether this selection has been expanded to files
    NOT_EXPANDED // selection will only have selectionRoot info and no information about
    // sub-dir/subfiles
  }

  private StatusType dirStatus;
  private Optional<FileAttributes> firstFileAttributes = null;

  /**
   * Creates a {@link FileSelection selection} out of given file statuses/files and selection root.
   *
   * @param status
   * @param fileAttributesList
   * @param selectionRoot
   * @param originalRootPath
   */
  private FileSelection(
      StatusType status,
      final ImmutableList<FileAttributes> fileAttributesList,
      final String selectionRoot,
      final Path originalRootPath,
      final boolean isRootPathDirectory) {
    this.fileAttributesList = Preconditions.checkNotNull(fileAttributesList);
    this.selectionRoot = Preconditions.checkNotNull(selectionRoot);
    this.dirStatus = status;
    this.originalRootPath = originalRootPath;
    this.isRootPathDirectory = isRootPathDirectory;
  }

  public boolean isEmpty() {
    return fileAttributesList.isEmpty();
  }

  public String getSelectionRoot() {
    return selectionRoot;
  }

  public List<FileAttributes> getFileAttributesList() {
    return fileAttributesList;
  }

  public FileSelection minusDirectories() throws IOException {
    if (dirStatus == StatusType.NO_DIRS) {
      return this;
    }

    Stopwatch timer = Stopwatch.createStarted();
    final int total = fileAttributesList.size();
    final ImmutableList<FileAttributes> nonDirectories =
        ImmutableList.copyOf(
            Iterables.filter(fileAttributesList, Predicates.not(FileAttributes::isDirectory)));

    final FileSelection fileSel = create(StatusType.NO_DIRS, nonDirectories, selectionRoot);
    logger.debug(
        "FileSelection.minusDirectories() took {} ms, numFiles: {}",
        timer.elapsed(TimeUnit.MILLISECONDS),
        total);

    return fileSel;
  }

  public int getMaxDepth(FileAttributes rootStatus) {
    final int selectionDepth = rootStatus.getPath().depth();

    int maxDepth = 0;
    for (final FileAttributes status : getFileAttributesList()) {
      maxDepth = Math.max(maxDepth, status.getPath().depth() - selectionDepth);
    }
    return maxDepth;
  }

  public Optional<FileAttributes> getFirstFileIteratively(FileSystem fs) throws IOException {
    if (firstFileAttributes != null) {
      return firstFileAttributes;
    }
    if (isExpanded()) {
      firstFileAttributes =
          fileAttributesList.stream().filter(FileAttributes::isRegularFile).findFirst();
      return firstFileAttributes;
    }
    firstFileAttributes = getFirstFileIteratively(fs, originalRootPath);
    return firstFileAttributes;
  }

  public static Optional<FileAttributes> getFirstFileIteratively(FileSystem fs, Path path)
      throws IOException {
    if (fs.isFile(path)) {
      FileAttributes fileAttribute = fs.getFileAttributes(path);
      if (fileAttribute.isRegularAndNoHiddenFile()) {
        return Optional.of(fileAttribute);
      } else {
        return Optional.empty();
      }
    } else {
      if (!fs.exists(path)) {
        return Optional.empty();
      }
    }

    try (DirectoryStream<FileAttributes> stream = fs.listFiles(path, true)) {
      return StreamSupport.stream(stream.spliterator(), false)
          .filter(FileAttributes::isRegularAndNoHiddenFile)
          .findFirst();
    } catch (DirectoryIteratorException e) {
      throw e.getCause();
    }
  }

  public boolean isExpanded() {
    return dirStatus == StatusType.EXPANDED;
  }

  public boolean isNotExpanded() {
    return dirStatus == StatusType.NOT_EXPANDED;
  }

  public boolean isNoDirs() {
    return dirStatus == StatusType.NO_DIRS;
  }

  public static FileSelection create(FileAttributes fileAttributes) throws IOException {
    return new FileSelection(
        StatusType.EXPANDED,
        ImmutableList.of(fileAttributes),
        fileAttributes.getPath().toString(),
        fileAttributes.getPath(),
        fileAttributes.isDirectory());
  }

  public void expand(String datasetName, FileSystem fs, int maxFiles) throws IOException {
    if (dirStatus == StatusType.NOT_EXPANDED) {
      this.fileAttributesList =
          generateListOfFileAttributes(datasetName, fs, originalRootPath, maxFiles);
      this.dirStatus = StatusType.EXPANDED;
    }
  }

  public static Path getPathBasedOnFullPath(List<String> fullPath) {
    String parent = Joiner.on(PATH_SEPARATOR).join(fullPath.subList(0, fullPath.size() - 1));
    if (Strings.isNullOrEmpty(parent)) {
      parent = "/";
    }
    parent = Path.withoutSchemeAndAuthority(Path.of(Path.SEPARATOR).resolve(parent)).toString();
    String path = PathUtils.removeQuotes(fullPath.get(fullPath.size() - 1));
    return Path.of(parent).resolve(removeLeadingSlash(path));
  }

  public static FileSelection createWithFullSchemaNotExpanded(
      final FileSystem fs, final String parent, final String fullSchemaPath) throws IOException {
    final Path combined = Path.mergePaths(Path.of(parent), PathUtils.toFSPath(fullSchemaPath));
    return createNotExpanded(fs, combined);
  }

  public static FileSelection createNotExpanded(final FileSystem fs, final List<String> fullPath)
      throws IOException {
    return createNotExpanded(fs, getPathBasedOnFullPath(fullPath));
  }

  public static FileSelection createNotExpanded(final FileSystem fs, Path root) throws IOException {
    if (root == null || root.toString().isEmpty()) {
      throw new IllegalArgumentException("Selection root is null or empty" + root);
    }
    final Path rootPath = handleWildCard(root.toString());
    if (!fs.exists(rootPath)) {
      return null;
    }
    boolean isRootPathDirectory = fs.isDirectory(rootPath);
    final String selectionRoot = Path.withoutSchemeAndAuthority(rootPath).toString();
    return new FileSelection(
        StatusType.NOT_EXPANDED, ImmutableList.of(), selectionRoot, root, isRootPathDirectory);
  }

  // These set of methods create a FileSelection that is EXPANDED - that is, it does a recursive
  // listing
  // of a folder and holds the attributes of files in the folder in memory
  public static FileSelection create(
      final String datasetName, final FileSystem fs, final List<String> fullPath, int maxFiles)
      throws IOException {
    return create(datasetName, fs, getPathBasedOnFullPath(fullPath), maxFiles);
  }

  public static FileSelection create(
      final String datasetName, final FileSystem fs, Path combined, int maxFiles)
      throws IOException {
    final ImmutableList<FileAttributes> fileAttributes =
        generateListOfFileAttributes(datasetName, fs, combined, maxFiles);
    if (fileAttributes.isEmpty()) {
      return null;
    }
    final FileSelection fileSel = createFromExpanded(fileAttributes, combined.toURI().getPath());
    return fileSel;
  }

  public static FileSelection createFromExpanded(
      final ImmutableList<FileAttributes> fileAttributes, final String root) {
    return create(StatusType.EXPANDED, fileAttributes, root);
  }

  /**
   * Creates a {@link FileSelection selection} with the given file statuses and selection root.
   *
   * @param status status
   * @param fileAttributes list of file attributes
   * @param root root path for selections
   * @return null if creation of {@link FileSelection} fails with an {@link
   *     IllegalArgumentException} otherwise a new selection.
   */
  private static FileSelection create(
      StatusType status, final ImmutableList<FileAttributes> fileAttributes, final String root) {
    if (Strings.isNullOrEmpty(root)) {
      throw new IllegalArgumentException("Selection root is null or empty" + root);
    }
    final Path originalRootPath = Path.of(root);
    final Path rootPath = handleWildCard(root);
    String selectionRoot = Path.withoutSchemeAndAuthority(rootPath).toString();
    return new FileSelection(
        status, fileAttributes, selectionRoot, originalRootPath, fileAttributes.size() > 1);
  }

  private static Path handleWildCard(final String root) {
    if (root.contains(WILD_CARD)) {
      int idx = root.indexOf(WILD_CARD); // first wild card in the path
      idx =
          root.lastIndexOf(PATH_SEPARATOR, idx); // file separator right before the first wild card
      final String newRoot = root.substring(0, idx);
      return newRoot.isEmpty() ? Path.of(Path.SEPARATOR) : Path.of(newRoot);
    } else {
      return Path.of(root);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileAttributesList.size(), selectionRoot);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof FileSelection)) {
      return false;
    }
    FileSelection that = (FileSelection) obj;
    if (this.isExpanded() != that.isExpanded()) {
      return false;
    }

    if (!Objects.equals(this.selectionRoot, that.selectionRoot)) {
      return false;
    }

    if (!isExpanded()) {
      // FileSelection is not expanded and the selectionRoots are the same
      return true;
    }

    // both are expanded
    // TODO: This looks bad - fileAttributesList can be large. This is using an O(N^2) algorithm
    // here
    return Utilities.listsUnorderedEquals(this.fileAttributesList, that.fileAttributesList);
  }

  public boolean isRootPathDirectory() {
    return isRootPathDirectory;
  }

  /**
   * Holds the FileAttributes of at most maxFiles in memory. Passing a large value for maxFiles can
   * lead to coordinator crashes when the argument combined has a lot of files in the folder
   */
  private static ImmutableList<FileAttributes> generateListOfFileAttributes(
      String datasetName, FileSystem fs, Path combined, int maxFiles) throws IOException {
    // NFS filesystems has delay before files written by executor shows up in the coordinator.
    // For NFS, fs.exists() will force a refresh if the directory is not found
    // No action is taken if it returns false as the code path already handles the Exception case
    Stopwatch timer = Stopwatch.createStarted();
    int numObjectsListed = 0;
    try {
      fs.exists(combined);

      ImmutableList<FileAttributes> fileAttributes;
      ImmutableList.Builder<FileAttributes> fileAttributesBuilder = new ImmutableList.Builder<>();
      int numFilesSeen = 0;
      try (DirectoryStream<FileAttributes> stream =
          FileSystemUtils.globRecursive(fs, combined, NO_HIDDEN_FILES)) {
        Iterator<FileAttributes> iterator = stream.iterator();
        while (iterator.hasNext()) {
          numObjectsListed++;
          FileAttributes fileAttribute = iterator.next();
          fileAttributesBuilder.add(fileAttribute);
          if (!fileAttribute.isDirectory()) {
            numFilesSeen++;
            if (numFilesSeen > maxFiles) {
              // reached the limit of files that can be loaded into memory
              throw new FileCountTooLargeException(datasetName, numFilesSeen, maxFiles);
            }
          }
        }

        fileAttributes = fileAttributesBuilder.build();
        if (logger.isTraceEnabled()) {
          for (FileAttributes fa : fileAttributes) {
            logger.trace(
                "File Path : "
                    + fa.getPath().toString()
                    + "; lastModifiedTime :"
                    + fa.lastModifiedTime().toString());
          }
        }
      } catch (DirectoryIteratorException e) {
        throw e.getCause();
      }

      logger.trace("Returned files are: {}", fileAttributes);
      return fileAttributes;
    } finally {
      long elapsedTime = timer.elapsed(TimeUnit.SECONDS);
      if (elapsedTime > 60) {
        logger.warn(
            "Expanding dataset {} with path {} took {} seconds for {} objects",
            datasetName,
            combined,
            elapsedTime,
            numObjectsListed);
      }
    }
  }
}
