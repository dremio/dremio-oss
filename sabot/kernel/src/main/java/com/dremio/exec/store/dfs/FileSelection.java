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

import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FilenameUtils;

import com.dremio.common.utils.PathUtils;
import com.dremio.exec.util.Utilities;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.FileSystemUtils;
import com.dremio.io.file.Path;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Jackson serializable description of a file selection.
 */
public class FileSelection {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSelection.class);
  public static final String PATH_SEPARATOR = System.getProperty("file.separator");
  private static final String WILD_CARD = "*";

  private ImmutableList<FileAttributes> fileAttributesList;
  private final String selectionRoot;
  private final Path originalRootPath;

  private enum StatusType {
    NO_DIRS,             // no directories in this selection
    HAS_DIRS,            // directories were found in the selection
    EXPANDED,            // whether this selection has been expanded to files
    NOT_EXPANDED         // selection will only have selectionRoot info and no information about sub-dir/subfiles
  }

  private StatusType dirStatus;

  /**
   * Creates a {@link FileSelection selection} out of given file statuses/files and selection root.
   * @param status
   * @param fileAttributesList
   * @param selectionRoot
   * @param originalRootPath
   */
  private FileSelection(StatusType status, final ImmutableList<FileAttributes> fileAttributesList, final String selectionRoot, final Path originalRootPath) {
    this.fileAttributesList = Preconditions.checkNotNull(fileAttributesList);
    this.selectionRoot = Preconditions.checkNotNull(selectionRoot);
    this.dirStatus = status;
    this.originalRootPath = originalRootPath;
  }

  public boolean isEmpty() {
    return fileAttributesList.isEmpty();
  }

  /**
   * Copy constructor for convenience.
   */
  protected FileSelection(final FileSelection selection) {
    Preconditions.checkNotNull(selection, "selection cannot be null");
    this.fileAttributesList = selection.fileAttributesList;
    this.selectionRoot = selection.selectionRoot;
    this.originalRootPath = selection.originalRootPath;
    this.dirStatus = selection.dirStatus;
  }

  public String getSelectionRoot() {
    return selectionRoot;
  }

  public List<FileAttributes> getFileAttributesList() {
    return fileAttributesList;
  }

  public boolean containsDirectories() throws IOException {
    if (dirStatus == StatusType.EXPANDED) {
      for (final FileAttributes status : getFileAttributesList()) {
        if (status.isDirectory()) {
          return true;
        }
      }
    }
    return dirStatus == StatusType.HAS_DIRS;
  }

  public List<FileAttributes> getAllDirectories() throws IOException {
    return Lists.newArrayList(Iterables.filter(fileAttributesList, FileAttributes::isDirectory));
  }

  public FileSelection minusDirectories() throws IOException {
    if (dirStatus == StatusType.NO_DIRS) {
      return this;
    }

    Stopwatch timer = Stopwatch.createStarted();
    final int total = fileAttributesList.size();
    final ImmutableList<FileAttributes> nonDirectories = ImmutableList.copyOf(Iterables.filter(fileAttributesList, Predicates.not(FileAttributes::isDirectory)));

    final FileSelection fileSel = create(StatusType.NO_DIRS, nonDirectories, selectionRoot);
    logger.debug("FileSelection.minusDirectories() took {} ms, numFiles: {}", timer.elapsed(TimeUnit.MILLISECONDS), total);

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

  public Optional<FileAttributes> getFirstFile() throws IOException {
    return Iterables.tryFind(fileAttributesList, FileAttributes::isRegularFile);
  }

  public static Optional<FileAttributes> getFirstFileIteratively(FileSystem fs, final List<String> fullPath) throws IOException {
    return getFirstFileIteratively(fs,getPathBasedOnFullPath(fullPath));
  }

  public static Optional<FileAttributes> getFirstFileIteratively(FileSystem fs, Path path) throws IOException {
    if (fs.isFile(path)) {
      FileAttributes fileAttribute = fs.getFileAttributes(path);
      if (fileAttribute.isRegularAndNoHiddenFile()) {
        return Optional.of(fileAttribute);
      } else {
        return Optional.absent();
      }
    } else {
      if (!fs.exists(path)) {
        return Optional.absent();
      }
    }

    try(DirectoryStream<FileAttributes> stream = fs.listFiles(path, true)) {
      return Iterables.tryFind(stream, FileAttributes::isRegularAndNoHiddenFile);
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

  public boolean isNoDirs() { return dirStatus == StatusType.NO_DIRS;}

  public static FileSelection create(FileAttributes fileAttributes) throws IOException {
    return new FileSelection(StatusType.EXPANDED, ImmutableList.of(fileAttributes), fileAttributes.getPath().toString(), fileAttributes.getPath());
  }

  public void expand(FileSystem fs) throws IOException {
    if(dirStatus == StatusType.NOT_EXPANDED) {
      this.fileAttributesList = generateListOfFileAttributes(fs, originalRootPath);
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

  public static FileSelection create(final FileSystem fs, final List<String> fullPath) throws IOException {
    return create(fs, getPathBasedOnFullPath(fullPath));
  }

  public static FileSelection createNotExpanded(final FileSystem fs, final List<String> fullPath) throws IOException {
    return createNotExpanded(fs, getPathBasedOnFullPath(fullPath));
  }

  // Check if path is actually a full schema path
  public static FileSelection createWithFullSchema(final FileSystem fs, final String parent, final String fullSchemaPath) throws IOException {
    final Path combined = Path.mergePaths(Path.of(parent), PathUtils.toFSPath(fullSchemaPath));
    return create(fs, combined);
  }

  public static FileSelection createWithFullSchemaNotExpanded(final FileSystem fs, final String parent, final String fullSchemaPath) throws IOException {
    final Path combined = Path.mergePaths(Path.of(parent), PathUtils.toFSPath(fullSchemaPath));
    return createNotExpanded(fs, combined);
  }

  public static FileSelection create(final FileSystem fs, Path combined) throws IOException {
    Stopwatch timer = Stopwatch.createStarted();
    final ImmutableList<FileAttributes> fileAttributes = generateListOfFileAttributes(fs, combined);
    if (fileAttributes.isEmpty()) {
      return null;
    }
    final FileSelection fileSel = createFromExpanded(fileAttributes, combined.toURI().getPath());
    logger.debug("FileSelection.create() took {} ms ", timer.elapsed(TimeUnit.MILLISECONDS));
    return fileSel;
  }

  public static FileSelection createNotExpanded(final FileSystem fs, Path root) throws IOException {
    if (root == null || root.toString().isEmpty()) {
      throw new IllegalArgumentException("Selection root is null or empty" + root);
    }
    final Path rootPath = handleWildCard(root.toString());
    if (!fs.exists(rootPath)) {
      return null;
    }
    final String selectionRoot = Path.withoutSchemeAndAuthority(rootPath).toString();
    return new FileSelection(StatusType.NOT_EXPANDED, ImmutableList.of(), selectionRoot, root);
  }

  public static FileSelection createFromExpanded(final ImmutableList<FileAttributes> fileAttributes, final String root) {
    return create(StatusType.EXPANDED, fileAttributes, root);
  }

  /**
   * Creates a {@link FileSelection selection} with the given file statuses and selection root.
   *
   * @param status  status
   * @param fileAttributes list of file attributes
   * @param root  root path for selections
   *
   * @return  null if creation of {@link FileSelection} fails with an {@link IllegalArgumentException}
   *          otherwise a new selection.
   *
   */
  private static FileSelection create(StatusType status, final ImmutableList<FileAttributes> fileAttributes, final String root) {
    if (Strings.isNullOrEmpty(root)) {
      throw new IllegalArgumentException("Selection root is null or empty" + root);
    }
    final Path originalRootPath = Path.of(root);
    final Path rootPath = handleWildCard(root);
    String selectionRoot = Path.withoutSchemeAndAuthority(rootPath).toString();
    return new FileSelection(status, fileAttributes, selectionRoot, originalRootPath);
  }

  private static Path handleWildCard(final String root) {
    if (root.contains(WILD_CARD)) {
      int idx = root.indexOf(WILD_CARD); // first wild card in the path
      idx = root.lastIndexOf(PATH_SEPARATOR, idx); // file separator right before the first wild card
      final String newRoot = root.substring(0, idx);
      return newRoot.isEmpty() ? Path.of(Path.SEPARATOR) : Path.of(newRoot);
    } else {
      return Path.of(root);
    }
  }

  public boolean supportDirPruning() {
    return isExpanded(); // currently we only support pruning if the directories have been expanded (this may change in the future)
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
    return Objects.equals(this.selectionRoot, that.selectionRoot)
        && Utilities.listsUnorderedEquals(this.fileAttributesList, that.fileAttributesList);
  }

  public List<String> getExtensions() {
    final List<String> extensions = Lists.newArrayList();
    for (FileAttributes fileStatus : fileAttributesList) {
      if (fileStatus.isRegularFile()) {
        final String ext = FilenameUtils.getExtension(fileStatus.getPath().getName());
        if (ext != null && !ext.isEmpty()) {
          extensions.add(ext);
        }
      }
    }
    return extensions;
  }

  private static ImmutableList<FileAttributes> generateListOfFileAttributes(FileSystem fs, Path combined) throws IOException {
    // NFS filesystems has delay before files written by executor shows up in the coordinator.
    // For NFS, fs.exists() will force a refresh if the directory is not found
    // No action is taken if it returns false as the code path already handles the Exception case
    fs.exists(combined);

    ImmutableList<FileAttributes> fileAttributes;
    try(DirectoryStream<FileAttributes> stream = FileSystemUtils.globRecursive(fs, combined, NO_HIDDEN_FILES)) {
      fileAttributes = ImmutableList.copyOf(stream);
    } catch (DirectoryIteratorException e) {
      throw e.getCause();
    }

    logger.trace("Returned files are: {}", fileAttributes);
    return fileAttributes;
  }
}
