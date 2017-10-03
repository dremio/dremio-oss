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
package com.dremio.exec.store.dfs;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.dremio.common.utils.PathUtils;
import com.dremio.exec.util.Utilities;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Jackson serializable description of a file selection.
 */
public class FileSelection {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSelection.class);
  private static final String PATH_SEPARATOR = System.getProperty("file.separator");
  private static final String WILD_CARD = "*";

  private final ImmutableList<FileStatus> statuses;
  private final String selectionRoot;

  private enum StatusType {
    NO_DIRS,             // no directories in this selection
    HAS_DIRS,            // directories were found in the selection
    EXPANDED             // whether this selection has been expanded to files
  }

  private final StatusType dirStatus;

  /**
   * Creates a {@link FileSelection selection} out of given file statuses/files and selection root.
   *
   * @param statuses  list of file statuses
   * @param files  list of files
   * @param selectionRoot  root path for selections
   */
  private FileSelection(StatusType status, final ImmutableList<FileStatus> statuses, final String selectionRoot) {
    this.statuses = Preconditions.checkNotNull(statuses);
    this.selectionRoot = Preconditions.checkNotNull(selectionRoot);
    this.dirStatus = status;
  }

  public boolean isEmpty() {
    return statuses.isEmpty();
  }

  /**
   * Copy constructor for convenience.
   */
  protected FileSelection(final FileSelection selection) {
    Preconditions.checkNotNull(selection, "selection cannot be null");
    this.statuses = selection.statuses;
    this.selectionRoot = selection.selectionRoot;
    this.dirStatus = selection.dirStatus;
  }

  public String getSelectionRoot() {
    return selectionRoot;
  }

  public List<FileStatus> getStatuses() {
    return statuses;
  }

  public boolean containsDirectories() throws IOException {
    if (dirStatus == StatusType.EXPANDED) {
      for (final FileStatus status : getStatuses()) {
        if (status.isDirectory()) {
          return true;
        }
      }
    }
    return dirStatus == StatusType.HAS_DIRS;
  }

  public List<FileStatus> getAllDirectories() throws IOException {
    return Lists.newArrayList(Iterables.filter(statuses, new Predicate<FileStatus>() {
      @Override
      public boolean apply(@Nullable FileStatus status) {
        return status.isDirectory();
      }
    }));
  }

  public FileSelection minusDirectories() throws IOException {
    if (dirStatus == StatusType.NO_DIRS) {
      return this;
    }

    Stopwatch timer = Stopwatch.createStarted();
    final int total = statuses.size();
    final ImmutableList<FileStatus> nonDirectories = FluentIterable.from(statuses).filter(new Predicate<FileStatus>() {
      @Override
      public boolean apply(@Nullable FileStatus status) {
        return !status.isDirectory();
      }
    }).toList();

    final FileSelection fileSel = create(StatusType.NO_DIRS, nonDirectories, selectionRoot);
    logger.debug("FileSelection.minusDirectories() took {} ms, numFiles: {}", timer.elapsed(TimeUnit.MILLISECONDS), total);

    return fileSel;
  }

  public FileStatus getFirstPath() throws IOException {
    return getStatuses().get(0);
  }

  public Optional<FileStatus> getFirstFile() throws IOException {
    return Iterables.tryFind(FluentIterable.from(statuses), new Predicate<FileStatus>() {

      @Override
      public boolean apply(FileStatus input) {
        return input.isFile();
      }});
  }

  public boolean isExpanded() {
    return dirStatus == StatusType.EXPANDED;
  }

  private static String commonPath(final List<FileStatus> statuses) {
    if (statuses == null || statuses.isEmpty()) {
      return "";
    }

    final List<String> files = Lists.newArrayList();
    for (final FileStatus status : statuses) {
      files.add(status.getPath().toString());
    }
    return commonPathForFiles(files);
  }

  /**
   * Returns longest common path for the given list of files.
   *
   * @param files  list of files.
   * @return  longest common path
   */
  private static String commonPathForFiles(final List<String> files) {
    if (files == null || files.isEmpty()) {
      return "";
    }

    final int total = files.size();
    final String[][] folders = new String[total][];
    int shortest = Integer.MAX_VALUE;
    for (int i = 0; i < total; i++) {
      final Path path = new Path(files.get(i));
      folders[i] = Path.getPathWithoutSchemeAndAuthority(path).toString().split(PATH_SEPARATOR);
      shortest = Math.min(shortest, folders[i].length);
    }

    int latest;
    out:
    for (latest = 0; latest < shortest; latest++) {
      final String current = folders[0][latest];
      for (int i = 1; i < folders.length; i++) {
        if (!current.equals(folders[i][latest])) {
          break out;
        }
      }
    }
    final Path path = new Path(files.get(0));
    final URI uri = path.toUri();
    final String pathString = buildPath(folders[0], latest);
    return new Path(uri.getScheme(), uri.getAuthority(), pathString).toString();
  }

  private static String buildPath(final String[] path, final int folderIndex) {
    final StringBuilder builder = new StringBuilder();
    for (int i=0; i<folderIndex; i++) {
      builder.append(path[i]).append(PATH_SEPARATOR);
    }
    builder.deleteCharAt(builder.length() - 1);
    return builder.toString();
  }

  public static FileSelection create(FileStatus status) throws IOException {
    return new FileSelection(StatusType.EXPANDED, ImmutableList.of(status), status.getPath().toString());
  }

  public static FileSelection create(final FileSystemWrapper fs, final List<String> fullPath) throws IOException {
    String parent = Joiner.on(PATH_SEPARATOR).join(fullPath.subList(0, fullPath.size() - 1));
    if (Strings.isNullOrEmpty(parent)) {
      parent = "/";
    }
    parent = Path.getPathWithoutSchemeAndAuthority(new Path(Path.SEPARATOR, parent)).toString();
    String path = PathUtils.removeQuotes(fullPath.get(fullPath.size() - 1));
    return create(fs, parent, path);
  }

  // Check if path is actually a full schema path
  public static FileSelection createWithFullSchema(final FileSystemWrapper fs, final String parent, final String fullSchemaPath) throws IOException {
    final Path combined = Path.mergePaths(new Path(parent), PathUtils.toFSPath(fullSchemaPath));
    return create(fs, combined);
  }

  private static FileSelection create(final FileSystemWrapper fs, final String parent, final String path) throws IOException {
    final Path combined = new Path(parent, removeLeadingSlash(path));
    return create(fs, combined);
  }

  public static FileSelection create(final FileSystemWrapper fs, Path combined) throws IOException {
    Stopwatch timer = Stopwatch.createStarted();

    final ImmutableList<FileStatus> statuses = fs.listRecursive(combined, false);
    if (statuses == null || statuses.isEmpty()) {
      return null;
    }

    final FileSelection fileSel = create(StatusType.EXPANDED, statuses, combined.toUri().getPath());
    logger.debug("FileSelection.create() took {} ms ", timer.elapsed(TimeUnit.MILLISECONDS));
    return fileSel;
  }

  /**
   * Creates a {@link FileSelection selection} with the given file statuses and selection root.
   *
   * @param statuses  list of file statuses
   * @param root  root path for selections
   *
   * @return  null if creation of {@link FileSelection} fails with an {@link IllegalArgumentException}
   *          otherwise a new selection.
   *
   */
  private static FileSelection create(StatusType status, final ImmutableList<FileStatus> statuses, final String root) {
    if (statuses == null || statuses.size() == 0) {
      return null;
    }

    final String selectionRoot;
    if (Strings.isNullOrEmpty(root)) {
      throw new IllegalArgumentException("Selection root is null or empty" + root);
    }
    final Path rootPath = handleWildCard(root);
    final URI uri = statuses.get(0).getPath().toUri();
    final Path path = new Path(uri.getScheme(), uri.getAuthority(), rootPath.toUri().getPath());
    selectionRoot = Path.getPathWithoutSchemeAndAuthority(path).toString();
    return new FileSelection(status, statuses, selectionRoot);
  }

  private static Path handleWildCard(final String root) {
    if (root.contains(WILD_CARD)) {
      int idx = root.indexOf(WILD_CARD); // first wild card in the path
      idx = root.lastIndexOf(PATH_SEPARATOR, idx); // file separator right before the first wild card
      final String newRoot = root.substring(0, idx);
      return newRoot.isEmpty() ? new Path(Path.SEPARATOR) : new Path(newRoot);
    } else {
      return new Path(root);
    }
  }

  public static String removeLeadingSlash(String path) {
    if (path.charAt(0) == '/') {
      String newPath = path.substring(1);
      return removeLeadingSlash(newPath);
    } else {
      return path;
    }
  }

  public List<FileStatus> getFileStatuses() {
    return statuses;
  }

  public boolean supportDirPruning() {
    return isExpanded(); // currently we only support pruning if the directories have been expanded (this may change in the future)
  }

  @Override
  public int hashCode() {
    return Objects.hash(statuses.size(), selectionRoot);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof FileSelection)) {
      return false;
    }
    FileSelection that = (FileSelection) obj;
    return Objects.equals(this.selectionRoot, that.selectionRoot)
        && Utilities.listsUnorderedEquals(this.statuses, that.statuses);
  }

  public List<String> getExtensions() {
    final List<String> extensions = Lists.newArrayList();
    for (FileStatus fileStatus : statuses) {
      if (fileStatus.isFile()) {
        final String ext = FilenameUtils.getExtension(fileStatus.getPath().getName());
        if (ext != null && !ext.isEmpty()) {
          extensions.add(ext);
        }
      }
    }
    return extensions;
  }
}
