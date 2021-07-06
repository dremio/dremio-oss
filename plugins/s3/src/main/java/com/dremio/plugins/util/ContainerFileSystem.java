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
package com.dremio.plugins.util;

import static com.dremio.common.utils.PathUtils.removeLeadingSlash;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ListAccessor;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.hadoop.DremioHadoopUtils;
import com.dremio.exec.util.RemoteIterators;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * FileSystem implementation that treats multiple inner container filesystems as a unified toplevel filesystem.
 */
public abstract class ContainerFileSystem extends FileSystem {
  private static final Logger logger = LoggerFactory.getLogger(ContainerFileSystem.class);

  private final String scheme;
  private final String containerName;
  private final Object refreshLock = new Object();
  private final Predicate<CorrectableFileStatus> listFileStatusPredicate;

  private volatile ImmutableMap<String, ContainerHolder> containerMap = ImmutableMap.of();
  private volatile List<ContainerFailure> failures = ImmutableList.of();

  // Work around bug in s3a filesystem where the parent directory is included in list. Similar to HADOOP-12169
  public static final Predicate<CorrectableFileStatus> ELIMINATE_PARENT_DIRECTORY =
      (input -> {
        final FileStatus status = input.getStatus();
        if (!status.isDirectory()) {
          return true;
        }
        return !Path.getPathWithoutSchemeAndAuthority(input.getPathWithoutContainerName()).equals(Path.getPathWithoutSchemeAndAuthority(status.getPath()));
      });

  /**
   * Subclass Constructor.
   * @param scheme Scheme of filesystem.
   * @param containerName The descriptive name of the container for this ContainerFileSystem (such as bucket).
   */
  protected ContainerFileSystem(String scheme, String containerName, Predicate<CorrectableFileStatus> listFileStatusPredicate) {
    this.scheme = scheme;
    this.containerName = containerName;
    this.listFileStatusPredicate = listFileStatusPredicate;
  }

  /**
   * Refresh the list of subfilesystems.
   * @throws IOException
   */
  public void refreshFileSystems() throws IOException {

    synchronized(refreshLock) {
      final Map<String, ContainerHolder> oldMap = new HashMap<>(containerMap);
      final ImmutableList.Builder<ContainerFailure> failures = ImmutableList.builder();
      final ImmutableMap.Builder<String, ContainerHolder> newMap = ImmutableMap.builder();
      getContainerCreators().forEach((creator) -> {

        // avoid recreating filesystem if it already exists.
        final ContainerHolder fs = oldMap.remove(creator.getName());
        if(fs != null) {
          newMap.put(creator.getName(), fs);
          return;
        }

        // new file system.
        try {
          newMap.put(creator.getName(), creator.toContainerHolder());
        } catch (Exception ex) {
          logger.warn("Failure while attempting to connect to {} named [{}].", containerName, creator.getName(), ex);
          failures.add(new ContainerFailure(creator.getName(), ex));
        }
      });

      containerMap = newMap.build();

      for(ContainerHolder old : oldMap.values()) {
        try {
          if (getUnknownContainer(old.getName()) == null) {
            logger.debug("Closing filesystem for the container {}", old.getName());
            old.close();
          }
        } catch (IOException ex) {
          logger.warn("Failure while closing {} named [{}].", containerName, old.getName(), ex);
        }
      }
      this.failures = failures.build();
    }
  }

  /**
   * Retrieve the most recent list of failures associated with trying to retrieve containers.
   * @return
   */
  public List<ContainerFailure> getSubFailures(){
    return failures;
  }

  /**
   * Description of container creation failure.
   */
  public static class ContainerFailure {
    private final String name;
    private final Exception exception;

    public ContainerFailure(String name, Exception exception) {
      super();
      this.name = name;
      this.exception = exception;
    }
    public String getName() {
      return name;
    }
    public Exception getException() {
      return exception;
    }

  }

  @Override
  public final void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    setup(conf);
    setConf(conf);
    refreshFileSystems();
  }

  /**
   * A class that is used to create inner filesystems if they don't already exist.
   */
  public abstract static class ContainerCreator {
    protected abstract String getName();
    protected abstract ContainerHolder toContainerHolder() throws IOException;
  }

  /**
   * Class used to holder a container filesystem and name.
   */
  protected static class ContainerHolder implements AutoCloseable {
    private final String name;
    private final FileSystemSupplier fs;

    public ContainerHolder(String name, FileSystemSupplier fs) {
      super();
      this.name = name;
      this.fs = fs;
    }

    public String getName() {
      return name;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name);
    }

    public FileSystem fs() throws IOException {
      return fs.get();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      ContainerHolder other = (ContainerHolder) obj;

      return Objects.equals(name, other.name);
    }

    @Override
    public void close() throws IOException {
      fs.close();
    }

  }

  /**
   * A custom memoizing supplier-like interface that also supports throwing an IOException.
   */
  public abstract static class FileSystemSupplier implements AutoCloseable {

    private volatile FileSystem fs;
    public final FileSystem get() throws IOException{
      if(fs != null) {
        return fs;
      }

      synchronized(this) {
        if(fs != null) {
          return fs;
        }

        this.fs = create();
        return fs;
      }
    }

    public abstract FileSystem create() throws IOException;

    public void close() throws IOException {
      if (fs != null) {
        fs.close();
      }
    }
  }

  /**
   * Returns true iff container with the given name exists.
   *
   * @param containerName container name
   * @return true iff container with the given name exists
   */
  public boolean containerExists(final String containerName) {
    return containerMap.containsKey(containerName);
  }

  /**
   * Do any initial file system setup before retrieving the containers for the first time.
   * @throws IOException
   */
  protected abstract void setup(Configuration conf) throws IOException;

  /**
   * Get a list of all containers.
   * @return A list of container creators that can be used for generating container filesystems.
   * @throws IOException
   */
  protected abstract Stream<ContainerCreator> getContainerCreators() throws IOException;

  /**
   * Attempt to retrieve a container FileSystem that wasn't previously known.
   *
   * @param name Container name.
   * @return The container filesystem associated with this container name. Or null, if the container does not exist.
   * @throws IOException
   */
  protected abstract ContainerHolder getUnknownContainer(String name) throws IOException;

  private boolean isRoot(Path path) {
    List<String> pathComponents = Arrays.asList(Path.getPathWithoutSchemeAndAuthority(path).toString().split(Path.SEPARATOR));
    return pathComponents.size() == 0;
  }

  private ContainerHolder getFileSystemForPath(Path path) throws IOException {
    final String name = DremioHadoopUtils.getContainerName(path);
    ContainerHolder container = containerMap.get(name);

    if (container == null) {
      try {
        synchronized (refreshLock) {
          container = containerMap.get(name);
          if (container == null) {
            container = getUnknownContainer(name);
            ImmutableMap.Builder<String, ContainerHolder> newMap = ImmutableMap.builder();
            newMap.putAll(containerMap);
            newMap.put(name, container);
            containerMap = newMap.build();
          }
        }
      } catch (IOException ex) {
        throw ex;
      } catch (Exception ex) {
        throw new IOException(String.format("Unable to retrieve %s named %s.", containerName, name), ex);
      }
    }

    return container;
  }

  @Override
  public URI getUri() {
    try {
      return new URI(scheme + ":///");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return getFileSystemForPath(f).fs().open(DremioHadoopUtils.pathWithoutContainer(f), bufferSize);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    return getFileSystemForPath(f).fs().create(DremioHadoopUtils.pathWithoutContainer(f), permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    return getFileSystemForPath(f).fs().append(DremioHadoopUtils.pathWithoutContainer(f), bufferSize, progress);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    Preconditions.checkArgument(DremioHadoopUtils.getContainerName(src).equals(DremioHadoopUtils.getContainerName(dst)), String.format("Cannot rename files across %ss.", containerName));
    return getFileSystemForPath(src).fs().rename(DremioHadoopUtils.pathWithoutContainer(src), DremioHadoopUtils.pathWithoutContainer(dst));
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return getFileSystemForPath(f).fs().delete(DremioHadoopUtils.pathWithoutContainer(f), recursive);
  }


  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f) throws FileNotFoundException, IOException {
    return super.listLocatedStatus(f);
  }

  @Override
  protected RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f, final PathFilter filter) throws FileNotFoundException, IOException {
    final String container = DremioHadoopUtils.getContainerName(f);
    final PathFilter alteredFilter = (path) -> {
      return filter.accept(transform(path, container));
    };

    return RemoteIterators.transform(
        ListAccessor.listLocatedFileStatus(getFileSystemForPath(f).fs(), DremioHadoopUtils.pathWithoutContainer(f), alteredFilter),
        t -> new LocatedFileStatus(ContainerFileSystem.transform(t, container), t.getBlockLocations())
        );
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive) throws FileNotFoundException, IOException {
    final String container = DremioHadoopUtils.getContainerName(f);
    return RemoteIterators.transform(
        getFileSystemForPath(f).fs().listFiles(DremioHadoopUtils.pathWithoutContainer(f), recursive),
        t -> new LocatedFileStatus(ContainerFileSystem.transform(t, container), t.getBlockLocations())
        );
  }


  @Override
  public FileStatus[] listStatus(final Path f) throws FileNotFoundException, IOException {
    if (isRoot(f)) {
      return containerMap.keySet().stream()
        .map((Function<String, FileStatus>) containerName ->
          new FileStatus(0, true, 0, 0, 0, new Path(containerName)))
        .toArray(FileStatus[]::new);
    }

    final String containerName = DremioHadoopUtils.getContainerName(f);
    final Path pathWithoutContainerName = DremioHadoopUtils.pathWithoutContainer(f);
    return Arrays.stream(getFileSystemForPath(f).fs().listStatus(pathWithoutContainerName))
      .map((Function<FileStatus, CorrectableFileStatus>) status ->
        new CorrectableFileStatus(status, pathWithoutContainerName))
      .filter(listFileStatusPredicate)
      .map(input -> transform(input.getStatus(), containerName))
      .toArray(FileStatus[]::new);
  }

  /**
   * Utility class that includes information to allow ContainerFileSystem implementations to filter
   * listed file statuses before returning to consumer.
   */
  protected static class CorrectableFileStatus {
    private final FileStatus status;
    private final Path pathWithoutContainerName;

    public CorrectableFileStatus(FileStatus status, Path pathWithoutContainerName) {
        super();
        this.status = status;
        this.pathWithoutContainerName = pathWithoutContainerName;
      }

    public FileStatus getStatus() {
      return status;
    }

    public Path getPathWithoutContainerName() {
      return pathWithoutContainerName;
    }

  }

  /**
   * Transform remote path to local.
   * @param path
   * @param containerName
   * @return
   */
  @VisibleForTesting
  static Path transform(Path path, String containerName) {
    final String relativePath = removeLeadingSlash(Path.getPathWithoutSchemeAndAuthority(path).toString());
    final Path containerPath  = new Path(Path.SEPARATOR + containerName);

    // If relativePath is not null or empty, ensure that it is treated as a real relative path by
    // constructing a new {@link Path} object where the schema and the authority fields are set to null.
    // If this is not done, and a relative path contains special characters (like a colon), this breaks the
    // URI#checkPath(String, String, String) method.
    return Strings.isNullOrEmpty(relativePath) ? containerPath : new Path(containerPath, new Path(null, null, relativePath));
  }

  /**
   * Transform remote file status to local.
   */
  private static FileStatus transform(FileStatus input, String containerName) {
    return new FileStatus(input.getLen(),
            input.isDirectory(),
            input.getReplication(),
            input.getBlockSize(),
            input.getModificationTime(),
            input.getAccessTime(),
            input.getPermission(),
            input.getOwner(),
            input.getGroup(),
            transform(input.getPath(), containerName));
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Path getWorkingDirectory() {
    return new Path("/");
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return getFileSystemForPath(f).fs().mkdirs(DremioHadoopUtils.pathWithoutContainer(f), permission);
  }

  @Override
  public boolean exists(Path f) throws IOException {
    try {
      return super.exists(f);
    } catch (ContainerNotFoundException ignored) {
      return false;
    }
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    if (isRoot(f)) {
      return new FileStatus(0, true, 0, 0, 0, f);
    }
    FileStatus fileStatus = getFileSystemForPath(f).fs().getFileStatus(DremioHadoopUtils.pathWithoutContainer(f));
    return transform(fileStatus, DremioHadoopUtils.getContainerName(f));
  }

  @Override
  public String getScheme() {
    return scheme;
  }
}
