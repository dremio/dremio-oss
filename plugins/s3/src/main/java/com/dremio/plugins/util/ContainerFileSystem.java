/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
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
      ImmutableList.Builder<ContainerFailure> failures = ImmutableList.builder();
      ImmutableMap.Builder<String, ContainerHolder> newMap = ImmutableMap.builder();
      for(ContainerCreator creator : getContainerCreators()) {

        // avoid recreating filesystem if it already exists.
        final ContainerHolder fs = oldMap.remove(creator.getName());
        if(fs != null) {
          newMap.put(creator.getName(), fs);
          continue;
        }

        // new file system.
        try {
          newMap.put(creator.getName(), creator.toContainerHolder());
        } catch (Exception ex) {
          logger.warn("Failure while attempting to connect to {} named [{}].", containerName, creator.getName(), ex);
          failures.add(new ContainerFailure(creator.getName(), ex));
        }
      }

      containerMap = newMap.build();

      for(ContainerHolder old : oldMap.values()) {
        try {
          old.close();
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
  public abstract class FileSystemSupplier implements AutoCloseable {

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
   * Do any initial file system setup before retrieving the containers for the first time.
   * @throws IOException
   */
  protected abstract void setup(Configuration conf) throws IOException;

  /**
   * Get a list of all containers.
   * @return A list of container creators that can be used for generating container filesystems.
   * @throws IOException
   */
  protected abstract Iterable<ContainerCreator> getContainerCreators() throws IOException;

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

  private String getContainer(Path path) {
    List<String> pathComponents = Arrays.asList(removeLeadingSlash(Path.getPathWithoutSchemeAndAuthority(path).toString()).split(Path.SEPARATOR));
    return pathComponents.get(0);
  }

  private Path pathWithoutContainer(Path path) {
    List<String> pathComponents = Arrays.asList(removeLeadingSlash(Path.getPathWithoutSchemeAndAuthority(path).toString()).split(Path.SEPARATOR));
    return new Path("/" + Joiner.on(Path.SEPARATOR).join(pathComponents.subList(1, pathComponents.size())));
  }

  private ContainerHolder getFileSystemForPath(Path path) throws IOException {
    final String name = getContainer(path);
    ContainerHolder container = containerMap.get(name);

    if (container == null) {
      try {
        synchronized(refreshLock) {
          container = containerMap.get(name);
          if(container == null) {
            container = getUnknownContainer(name);
            if(container == null) {
              throw new IOException(String.format("Unable to find %s named %s.", containerName, name));
            }

            ImmutableMap.Builder<String, ContainerHolder> newMap = ImmutableMap.builder();
            newMap.putAll(containerMap);
            newMap.put(name, container);
            containerMap = newMap.build();
          }
        }
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
    return getFileSystemForPath(f).fs().open(pathWithoutContainer(f), bufferSize);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    return getFileSystemForPath(f).fs().create(pathWithoutContainer(f), permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    return getFileSystemForPath(f).fs().append(pathWithoutContainer(f), bufferSize, progress);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    Preconditions.checkArgument(getContainer(src).equals(getContainer(dst)), String.format("Cannot rename files across %ss.", containerName));
    return getFileSystemForPath(src).fs().rename(pathWithoutContainer(src), pathWithoutContainer(dst));
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return getFileSystemForPath(f).fs().delete(pathWithoutContainer(f), recursive);
  }

  @Override
  public FileStatus[] listStatus(final Path f) throws FileNotFoundException, IOException {
    if (isRoot(f)) {
      return FluentIterable.from(containerMap.keySet())
          .transform(new Function<String,FileStatus>() {
            @Nullable
            @Override
            public FileStatus apply(@Nullable String containerName) {
              return new FileStatus(0, true, 0, 0, 0, new Path(containerName));
            }
      }).toArray(FileStatus.class);
    }

    final String containerName = getContainer(f);
    final Path pathWithoutContainerName = pathWithoutContainer(f);
    return FluentIterable.of(getFileSystemForPath(f).fs().listStatus(pathWithoutContainerName))
        .transform(new Function<FileStatus, CorrectableFileStatus>(){
          @Override
          public CorrectableFileStatus apply(FileStatus status) {
            return new CorrectableFileStatus(status, pathWithoutContainerName);
          }})
        .filter(listFileStatusPredicate)
        .transform(new Function<CorrectableFileStatus, FileStatus>(){

          @Override
          public FileStatus apply(CorrectableFileStatus input) {
            return transform(input.getStatus(), containerName);
          }})
        .toArray(FileStatus.class);
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

  private static FileStatus transform(FileStatus input, String containerName) {
    String relativePath = removeLeadingSlash(Path.getPathWithoutSchemeAndAuthority(input.getPath()).toString());
    Path containerPath  = new Path(Path.SEPARATOR + containerName);
    Path fullPath = Strings.isNullOrEmpty(relativePath) ? containerPath : new Path(containerPath, relativePath);
    return new FileStatus(input.getLen(),
            input.isDirectory(),
            input.getReplication(),
            input.getBlockSize(),
            input.getModificationTime(),
            input.getAccessTime(),
            input.getPermission(),
            input.getOwner(),
            input.getGroup(),
            fullPath);
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
    return getFileSystemForPath(f).fs().mkdirs(f, permission);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    if (isRoot(f)) {
      return new FileStatus(0, true, 0, 0, 0, f);
    }
    FileStatus fileStatus = getFileSystemForPath(f).fs().getFileStatus(pathWithoutContainer(f));
    return transform(fileStatus, getContainer(f));
  }

  @Override
  public String getScheme() {
    return scheme;
  }
}
