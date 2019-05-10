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
package com.dremio.exec.store.dfs;

import static java.lang.String.format;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.services.fabric.api.FabricCommandRunner;
import com.dremio.services.fabric.api.FabricRunnerFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Pseudo-distributed filesystem implementation.
 *
 * Provides access to remote node local filesystem metadata.
 *
 * <em>FileSystem layout</em>
 * <ul>
 * <li>Remote directories containing the same data are merged together</li>
 * <li>Files are prefixed with <code>@&lt;address&gt;</code> (if file is hidden, the hidden character is preserved)</li>
 * </ul>
 *
 * <em>Operations</em>
 * <ul>
 * <li>Opening a file/Getting file status: if file starts with an address -> try to get it remotely. Otherwise throw FileNotFoundException</li>
 * <li>Getting directory status -> try to get it locally, otherwise, try to get it remotely.
 *     Permissions might have been to be merged</li>
 * <li>Listing directory content -> get all the directories content.
 * If a remote directory doesn't exist, ignore it (empty directory). If permission failure, throw back to the client.</li>
 * </ul>
 *
 */
public class PseudoDistributedFileSystem extends FileSystem implements PathCanonicalizer {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PseudoDistributedFileSystem.class);
  private static final Path ROOT_PATH = new Path(Path.SEPARATOR);

  private static PDFSConfig globalConfig;

  private final PDFSConfig config;

  private ExecutorService executor;
  private FabricRunnerFactory runnerFactory;
  private BufferAllocator allocator;
  private Provider<Iterable<NodeEndpoint>> endpointProvider;
  private boolean localAccessAllowed;
  private NodeEndpoint localIdentity;

  private URI uri;
  private Path workingDirectory;
  private FileSystem underlyingFS;

  /**
   * Set global configuration that will be shared by all PDFS instances
   *
   * @param config
   *          the configuration to use
   */
  public static synchronized void configure(PDFSConfig config) {
    PseudoDistributedFileSystem.globalConfig = Preconditions.checkNotNull(config);
  }

  static FileSystem newLocalFileSystem(Configuration conf, boolean isLocalAccessAllowed) throws IOException {
    // we'll grab our own local file system so append is supported (rather than the checksum local file system).
    final FileSystem localFS = isLocalAccessAllowed ? new PDFSLocalFileSystem() : new NoopFileSystem();
    localFS.initialize(localFS.getUri(), conf);

    return localFS;
  }

  public static final String NAME = "pdfs";

  // CHECKSTYLE:OFF VisibilityModifier
  private static final class RemotePath {
    public final String address;
    public final Path path;

    public RemotePath(String address, Path path) {
      this.address = address;
      this.path = path;
    }
  }
  // CHECKSTYLE:ON

  /**
   * Returns true if a name is one of a hidden file.
   *
   * By convention Hadoop hidden files start with . and _ characters
   *
   * @param name
   * @return true if hidden
   */
  private static final boolean isHidden(String name) {
    if (name.isEmpty()) {
      // How can name be empty? "" is the basename for root
      return false;
    }

    char firstChar = name.charAt(0);

    return firstChar == '.' || firstChar == '_';
  }

  private static final Pattern BASENAME_SPLIT_PATTERN = Pattern.compile("([^@]+)@(.+)");

  private static boolean isRemoteFile(Path path) {
    final String basename = path.getName();

    boolean hidden = isHidden(basename);

    Matcher matcher = BASENAME_SPLIT_PATTERN.matcher(hidden ? basename.substring(1) : basename);
    return matcher.matches();
  }

  public static String getRemoteFileName(String basename) {
    Matcher matcher = BASENAME_SPLIT_PATTERN.matcher(basename);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Cannot parse basename for " + basename);
    }
    return matcher.group(2);
  }

  public static RemotePath getRemotePath(Path path) throws IOException {
    final String basename = path.getName();

    boolean hidden = isHidden(basename);

    Matcher matcher = BASENAME_SPLIT_PATTERN.matcher(hidden ? basename.substring(1) : basename);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Cannot parse basename for path " + path);
    }

    final String remoteBasename = matcher.group(2);
    return new RemotePath(
        matcher.group(1),
        new Path(Path.getPathWithoutSchemeAndAuthority(path.getParent()), hidden ? basename.charAt(0) + remoteBasename : remoteBasename));
  }

  public PseudoDistributedFileSystem() {
    this(globalConfig);
  }

  @VisibleForTesting
  PseudoDistributedFileSystem(PDFSConfig config) {
    this.config = config;
    this.workingDirectory = ROOT_PATH;
  }

  @VisibleForTesting
  void initialize(URI name, Configuration conf, FileSystem underlyingFS) throws IOException {
    super.initialize(name, conf);

    this.allocator = config.getAllocator();
    this.localAccessAllowed = config.isLocalAccessAllowed();
    this.executor = config.getExecutor();
    this.runnerFactory = config.getRunnerFactory();
    this.endpointProvider = config.getEndpointProvider();
    this.localIdentity = config.getLocalIdentity();

    this.uri = name;
    this.underlyingFS = underlyingFS;
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    initialize(name, conf,  newLocalFileSystem(conf, config.isLocalAccessAllowed()));
  }

  // Keeping a cache of remote filesystem instances
  private Cache<NodeEndpoint, FileSystem> remoteFileSystems = CacheBuilder.newBuilder().softValues().build();

  /**
   * Creates a {@link RemoteNodeFileSystem} instance.
   *
   * Only exposed for unit testing
   */
  @VisibleForTesting
  FileSystem newRemoteFileSystem(final NodeEndpoint endpoint) throws IOException {
    final FabricCommandRunner runner = runnerFactory.getCommandRunner(endpoint.getAddress(), endpoint.getFabricPort());
    RemoteNodeFileSystem rdfs = new RemoteNodeFileSystem(runner, allocator);
    rdfs.initialize(URI.create(format("sabot://%s:%d", endpoint.getAddress(), endpoint.getFabricPort())), getConf());

    return rdfs;
  }

  private List<NodeEndpoint> getEndpoints(final String address) {
    final List<NodeEndpoint> endpoints = StreamSupport.stream(getEndpoints().spliterator(), false)
        .filter(input -> address.equals(input.getAddress()))
        .collect(Collectors.toList());

    return endpoints;
  }

  // DX-5178: more hackery given static cached references.
  private Iterable<NodeEndpoint> getEndpoints() {
    final Iterable<NodeEndpoint> endpoints = endpointProvider.get();
    if (Iterables.isEmpty(endpoints)) {
      return globalConfig.getEndpointProvider().get();
    } else {
      return endpoints;
    }
  }

  private NodeEndpoint getRandomDelegate() throws IOException {
    List<NodeEndpoint> selectable = Lists.newArrayList(getEndpoints());
    if(selectable.isEmpty()){
      throw new UnsupportedOperationException("Unable to use pdfs when no executors are running.");
    }
    Collections.shuffle(selectable);

    return selectable.get(0);
  }

  private final FileSystem getOrCreateRemote(final NodeEndpoint endpoint) throws IOException {
    try{
      return remoteFileSystems.get(endpoint, new Callable<FileSystem>() {
        @Override
        public FileSystem call() throws Exception {
          return newRemoteFileSystem(endpoint);
        }
      });
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class);
      // Code below is dead, but javac cannot deduce it
      throw new AssertionError("Unexpected", e);
    }
  }

  private FileSystem getDelegateFileSystem(final String address) throws IOException {
    // Is it possible to have local access not allowed with a local address?
    // Yes, if there are two nodes on the same machines!
    if (localAccessAllowed && localIdentity.getAddress().equals(address)) {
      return underlyingFS;
    }

    List<NodeEndpoint> endpoints = getEndpoints(address);
    if (endpoints.isEmpty()) {
      throw new IllegalArgumentException("No remote address for address " + address);
    }

    final NodeEndpoint endpoint = endpoints.get(ThreadLocalRandom.current().nextInt(endpoints.size()));
    return getOrCreateRemote(endpoint);
  }

  private Path toAbsolutePath(Path p) {
    if (p.isAbsolute()) {
      return p;
    }

    return new Path(workingDirectory, p);
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public String getScheme() {
    return NAME;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    Path absolutePath = toAbsolutePath(f);
    checkPath(absolutePath);

    // Handle root
    if (absolutePath.isRoot()) {
      throw new AccessControlException("Cannot open " + f);
    }

    try {
      RemotePath remotePath = getRemotePath(absolutePath);

      FileSystem delegate = getDelegateFileSystem(remotePath.address);
      return delegate.open(remotePath.path, bufferSize);
    } catch (IllegalArgumentException e) {
      throw (FileNotFoundException) (new FileNotFoundException("No file " + absolutePath).initCause(e));
    }
  }

  /**
   * Create a new file. Three possibilities:
   *  - This is a data node and you're trying to create a unqualified file => write locally.
   *  - This is a client node and you're trying to create unqualified file => pick a random data node and write there.
   *  - The path you provide is qualified => write to that node.
   */
  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
    final Path absolutePath = toAbsolutePath(f);
    checkPath(absolutePath);

    // Handle root
    if (absolutePath.isRoot()) {
      throw new AccessControlException("Cannot create " + f);
    }

    if(!isRemoteFile(f)){
      if (isDirectory(absolutePath)) {
        throw new FileAlreadyExistsException("Directory already exists: " + f);
      }

      // Only canonicalized path/remote files are allowed
      throw new IOException("Cannot create non-canonical path " + f);
    }

    try {
      RemotePath remotePath = getRemotePath(absolutePath);
      return getDelegateFileSystem(remotePath.address).create(remotePath.path, permission, overwrite, bufferSize, replication, blockSize, progress);
    } catch (IllegalArgumentException e) {
      throw (IOException) (new IOException("Cannot create file " + absolutePath).initCause(e));
    }
  }

  /**
   * Create a new file. Three possibilities:
   *  - This is a data node and you're trying to append a unqualified file => write locally.
   *  - The path you provide is qualified => write to that node.
   *
   *  If this is a client node and you try to write to a unqualified file, we'll throw
   */
  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    Path absolutePath = toAbsolutePath(f);
    checkPath(absolutePath);

    // Handle root
    if (absolutePath.isRoot()) {
      throw new AccessControlException("Cannot open " + f);
    }

    if(!isRemoteFile(f)){
      if (isDirectory(absolutePath)) {
        throw new FileAlreadyExistsException("Directory already exists: " + f);
      }

      // Only fully canonicalized/remote files are allowed
      throw new IOException("Cannot create non-canonical path " + f);
    }

    try {
      RemotePath remotePath = getRemotePath(absolutePath);

      FileSystem delegate = getDelegateFileSystem(remotePath.address);
      return delegate.append(remotePath.path, bufferSize, progress);
    } catch (IllegalArgumentException e) {
      throw (FileNotFoundException) (new FileNotFoundException("No file " + absolutePath).initCause(e));
    }
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    Path absoluteSrc = toAbsolutePath(src);
    Path absoluteDst = toAbsolutePath(dst);
    checkPath(absoluteSrc);
    checkPath(absoluteDst);

    // Handle root
    if (absoluteSrc.isRoot()) {
      throw new IOException("Cannot rename " + absoluteSrc);
    }

    if (absoluteDst.isRoot()) {
      throw new IOException("Cannot rename into" + absoluteDst);
    }

    boolean isSrcRemote = isRemoteFile(absoluteSrc);
    boolean isDstRemote = isRemoteFile(absoluteDst);

    // Check if src is a remote file
    // - if dest is a directory, move it locally
    // - if dest is a "remote file" too, they should be over the same endpoint
    // Is it possible to just rename a file? probably not, unless you know you're dealing with PDFS
    // and keep the endpoint prefix...
    if (isSrcRemote) {
      RemotePath srcRemotePath = getRemotePath(absoluteSrc);
      final String srcAddress = srcRemotePath.address;
      final Path srcPath = srcRemotePath.path;


      // Check destination
      if (isDstRemote) {
        RemotePath dstRemotePath = getRemotePath(absoluteDst);
        final String dstAddress = dstRemotePath.address;
        final Path dstPath = dstRemotePath.path;

        if (!Objects.equal(srcAddress, dstAddress)) {
          throw new IOException("Cannot rename across endpoints: from " + absoluteSrc + " to " + absoluteDst);
        }

        FileSystem delegateFS = getDelegateFileSystem(dstAddress);
        return delegateFS.rename(srcPath, dstPath);
      }

      // Since dst is not a remote file, it is assumed to be a directory, but
      // let's check if it exists
      boolean isDstDirectory = isDirectory(absoluteDst);
      if (!isDstDirectory) {
        throw new IOException("Rename destination " + absoluteDst + " does not exist or is not a directory");
      }

      // just in case, let's check that the directory exists on the remote endpoint
      FileSystem delegateFS = getDelegateFileSystem(srcAddress);
      try {
        FileStatus status = delegateFS.getFileStatus(absoluteDst);
        if (!status.isDirectory()) {
          throw new IOException("A remote file for path " + absoluteDst + " already exists on endpoint " + srcAddress);
        }
      } catch(FileNotFoundException e) {
        delegateFS.mkdirs(absoluteDst);
      }
      return delegateFS.rename(srcPath, absoluteDst);
    }

    // If dst is endpoint specific, let's assume it's a file (and src is a directory) and fail fast
    if (isDstRemote) {
      throw new IOException("Cannot rename " + absoluteSrc + " to " + absoluteDst);
    }

    // both src and dst should now be directories
    boolean isSrcDirectory = isDirectory(absoluteSrc);
    boolean isDstDirectory = isDirectory(absoluteDst);

    if (!(isSrcDirectory || isDstDirectory)) {
      throw new IOException("Cannot rename " + absoluteSrc + " to " + absoluteDst);
    }

    return new RenameTask(absoluteSrc, absoluteDst).get();
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    Path absolutePath = toAbsolutePath(f);
    checkPath(absolutePath);

    // Handle root
    if (absolutePath.isRoot()) {
      throw new AccessControlException("Cannot delete " + f);
    }

    if (!isRemoteFile(f)) {
      // In our remote view, there might be a directory, so delete task should handle this case
      return new DeleteTask(absolutePath, recursive).get();
    }

    try {
      RemotePath remotePath = getRemotePath(absolutePath);

      FileSystem delegate = getDelegateFileSystem(remotePath.address);
      return delegate.delete(remotePath.path, recursive);
    } catch (IllegalArgumentException e) {
      throw (FileNotFoundException) (new FileNotFoundException("No file " + absolutePath).initCause(e));
    }
  }

  private FileStatus fixFileStatus(String endpoint, FileStatus status) throws IOException {
    final Path remotePath = Path.getPathWithoutSchemeAndAuthority(status.getPath());

    if (status.isDirectory()) {
      return new PDFSFileStatus(makeQualified(remotePath), status);
    }

    String basename = remotePath.getName();
    boolean hidden = isHidden(basename);

    StringBuilder sb = new StringBuilder();
    if (hidden) {
      sb.append(basename.charAt(0));
    }
    sb.append(endpoint).append('@');
    sb.append(hidden ? basename.substring(1) : basename);

    return new PDFSFileStatus(makeQualified(new Path(remotePath.getParent(), sb.toString())), status);
  }

  private static final class PDFSFileStatus extends FileStatus {
    private final FileStatus delegate;
    private PDFSFileStatus(Path path, FileStatus delegate) {
      this.delegate = delegate;
      this.setPath(path);
    }

    @Override
    public long getLen() {
      return delegate.getLen();
    }

    @Override
    public boolean isFile() {
      return delegate.isFile();
    }

    @Override
    public boolean isDirectory() {
      return delegate.isDirectory();
    }

    @SuppressWarnings("deprecation")
    @Override
    public boolean isDir() {
      return delegate.isDir();
    }

    @Override
    public boolean isSymlink() {
      return delegate.isSymlink();
    }

    @Override
    public long getBlockSize() {
      return delegate.getBlockSize();
    }

    @Override
    public short getReplication() {
      return delegate.getReplication();
    }

    @Override
    public long getModificationTime() {
      return delegate.getModificationTime();
    }

    @Override
    public long getAccessTime() {
      return delegate.getAccessTime();
    }

    @Override
    public FsPermission getPermission() {
      return delegate.getPermission();
    }

    @Override
    public boolean isEncrypted() {
      return delegate.isEncrypted();
    }

    @Override
    public String getOwner() {
      return delegate.getOwner();
    }

    @Override
    public String getGroup() {
      return delegate.getGroup();
    }

    @Override
    public Path getSymlink() throws IOException {
      return delegate.getSymlink();
    }

    @Override
    public void setSymlink(Path p) {
      delegate.setSymlink(p);
    }
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    final RemoteIterator<FileStatus> remoteIterator = listStatusIterator(f);

    // Note: RemoteIterator has no relation to java.util.Iterator so frequently used
    // helper functions can't be called on them.
    final List<FileStatus> statuses = Lists.newArrayList();
    while (remoteIterator.hasNext()) {
      statuses.add(remoteIterator.next());
    }

    return statuses.toArray(new FileStatus[0]);
  }

  @Override
  public RemoteIterator<FileStatus> listStatusIterator(Path f) throws FileNotFoundException, IOException {
    final Path absolutePath = toAbsolutePath(f);
    checkPath(absolutePath);

    if (isRemoteFile(absolutePath)) {
      return new RemoteIterator<FileStatus>() {
        private boolean hasNext = true;
        @Override
        public boolean hasNext() throws IOException {
          return hasNext;
        }

        @Override
        public FileStatus next() throws IOException {
          if (!hasNext) {
            throw new NoSuchElementException();
          }

          hasNext = false;
          return getFileStatus(absolutePath);
        }
      };
    }

    return new ListStatusIteratorTask(absolutePath).get();
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    Path absolutePath = toAbsolutePath(newDir);
    checkPath(absolutePath);
    this.workingDirectory = absolutePath;
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDirectory;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    Path absolutePath = toAbsolutePath(f);
    checkPath(absolutePath);

    // Handle root
    if (absolutePath.isRoot()) {
      // Path always exists
      return true;
    }

    if (isRemoteFile(absolutePath)) {
      // Attempting to create a subdirectory for a file
      throw new IOException("Cannot create a directory under file " + f);
    }

    return new MkdirsTask(absolutePath, permission).get();
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    Path absolutePath = toAbsolutePath(f);
    checkPath(absolutePath);

    // if the path is not a remote file path
    if (!isRemoteFile(absolutePath)) {
      return new GetFileStatusTask(absolutePath).get();
    }

    // Parse top level directory
    try {
      RemotePath remotePath = getRemotePath(absolutePath);

      FileSystem delegate = getDelegateFileSystem(remotePath.address);
      FileStatus status = delegate.getFileStatus(remotePath.path);

      return fixFileStatus(remotePath.address, status);
    } catch (IllegalArgumentException e) {
      throw (FileNotFoundException) (new FileNotFoundException("No file " + absolutePath).initCause(e));
    }
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
    Preconditions.checkArgument(start >= 0, "start should be positive");
    Preconditions.checkArgument(len >= 0, "len should be positive");

    if (start >= file.getLen()) {
      // No block to cover after end of file
      return new BlockLocation[] {};
    }

    try {
      final RemotePath remotePath = getRemotePath(file.getPath());
      final List<NodeEndpoint> endpoints = getEndpoints(remotePath.address);

      if (endpoints.isEmpty()) {
        if (localAccessAllowed && localIdentity.getAddress().equals(remotePath.address)) { // best effort
          return new BlockLocation[]{
              new BlockLocation(
                  new String[]{format("%s:%d", localIdentity.getAddress(), localIdentity.getFabricPort())},
                  new String[]{localIdentity.getAddress()},
                  0, file.getLen())
          };
        }

        return new BlockLocation[]{};
      }

      String address = endpoints.get(0).getAddress();
      String[] hosts = new String[] { address };
      String[] names = new String[endpoints.size()];

      int i = 0;
      for (NodeEndpoint endpoint : endpoints) {
        names[i++] = format("%s:%d", address, endpoint.getFabricPort());
      }

      return new BlockLocation[] { new BlockLocation(names, hosts, 0, file.getLen()) };
    } catch (IllegalArgumentException e) {
      throw (FileNotFoundException) (new FileNotFoundException("No file " + file.getPath()).initCause(e));
    }
  }

  @Override
  public Path canonicalizePath(Path p) throws IOException {
    Path absolutePath = toAbsolutePath(p);
    checkPath(absolutePath);

    if (isRemoteFile(absolutePath)) {
      return absolutePath;
    }

    if (isDirectory(absolutePath)) {
      return absolutePath;
    }

    if (localAccessAllowed) {
      return createRemotePath(localIdentity.getAddress(), absolutePath);
    }

    // We aren't local or remote. That means we are a client node that wants to create a file.
    // We'll pick a random external location.
    final NodeEndpoint randomDelegate = getRandomDelegate();
    return createRemotePath(randomDelegate.getAddress(), absolutePath);
  }

  private Path createRemotePath(String address, Path path) {
    String basename = path.getName();
    return new Path(path.getParent(),
        isHidden(basename)
        ? String.format("%s%s@%s", basename.charAt(0), address, basename.substring(1))
        : String.format("%s@%s", address, basename)
    );
  }

  // DX-5178
  protected ExecutorService getExecutor(){
    // total hack. since our executor could have been shutdown in tests, we need
    // to also try to use the latest statically provided one just in case this
    // instance of pdfs is cached.
    synchronized(executor){
      if(executor.isShutdown()){
        return globalConfig.getExecutor();
      }else {
        return executor;
      }
    }
  }

  /**
   * Checks if a status for a file appears more than once.
   * @param newStatus The new status for a file.
   * @param oldStatuses Map of paths to previous file statuses.
   * @return True if the file has already appeared. False otherwise.
   */
  private static boolean checkDuplicateFileStatus(FileStatus newStatus, Map<String, FileStatus> oldStatuses) {
    final FileStatus previousStatus = oldStatuses.put(newStatus.getPath().getName(), newStatus);
    if (previousStatus != null) {
      // merge conflict
      if (previousStatus.isDirectory() != newStatus.isDirectory()) {
        // Trying to merge a file and a directory but it's not supposed to happen
        throw new IllegalStateException("Attempting to merge a file and a directory");
      }

      if (previousStatus.isFile()) {
        // Trying to merge two files from different endpoints. Should not be possible either
        throw new IllegalStateException("Attempting to merge two files for the same remote endpoint");
      }

      // TODO: DX-11234 Identify the correct behavior when multiple nodes have the same directory.
      return true;
    }
    return false;
  }

  private abstract class PDFSDistributedTask<V> {
    protected Iterable<Future<V>> map() throws IOException {
      Iterable<NodeEndpoint> endpoints = endpointProvider.get();
      final Set<String> addresses;
      if (!endpoints.iterator().hasNext()) {
        if (localAccessAllowed) {
          addresses = ImmutableSet.of(localIdentity.getAddress());
        } else {
          addresses = ImmutableSet.of();
        }
      } else {
        addresses = StreamSupport.stream(endpoints.spliterator(), false)
            .map(NodeEndpoint::getAddress)
            .collect(Collectors.toSet());

      }
      List<Future<V>> futures = new ArrayList<>();
      for(String address: addresses) {
        futures.add(getExecutor().submit(newMapTask(address)));
      }
      return futures;
    }

    protected final Path path;

    protected PDFSDistributedTask(Path path) {
      this.path = Path.getPathWithoutSchemeAndAuthority(path);
    }

    protected abstract Callable<V> newMapTask(String address) throws IOException;

    protected abstract V reduce(Iterable<Future<V>> futures) throws IOException;

    public V get() throws IOException {
      final Iterable<Future<V>> futures = map();
      return reduce(futures);
    }
  }

  private class ListStatusIteratorTask extends PDFSDistributedTask<RemoteIterator<FileStatus>> {
    public ListStatusIteratorTask(Path path) {
      super(path);
    }

    @Override
    protected Callable<RemoteIterator<FileStatus>> newMapTask(final String address) throws IOException {
      return new Callable<RemoteIterator<FileStatus>>() {
        @Override
        public RemoteIterator<FileStatus> call() throws Exception {
          // Only directories should be listed with a fork/join task
          final FileSystem fs = getDelegateFileSystem(address);
          FileStatus status = fs.getFileStatus(path);
          if (status.isFile()) {
            throw new FileNotFoundException("Directory not found: " + path);
          }
          final RemoteIterator<FileStatus> remoteStatusIter = fs.listStatusIterator(path);
          return new RemoteIterator<FileStatus>() {
            @Override
            public boolean hasNext() throws IOException {
              return remoteStatusIter.hasNext();
            }

            @Override
            public FileStatus next() throws IOException {
              return fixFileStatus(address, remoteStatusIter.next());
            }
          };
        }
      };
    }

    @Override
    protected RemoteIterator<FileStatus> reduce(final Iterable<Future<RemoteIterator<FileStatus>>> futures) throws IOException {
      return new RemoteIterator<FileStatus>() {
        private final Iterator<Future<RemoteIterator<FileStatus>>> statusIteratorIterator = futures.iterator();
        private RemoteIterator<FileStatus> currentRemoteIterator = null;
        private final Map<String, FileStatus> previousStatuses = new HashMap<>();
        private FileStatus nextAvailableStatus = null;

        @Override
        public boolean hasNext() throws IOException {
          // We have a status that was consumed by calling hasNext() without yet calling
          // next(). Just return true since the user hasn't actually iterated.
          if (nextAvailableStatus != null) {
            return true;
          }

          // Loop until we get a RemoteIterator with data.
          while (true) {
            while (currentRemoteIterator == null
              || !currentRemoteIterator.hasNext()) {

              // If there are no more Futures, stop.
              if (!statusIteratorIterator.hasNext()) {
                if (currentRemoteIterator == null) {
                  // We did not get any valid futures which means the directory did
                  // not exist on any nodes.
                  throw new FileNotFoundException("Path not found: " + path);
                }
                return false;
              } else {
                try {
                  // Evaluate the next future (results from a different node).
                  currentRemoteIterator = statusIteratorIterator.next().get();
                } catch (ExecutionException e) {
                  if (e.getCause() instanceof FileNotFoundException) {
                    // ignore
                    continue;
                  }
                  Throwables.propagateIfPossible(e.getCause(), IOException.class);
                  throw new RuntimeException(e);
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              }
            }
            // We got a new status from a RemoteIterator. Check if it's a duplicate.
            nextAvailableStatus = currentRemoteIterator.next();

            if (!checkDuplicateFileStatus(nextAvailableStatus, previousStatuses)) {
              // Not a duplicate.
              return true;
            }

            // Was a duplicate. Go back to the inner loop to find another status.
            // Also reset nextAvailableStatus to null to invalidate it.
            nextAvailableStatus = null;
          }
        }

        @Override
        public FileStatus next() throws IOException {
          if (hasNext()) {
            // Reset nextAvailableStatus so that hasNext knows to search for a new one.
            final FileStatus status = nextAvailableStatus;
            nextAvailableStatus = null;
            return status;
          }

          throw new NoSuchElementException();
        }
      };
    }
  }

  private class MkdirsTask extends PDFSDistributedTask<Boolean> {
    private final FsPermission permission;

    public MkdirsTask(Path path, FsPermission permission) {
      super(path);
      this.permission = permission;
    }

    @Override
    protected Callable<Boolean> newMapTask(final String address) throws IOException {
      return new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return getDelegateFileSystem(address).mkdirs(path, permission);
        }
      };
    }

    @Override
    protected Boolean reduce(Iterable<Future<Boolean>> tasks) throws IOException {
      boolean result = false;

      for(Future<Boolean> task: tasks) {
        boolean taskResult;
        try {
          taskResult = task.get();
        } catch(ExecutionException e) {
          Throwables.propagateIfPossible(e.getCause(), IOException.class);
          throw new RuntimeException(e);
        } catch(InterruptedException e) {
          throw new RuntimeException(e);
        }
        result |= taskResult;
      }

      return result;
    }
  }

  private class DeleteTask extends PDFSDistributedTask<Boolean> {
    private final boolean recursive;

    public DeleteTask(Path path, boolean recursive) {
      super(path);
      this.recursive = recursive;
    }

    @Override
    protected Callable<Boolean> newMapTask(final String address) throws IOException {
      return new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          // Only directories should be removed with a fork/join task
          final FileSystem fs = getDelegateFileSystem(address);
          FileStatus status = fs.getFileStatus(path);
          if (status.isFile()) {
            throw new FileNotFoundException("Directory not found: " + path);
          }
          return fs.delete(path, recursive);
        }
      };
    }

    @Override
    protected Boolean reduce(Iterable<Future<Boolean>> futures) throws IOException {
      boolean result = false;
      boolean found = false;
      for(Future<Boolean> task: futures) {
        try {
           result |= task.get();
        } catch(ExecutionException e) {
          if (e.getCause() instanceof FileNotFoundException) {
            // ignore
            continue;
          }
          Throwables.propagateIfPossible(e.getCause(), IOException.class);
          throw new RuntimeException(e);
        } catch(InterruptedException e) {
          throw new RuntimeException(e);
        }

        // At least the directory was found of one of remote endpoint
        found = true;
      }
      if (!found) {
        throw new FileNotFoundException("Directory not found: " + path);
      }
      return result;
    }
  }

  private class RenameTask extends PDFSDistributedTask<Boolean> {
    private final Path dst;

    public RenameTask(Path src, Path dst) {
      super(src);
      this.dst = dst;
    }

    @Override
    protected Callable<Boolean> newMapTask(final String address) throws IOException {
      return new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          // Only directories should be removed with a fork/join task
          final FileSystem fs = getDelegateFileSystem(address);
          FileStatus status = fs.getFileStatus(path);
          if (status.isFile()) {
            throw new FileNotFoundException("Directory not found: " + path);
          }
          return fs.rename(path, dst);
        }
      };
    }

    @Override
    protected Boolean reduce(Iterable<Future<Boolean>> futures) throws IOException {
      boolean result = false;
      boolean found = false;
      for(Future<Boolean> task: futures) {
        try {
           result |= task.get();
        } catch(ExecutionException e) {
          if (e.getCause() instanceof FileNotFoundException) {
            // ignore
            continue;
          }
          Throwables.propagateIfPossible(e.getCause(), IOException.class);
          throw new RuntimeException(e);
        } catch(InterruptedException e) {
          throw new RuntimeException(e);
        }

        // At least the directory was found of one of remote endpoint
        found = true;
      }
      if (!found) {
        throw new FileNotFoundException("Directory not found: " + path);
      }
      return result;
    }
  }

  private class GetFileStatusTask extends PDFSDistributedTask<FileStatus> {
    public GetFileStatusTask(Path path) {
      super(path);
    }

    @Override
    protected Callable<FileStatus> newMapTask(final String address) throws IOException {
      // TODO Auto-generated method stub
      return new Callable<FileStatus>() {
        @Override
        public FileStatus call() throws Exception {
          // Only directories should be removed with a fork/join task
          FileStatus status = getDelegateFileSystem(address).getFileStatus(path);
          if (status.isFile()) {
            throw new FileNotFoundException("Directory not found: " + path);
          }
          return status;
        }
      };
    }

    @Override
    protected FileStatus reduce(Iterable<Future<FileStatus>> futures) throws IOException {

      FileStatus fileStatus = null;
      long maxModificationTime = Long.MIN_VALUE;
      long maxAccessTime = Long.MIN_VALUE;
      for(Future<FileStatus> f : futures) {
        try {
            fileStatus = f.get();
            maxModificationTime = Math.max(maxModificationTime, fileStatus.getModificationTime());
            maxAccessTime = Math.max(maxAccessTime, fileStatus.getAccessTime());
        } catch(ExecutionException e) {
          if (e.getCause() instanceof FileNotFoundException) {
            // ignore
            continue;
          }
          Throwables.propagateIfPossible(e.getCause(), IOException.class);
          throw new RuntimeException(e);
        } catch(InterruptedException e) {
          throw new RuntimeException(e);
        }
      }

      if (fileStatus == null) {
        throw new FileNotFoundException("Directory not found:" + path);
      }

      return fixFileStatus(null, new FileStatus(
          fileStatus.getLen(),
          fileStatus.isDirectory(),
          fileStatus.getReplication(),
          fileStatus.getBlockSize(),
          maxModificationTime,
          maxAccessTime,
          fileStatus.getPermission(),
          fileStatus.getOwner(),
          fileStatus.getGroup(),
          fileStatus.getPath()
          ));
    }
  }
}
