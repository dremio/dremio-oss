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
package com.dremio.plugins.adl.store;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.adl.AdlFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;

import com.dremio.exec.hadoop.MayProvideAsyncStream;
import com.dremio.exec.store.dfs.FileSystemConf;
import com.dremio.io.AsyncByteReader;
import com.google.common.base.Preconditions;
import com.microsoft.azure.datalake.store.ADLSClient;
import com.microsoft.azure.datalake.store.AdlsListPathResponse;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.DirectoryEntryType;
import com.microsoft.azure.datalake.store.UserGroupRepresentation;


/**
 * Specialized Hadoop FileSystem implementation for ADLS gen 1 which adds async reading capabilities.
 * Also supports batchlisting of directory entries
 */
@SuppressWarnings("Unchecked")
public class DremioAdlFileSystem extends AdlFileSystem implements MayProvideAsyncStream {
  class AdlPermission extends FsPermission {
    private final boolean aclBit;

    AdlPermission(boolean aclBitStatus, Short aShort) {
      super(aShort);
      this.aclBit = aclBitStatus;
    }

    public boolean getAclBit() {
      return this.aclBit;
    }

    public boolean equals(Object obj) {
      if (!(obj instanceof FsPermission)) {
        return false;
      } else {
        FsPermission that = (FsPermission) obj;
        return this.getUserAction() == that.getUserAction() && this.getGroupAction() == that.getGroupAction() && this.getOtherAction() == that.getOtherAction() && this.getStickyBit() == that.getStickyBit();
      }
    }

    public int hashCode() {
      return this.toShort();
    }
  }

  public FileStatus toFileStatus(DirectoryEntry entry, Path f) throws IOException {
    boolean overrideOwner = getConf().getBoolean("adl.debug.override.localuserasfileowner", false);
    boolean aclBitStatus = getConf().getBoolean("adl.feature.support.acl.bit", true);
    boolean aclBit = aclBitStatus ? entry.aclBit : false;
    return overrideOwner ? new FileStatus(entry.length, DirectoryEntryType.DIRECTORY == entry.type, 1, entry.blocksize, entry.lastModifiedTime.getTime(), entry.lastAccessTime.getTime(), new AdlPermission(aclBit, Short.parseShort(entry.permission, 8)), UserGroupInformation.getCurrentUser().getShortUserName(), UserGroupInformation.getCurrentUser().getPrimaryGroupName(), (Path) null, f) : new FileStatus(entry.length, DirectoryEntryType.DIRECTORY == entry.type, 1, entry.blocksize, entry.lastModifiedTime.getTime(), entry.lastAccessTime.getTime(), new AdlPermission(aclBit, Short.parseShort(entry.permission, 8)), entry.user, entry.group, (Path) null, f);

  }


  class AdlsListIterator implements RemoteIterator<LocatedFileStatus> {
    private AdlsListPathResponse currBatchResponse;
    private int currIndex;
    private Path path;
    private UserGroupRepresentation oidOrUpn;

    public AdlsListIterator(Path path, AdlsListPathResponse response) {
      currBatchResponse = response;
      currIndex = 0;
      this.path = path;
    }

    public AdlsListIterator(Path path, UserGroupRepresentation oidOrUpn, AdlsListPathResponse response) {
      currBatchResponse = response;
      currIndex = 0;
      this.path = path;
      this.oidOrUpn = oidOrUpn;
    }

    public Path getPath() {
      return path;
    }

    public void setPath(Path path) {
      this.path = path;
    }

    @Override
    public boolean hasNext() throws IOException {
      return currIndex < currBatchResponse.getEntries().size();
    }

    @Override
    public LocatedFileStatus next() throws IOException {
      Preconditions.checkArgument(hasNext(), "No next found");
      DirectoryEntry currentFile = currBatchResponse.getEntries().get(currIndex);
      currIndex = currIndex + 1;
      if (currBatchResponse.shouldLoadNextBatch(currIndex)) {
        currBatchResponse = getAdlClient().enumerateDirectories(currBatchResponse.getPath(), oidOrUpn, currBatchResponse.getContinuation());
        currIndex = 0;
      }
      return new LocatedFileStatus(toFileStatus(currentFile, new Path(currentFile.fullName)), null);
    }
  }

  //recursive case handled from dremio code (in filesystem class)
  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f) throws FileNotFoundException, IOException {
    statistics.incrementReadOps(1);
    boolean enableUPN = getConf().getBoolean("adl.feature.ownerandgroup.enableupn", false);
    UserGroupRepresentation oidOrUpn = enableUPN ? UserGroupRepresentation.UPN : UserGroupRepresentation.OID;
    return new AdlsListIterator(f, oidOrUpn, getAdlClient().enumerateDirectories(f.toString(), oidOrUpn, null));
  }

  private volatile AsyncHttpClientManager asyncHttpClientManager;

  @Override
  public String getScheme() {
    return FileSystemConf.CloudFileSystemScheme.ADL_FILE_SYSTEM_SCHEME.getScheme();
  }

  @Override
  public void close() throws IOException {
    if (asyncHttpClientManager != null) {
      asyncHttpClientManager.close();
    }
  }

  @Override
  public boolean supportsAsync() {
    return true;
  }

  @Override
  public AsyncByteReader getAsyncByteReader(Path path, String version, Map<String, String> options) throws IOException {
    if (asyncHttpClientManager == null) {
      synchronized (this) {
        if (asyncHttpClientManager == null) {
          final AzureDataLakeConf adlsConf = AzureDataLakeConf.fromConfiguration(getUri(), getConf());
          asyncHttpClientManager = new AsyncHttpClientManager("dist-uri-" + getUri().toASCIIString(), adlsConf);
        }
      }
    }

    return new AdlsAsyncFileReader(
      new ADLSClient(asyncHttpClientManager.getClient()),
      asyncHttpClientManager.getAsyncHttpClient(),
      path.toUri().getPath(), version, this, asyncHttpClientManager.getUtilityThreadPool());
  }
}
