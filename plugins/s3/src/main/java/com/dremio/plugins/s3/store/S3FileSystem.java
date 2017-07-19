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
package com.dremio.plugins.s3.store;

import static org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.directory.api.util.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.Region;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
/**
 * FileSystem implementation that treats multiple s3 buckets as a unified namespace
 */
public class S3FileSystem extends FileSystem {
  private static final Logger logger = LoggerFactory.getLogger(S3FileSystem.class);
  private static final String S3_URI_SCHEMA = "s3a://";

  private Map<String,FileSystem> bucketFileSystemMap = Maps.newConcurrentMap();

  private static final LoadingCache<S3ClientKey, AmazonS3Client> clientCache = CacheBuilder
          .newBuilder()
          .expireAfterAccess(1,TimeUnit.HOURS)
          .maximumSize(20)
          .build(new CacheLoader<S3ClientKey, AmazonS3Client>() {
            @Override
            public AmazonS3Client load(S3ClientKey clientKey) throws Exception {
              logger.debug("Opening S3 client connection for {}", clientKey);
              ClientConfiguration clientConf = new ClientConfiguration();
              clientConf.setProtocol(clientKey.isSecure ? Protocol.HTTPS : Protocol.HTTP);

              if(clientKey.accessKey == null){
                return new AmazonS3Client(new AnonymousAWSCredentialsProvider(), clientConf);
              }else{
                return new AmazonS3Client(new BasicAWSCredentials(clientKey.accessKey, clientKey.secretKey), clientConf);
              }
            }
          }); // Looks like there is no close/cleanup for AmazonS3Client

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    try {
      AmazonS3 s3 = clientCache.get(S3ClientKey.create(conf));

      if(conf.get(ACCESS_KEY) != null){
        List<Bucket> buckets = s3.listBuckets();
        for (Bucket b : buckets) {
          addBucketFSEntry(s3, conf, b.getName());
        }
      }

      String externalBucketString = conf.get(S3PluginConfig.EXTERNAL_BUCKETS);
      if (externalBucketString != null) {
        for (String bucket : externalBucketString.split(",")) {
          final String trimmedBucket = bucket.trim();
          if (Strings.isEmpty(trimmedBucket)) {
            continue;
          }
          addBucketFSEntry(s3, conf, trimmedBucket);
        }
      }
    } catch (final Exception e) {
      throw new RuntimeException("Failed to create workspaces for buckets owned by the account.", e);
    }
  }

  /**
   * To reuse code for accountbased and external buckets
   * @param s3
   * @param parentConf
   * @param bucketName
   * @throws IOException
   */
  private void addBucketFSEntry(AmazonS3 s3, Configuration parentConf, String bucketName) throws IOException {
    try {
      final String bucketRegion = s3.getBucketLocation(bucketName);
      final String projectedBucketEndPoint = "s3." + bucketRegion + ".amazonaws.com";
      String regionEndPoint = projectedBucketEndPoint;
      try {
        Region region = Region.fromValue(bucketRegion);
        com.amazonaws.regions.Region awsRegion = region.toAWSRegion();
        if (awsRegion != null) {
          regionEndPoint = awsRegion.getServiceEndpoint("s3");
        }
      } catch (IllegalArgumentException iae) {
        // try heuristic mapping if not found
        regionEndPoint = projectedBucketEndPoint;
        logger.warn("Unknown or unmapped region {} for bucket {}. Will use following fs.s3a.endpoint: {}",
          bucketRegion, bucketName, regionEndPoint);
      }
      // it could be null because no mapping from Region to aws region or there is no such region is the map of endpoints
      // not sure if latter is possible
      if (regionEndPoint == null) {
        logger.error("Could not get AWSRegion for bucket {}. Will use following fs.s3a.endpoint: " + "{} ",
          bucketName, projectedBucketEndPoint);
      }
      String location = S3_URI_SCHEMA + bucketName + "/";
      Configuration bucketConf = new Configuration(parentConf);
      bucketConf.set(ENDPOINT, (regionEndPoint != null) ? regionEndPoint : projectedBucketEndPoint);
      FileSystem.setDefaultUri(bucketConf, new Path(location).toUri());
      bucketFileSystemMap.put(bucketName, FileSystem.get(bucketConf));
    } catch (AmazonS3Exception as3ex) {
      logger.info("Unable to process bucket: {} with error: {}", bucketName, as3ex.getLocalizedMessage());
    } catch (Exception ex) {
      logger.error("Unable to process bucket: " + bucketName, ex);
    }
  }

  private boolean isRoot(Path path) {
    List<String> pathComponents = Arrays.asList(Path.getPathWithoutSchemeAndAuthority(path).toString().split(Path.SEPARATOR));
    return pathComponents.size() == 0;
  }

  private String getBucket(Path path) {
    List<String> pathComponents = Arrays.asList(removeLeadingSlash(Path.getPathWithoutSchemeAndAuthority(path).toString()).split(Path.SEPARATOR));
    return pathComponents.get(0);
  }

  private static String removeLeadingSlash(String path) {
    if (path.charAt(0) == '/') {
      return path.substring(1, path.length());
    } else {
      return path;
    }
  }

  private Path pathWithoutBucket(Path path) {
    List<String> pathComponents = Arrays.asList(removeLeadingSlash(Path.getPathWithoutSchemeAndAuthority(path).toString()).split(Path.SEPARATOR));
    return new Path("/" + Joiner.on(Path.SEPARATOR).join(pathComponents.subList(1, pathComponents.size())));
  }

  private FileSystem getFileSystemForPath(Path path) {
    String bucket = getBucket(path);
    return bucketFileSystemMap.get(bucket);
  }

  /**
   * Key to identify a connection.
   */
  public static final class S3ClientKey {
    private final String accessKey;
    private final String secretKey;
    private final boolean isSecure;

    public static S3ClientKey create(final Configuration fsConf) {
      return new S3ClientKey(
              fsConf.get(Constants.ACCESS_KEY),
              fsConf.get(Constants.SECRET_KEY),
              Boolean.valueOf(fsConf.get(Constants.SECURE_CONNECTIONS, "true" /*default is true*/)));
    }

    private S3ClientKey(final String accessKey, final String secretKey, final boolean isSecure) {
      this.accessKey = accessKey;
      this.secretKey = secretKey;
      this.isSecure = isSecure;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      S3ClientKey that = (S3ClientKey) o;
      return Objects.equals(accessKey, that.accessKey) &&
              Objects.equals(secretKey, that.secretKey) &&
              isSecure == that.isSecure;
    }

    @Override
    public int hashCode() {
      return Objects.hash(accessKey, secretKey, isSecure);
    }

    @Override
    public String toString() {
      return "[ Access Key=" + accessKey + ", Secret Key =*****, isSecure="+ isSecure + " ]";
    }
  }

  @Override
  public URI getUri() {
    try {
      return new URI("dremioS3:///");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return getFileSystemForPath(f).open(pathWithoutBucket(f), bufferSize);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    return getFileSystemForPath(f).create(pathWithoutBucket(f), permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    return getFileSystemForPath(f).append(pathWithoutBucket(f), bufferSize, progress);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    Preconditions.checkArgument(getBucket(src).equals(getBucket(dst)), "Cannot rename files across buckets");
    return getFileSystemForPath(src).rename(pathWithoutBucket(src), pathWithoutBucket(dst));
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return getFileSystemForPath(f).delete(pathWithoutBucket(f), recursive);
  }

  @Override
  public FileStatus[] listStatus(final Path f) throws FileNotFoundException, IOException {
    if (isRoot(f)) {
      return Iterables.toArray(Iterables.transform(bucketFileSystemMap.keySet(), new Function<String,FileStatus>() {

        @Nullable
        @Override
        public FileStatus apply(@Nullable String bucketName) {
          return new FileStatus(0, true, 0, 0, 0, new Path(bucketName));
        }
      }), FileStatus.class);
    }
    final String bucket = getBucket(f);
    final Path pathWithoutBucket = pathWithoutBucket(f);
    // Work around bug in s3a filesystem where the parent directory is included in list. Similar to HADOOP-12169
    Iterable<FileStatus> correctedFiles = Iterables.filter(Arrays.asList(getFileSystemForPath(f).listStatus(pathWithoutBucket)), new Predicate<FileStatus>() {
      @Override
      public boolean apply(@Nullable FileStatus input) {
        if (!input.isDirectory()) {
          return true;
        }
        return !Path.getPathWithoutSchemeAndAuthority(pathWithoutBucket).equals(Path.getPathWithoutSchemeAndAuthority(input.getPath()));
      }
    });
    return Iterables.toArray(Iterables.transform(correctedFiles, new Function<FileStatus, FileStatus>() {
      @Nullable
      @Override
      public FileStatus apply(@Nullable FileStatus input) {
        return transform(input, bucket);
      }
    }), FileStatus.class);
  }

  private static FileStatus transform(FileStatus input, String bucket) {
    String relativePath = removeLeadingSlash(Path.getPathWithoutSchemeAndAuthority(input.getPath()).toString());
    Path bucketPath  = new Path(Path.SEPARATOR + bucket);
    Path fullPath = Strings.isEmpty(relativePath) ? bucketPath : new Path(bucketPath, relativePath);
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
  public void setWorkingDirectory(Path new_dir) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Path getWorkingDirectory() {
    return new Path("/");
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return getFileSystemForPath(f).mkdirs(f, permission);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    if (isRoot(f)) {
      return new FileStatus(0, true, 0, 0, 0, f);
    }
    FileSystem fs = getFileSystemForPath(f);
    if (fs == null) {
      throw new FileNotFoundException(String.format("%s not found", f));
    }
    FileStatus fileStatus = getFileSystemForPath(f).getFileStatus(pathWithoutBucket(f));
    String bucket = getBucket(f);
    return transform(fileStatus, bucket);
  }

  @Override
  public String getScheme() {
    return "dremioS3";
  }
}
