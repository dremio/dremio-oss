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
package com.dremio.plugins.s3.store;

import static com.dremio.plugins.s3.store.S3StoragePlugin.ACCESS_KEY_PROVIDER;
import static com.dremio.plugins.s3.store.S3StoragePlugin.EC2_METADATA_PROVIDER;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.Region;
import com.dremio.plugins.s3.store.copy.S3ClientFactory.DefaultS3ClientFactory;
import com.dremio.plugins.s3.store.copy.S3Constants;
import com.dremio.plugins.util.ContainerFileSystem;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 * FileSystem implementation that treats multiple s3 buckets as a unified namespace
 */
public class S3FileSystem extends ContainerFileSystem {

  private static final Logger logger = LoggerFactory.getLogger(S3FileSystem.class);
  private static final String S3_URI_SCHEMA = "s3a://";
  private static final URI S3_URI = URI.create("s3a://aws"); // authority doesn't matter here, it is just to avoid exceptions

  private AmazonS3 s3;
  private String ownerId = null;

  // TODO: why static?
  private static final LoadingCache<S3ClientKey, AmazonS3> clientCache = CacheBuilder
          .newBuilder()
          .expireAfterAccess(1,TimeUnit.HOURS)
          .maximumSize(20)
          .build(new CacheLoader<S3ClientKey, AmazonS3>() {
            @Override
            public AmazonS3 load(S3ClientKey clientKey) throws Exception {
              logger.debug("Opening S3 client connection for {}", clientKey);
              DefaultS3ClientFactory clientFactory = new DefaultS3ClientFactory();
              clientFactory.setConf(clientKey.s3Config);
              return clientFactory.createS3Client(S3_URI);
            }
          }); // Looks like there is no close/cleanup for AmazonS3Client


  public S3FileSystem() {
    super("dremioS3", "bucket", ELIMINATE_PARENT_DIRECTORY);
  }

  // Work around bug in s3a filesystem where the parent directory is included in list. Similar to HADOOP-12169
  private static final Predicate<CorrectableFileStatus> ELIMINATE_PARENT_DIRECTORY =
      (input -> {
        final FileStatus status = input.getStatus();
        if (!status.isDirectory()) {
          return true;
        }
        return !Path.getPathWithoutSchemeAndAuthority(input.getPathWithoutContainerName()).equals(Path.getPathWithoutSchemeAndAuthority(status.getPath()));
      });

  @Override
  protected void setup(Configuration conf) throws IOException {
    try {
      s3 = clientCache.get(S3ClientKey.create(conf));

      if (!S3StoragePlugin.NONE_PROVIDER.equals(conf.get(S3Constants.AWS_CREDENTIALS_PROVIDER))) {
        ownerId = s3.getS3AccountOwner().getId();
      }
    } catch (ExecutionException e) {
      if(e.getCause() != null && e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
    }
  }


  @Override
  public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive) throws FileNotFoundException, IOException {
    return super.listFiles(f, recursive);
  }


  @Override
  protected Iterable<ContainerCreator> getContainerCreators() throws IOException {

    String externalBucketList = getConf().get(S3StoragePlugin.EXTERNAL_BUCKETS);
    FluentIterable<String> buckets = externalBucketList == null ? FluentIterable.of(new String[0]) :
        FluentIterable.of(externalBucketList.split(","))
            .transform(input -> input.trim())
            .filter(input -> !Strings.isNullOrEmpty(input));

    if (ACCESS_KEY_PROVIDER.equals(getConf().get(S3Constants.AWS_CREDENTIALS_PROVIDER))
        || EC2_METADATA_PROVIDER.equals(getConf().get(S3Constants.AWS_CREDENTIALS_PROVIDER))) {
      // if we have authentication to access S3, add in owner buckets.
      buckets = buckets.append(FluentIterable.from(s3.listBuckets()).transform(input -> input.getName()));
    }

    return FluentIterable.from(buckets.toSet()).transform(input -> new BucketCreator(getConf(), input));
  }

  /**
   * Checks if the account may have write permission to the given bucket.
   *
   * @param bucketName bucket name
   * @return false (implies no write permission) or true (implies maybe)
   */
  boolean mayHaveWritePermission(String bucketName) {
    assert containerExists(bucketName);
    if (ownerId == null) {
      return true; // cannot get ACL for anonymous users
    }

    final AccessControlList acl;
    try {
      acl = s3.getBucketAcl(bucketName);
    } catch (AmazonS3Exception e) {
      if ("AccessDenied".equals(e.getErrorCode())) {
        return false;
      }

      return true; // getting ACL itself failed
    }

    // using deprecated method (getGrants) to build with mapr
    boolean checked = false;
    for (Grant grant : acl.getGrants()) {
      if (ownerId.equals(grant.getGrantee().getIdentifier())) {
        checked = true;
        switch (grant.getPermission()) {
        case FullControl:
        case Write:
        case WriteAcp:
          return true;

        default:
          // there could be multiple grants for same grantee (unclear API)
          break;
        }
      }
    }

    return !checked;
  }

  @Override
  protected ContainerHolder getUnknownContainer(String name) {
    // no lazy loading

    // Per docs, if invalid security credentials are used to execute
    // AmazonS3#doesBucketExist method, the client is not able to distinguish
    // between bucket permission errors and invalid credential errors, and the
    // method could return an incorrect result.

    // New S3 buckets will be visible on the next refresh.

    return null;
  }

  private class BucketCreator extends ContainerCreator {
    private final Configuration parentConf;
    private final String bucketName;

    public BucketCreator(Configuration parentConf, String bucketName) {
      super();
      this.parentConf = parentConf;
      this.bucketName = bucketName;
    }

    @Override
    protected String getName() {
      return bucketName;
    }

    @Override
    protected ContainerHolder toContainerHolder() throws IOException {

      return new ContainerHolder(bucketName, new FileSystemSupplier() {
        @Override
        public FileSystem create() throws IOException {
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
          final Configuration bucketConf = new Configuration(parentConf);
          bucketConf.set(ENDPOINT, (regionEndPoint != null) ? regionEndPoint : projectedBucketEndPoint);
          FileSystem.setDefaultUri(bucketConf, new Path(location).toUri());
          return FileSystem.get(bucketConf);
        }
      });
    }

  }

  /**
   * Key to identify a connection.
   */
  public static final class S3ClientKey {

    /**
     * List of properties unique to a connection. This works in conjuction with {@link DefaultS3ClientFactory}
     * implementation.
     */
    private static final List<String> UNIQUE_PROPS = ImmutableList.of(
        Constants.ACCESS_KEY,
        Constants.SECRET_KEY,
        Constants.SECURE_CONNECTIONS,
        Constants.ENDPOINT,
        S3Constants.AWS_CREDENTIALS_PROVIDER,
        Constants.MAXIMUM_CONNECTIONS,
        Constants.MAX_ERROR_RETRIES,
        Constants.ESTABLISH_TIMEOUT,
        Constants.SOCKET_TIMEOUT,
        S3Constants.SOCKET_SEND_BUFFER,
        S3Constants.SOCKET_RECV_BUFFER,
        S3Constants.SIGNING_ALGORITHM,
        S3Constants.USER_AGENT_PREFIX,
        Constants.PROXY_HOST,
        Constants.PROXY_PORT,
        Constants.PROXY_DOMAIN,
        Constants.PROXY_USERNAME,
        Constants.PROXY_PASSWORD,
        Constants.PROXY_WORKSTATION,
        S3Constants.PATH_STYLE_ACCESS
    );

    private final Configuration s3Config;

    public static S3ClientKey create(final Configuration fsConf) {
      return new S3ClientKey(fsConf);
    }

    private S3ClientKey(final Configuration s3Config) {
      this.s3Config = s3Config;
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

      for(String prop : UNIQUE_PROPS) {
        if (!Objects.equals(s3Config.get(prop), that.s3Config.get(prop))) {
          return false;
        }
      }

      return true;
    }

    @Override
    public int hashCode() {
      int hash = 1;
      for(String prop : UNIQUE_PROPS) {
        hash = Objects.hash(hash, s3Config.get(prop));
      }

      return hash;
    }

    @Override
    public String toString() {
      return "[ Access Key=" + s3Config.get(Constants.ACCESS_KEY) + ", Secret Key =*****, isSecure=" +
          s3Config.get(Constants.SECURE_CONNECTIONS) + " ]";
    }
  }
}
