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
package com.dremio.plugins.s3.store;

import static com.dremio.plugins.s3.store.S3StoragePlugin.ACCESS_KEY_PROVIDER;
import static com.dremio.plugins.s3.store.S3StoragePlugin.ASSUME_ROLE_PROVIDER;
import static com.dremio.plugins.s3.store.S3StoragePlugin.EC2_METADATA_PROVIDER;
import static com.dremio.plugins.s3.store.S3StoragePlugin.NONE_PROVIDER;
//import static com.dremio.plugins.s3.store.S3StoragePlugin.TEMP_PROVIDER;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.SECURE_CONNECTIONS;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3ClientFactoryHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Region;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.hadoop.MayProvideAsyncStream;
import com.dremio.exec.store.dfs.DremioFileSystemCache;
import com.dremio.io.AsyncByteReader;
import com.dremio.plugins.util.ContainerFileSystem;
import com.google.common.base.FinalizablePhantomReference;
import com.google.common.base.FinalizableReference;
import com.google.common.base.FinalizableReferenceQueue;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityRequest;
import software.amazon.awssdk.services.sts.model.StsException;

/**
 * FileSystem implementation that treats multiple s3 buckets as a unified namespace
 */
public class S3FileSystem extends ContainerFileSystem implements MayProvideAsyncStream {

  public static final String COMPATIBILITY_MODE = "dremio.s3.compat";
  public static final String REGION_OVERRIDE = "dremio.s3.region";
  public static final String ACCESS_DENIED = "AccessDenied";

  private static final Logger logger = LoggerFactory.getLogger(S3FileSystem.class);
  private static final String S3_URI_SCHEMA = "s3a://";
  private static final URI S3_URI = URI.create("s3a://aws"); // authority doesn't matter here, it is just to avoid exceptions

  // Used to close the S3AsyncClient objects
  // Lifecycle of an S3AsyncClient object:
  // - created by the LoadingCache on first access
  //   - inserted in the set of async client references, below, upon creation
  // - accessed by readers, and potentially held longer than it takes for the LoadingCache to evict
  // - evicted from the LoadingCache some time after last access
  // - once both the LoadingCache and the reader finish using the S3AsyncClient, the only outstanding reference
  //   is the one in the async client reference set
  // - GC is allowed to reap such objects. Upon finalize, the S3AsyncClient is closed, and removed from the
  //   async client reference set
  private static final FinalizableReferenceQueue FINALIZABLE_REFERENCE_QUEUE = new FinalizableReferenceQueue();

  // TODO: why static?
  private static final LoadingCache<S3ClientKey, AmazonS3> clientCache = CacheBuilder
          .newBuilder()
          .expireAfterAccess(1,TimeUnit.HOURS)
          .maximumSize(20)
          .build(new CacheLoader<S3ClientKey, AmazonS3>() {
            @Override
            public AmazonS3 load(S3ClientKey clientKey) throws Exception {
              logger.debug("Opening S3 client connection for {}", clientKey);
              return S3ClientFactoryHelper.createS3ClientHelper(clientKey.s3Config, S3_URI);
            }
          }); // Looks like there is no close/cleanup for AmazonS3Client

  @SuppressWarnings("MismatchedQueryAndUpdateOfCollection") // intentionally just updating and not querying. See large comment above
  private final Set<FinalizableReference> asyncClientReferences = Sets.newConcurrentHashSet();
  private final Set<FinalizableReference> syncClientReferences = Sets.newConcurrentHashSet();

  private final DremioFileSystemCache fsCache = new DremioFileSystemCache();

  private final LoadingCache<String, S3Client> syncClientCache = CacheBuilder
    .newBuilder()
    .expireAfterAccess(1, TimeUnit.HOURS)
    .build(new CacheLoader<String, S3Client>() {
      @Override
      public S3Client load(String bucket) throws Exception {
        return newSyncClientReference(bucket);
      }
    });

  private AmazonS3 s3;
  private boolean useWhitelistedBuckets;

  private S3Client newSyncClientReference(final String bucket) {
    S3Client syncClient = configClientBuilder(S3Client.builder(), bucket).build();
    FinalizableReference ref = new FinalizablePhantomReference<S3Client>(syncClient, FINALIZABLE_REFERENCE_QUEUE) {
      @Override
      public void finalizeReferent() {
        try {
          syncClient.close();
        } catch (Exception e) {
          logger.warn(String.format("Failed to close the S3 sync client for bucket %s", bucket), e);
        }
      }
    };
    syncClientReferences.add(ref);
    return syncClient;
  }

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
  protected void setup(Configuration conf) throws IOException, RuntimeException {
    try {
      s3 = clientCache.get(S3ClientKey.create(conf));
      useWhitelistedBuckets = !conf.get(S3StoragePlugin.WHITELISTED_BUCKETS,"").isEmpty();
      if (!S3StoragePlugin.NONE_PROVIDER.equals(conf.get(Constants.AWS_CREDENTIALS_PROVIDER))
        && !conf.getBoolean(COMPATIBILITY_MODE, false)) {
        verifyCredentials(conf);
      }
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      } else if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      } else if (cause != null) {
        throw new RuntimeException(cause);
      }
      throw new RuntimeException(e);
    }
  }

  /**
   * Checks if credentials are valid using GetCallerIdentity API call.
   */
  protected void verifyCredentials(Configuration conf) throws RuntimeException {
    AwsCredentialsProvider awsCredentialsProvider = getAsync2Provider(conf);
    final StsClientBuilder stsClientBuilder = StsClient.builder()
      .credentialsProvider(awsCredentialsProvider)
      .region(getAwsRegionFromConfigurationOrDefault(conf));
    try (StsClient stsClient = stsClientBuilder.build()) {
      GetCallerIdentityRequest request = GetCallerIdentityRequest.builder().build();
      stsClient.getCallerIdentity(request);
    } catch (StsException e) {
      throw new RuntimeException(String.format("Credential Verification failed. Exception: %s ", e.getLocalizedMessage()), e);
    }
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive) throws FileNotFoundException, IOException {
    return super.listFiles(f, recursive);
  }

  @Override
  protected Stream<ContainerCreator> getContainerCreators() throws IOException {


    FluentIterable<String> buckets = getBucketNamesFromConfigurationProperty(S3StoragePlugin.EXTERNAL_BUCKETS);
    if (!NONE_PROVIDER.equals(getConf().get(Constants.AWS_CREDENTIALS_PROVIDER))) {
      if (!useWhitelistedBuckets) {
        // if we have authentication to access S3, add in owner buckets.
        buckets = buckets.append(FluentIterable.from(s3.listBuckets()).transform(input -> input.getName()));
      } else {
        // Only add the buckets provided in the configuration.
        buckets = buckets.append(FluentIterable.from(getBucketNamesFromConfigurationProperty(S3StoragePlugin.WHITELISTED_BUCKETS)));
      }

    }
    return buckets.toSet() // Remove duplicate bucket names.
        .stream()
        .map(input -> new BucketCreator(getConf(), input));
  }

  private FluentIterable<String> getBucketNamesFromConfigurationProperty(String bucketConfigurationProperty) {
    String bucketList = getConf().get(bucketConfigurationProperty,"");
    return FluentIterable.of(bucketList.split(","))
            .transform(input -> input.trim())
            .filter(input -> !Strings.isNullOrEmpty(input));
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

  software.amazon.awssdk.regions.Region getAWSBucketRegion(String bucketName) throws SdkClientException {
    String awsRegionName = Region.fromValue(s3.getBucketLocation(bucketName)).toAWSRegion().getName();
    return software.amazon.awssdk.regions.Region.of(awsRegionName);
  }

  @Override
  public boolean supportsAsync() {
    return true;
  }

  @Override
  public AsyncByteReader getAsyncByteReader(Path path, String version) throws IOException {
    final String bucket = ContainerFileSystem.getContainerName(path);
    //return new S3AsyncByteReader(getAsyncClient(bucket), bucket,
    //  ContainerFileSystem.pathWithoutContainer(path).toString());
    return new S3AsyncByteReaderUsingSyncClient(getSyncClient(bucket), bucket,
      ContainerFileSystem.pathWithoutContainer(path).toString(), version);
  }

  private S3Client getSyncClient(String bucket) throws IOException {
    try {
      return syncClientCache.get(bucket);
    } catch (ExecutionException | SdkClientException e ) {
      Throwable toChain = e;
      if (e.getCause() != null) {
        Throwable cause = e.getCause();
        if (cause instanceof UserException) {
          throw (UserException) cause;
        } else if (e.getCause() instanceof IOException) {
          throw (IOException) e.getCause();
        } else {
          toChain = e.getCause();
        }
      }

      throw new IOException(String.format("Unable to create a sync S3 client for bucket %s", bucket), toChain);
    }
  }

  @Override
  public void close() throws IOException {
    fsCache.closeAll(true);
    super.close();
  }

  private AwsCredentialsProvider getAsync2Provider(Configuration config) {
    switch(config.get(Constants.AWS_CREDENTIALS_PROVIDER)) {
      case ACCESS_KEY_PROVIDER:
        return StaticCredentialsProvider.create(AwsBasicCredentials.create(
          config.get(Constants.ACCESS_KEY), config.get(Constants.SECRET_KEY)));
      case EC2_METADATA_PROVIDER:
        return InstanceProfileCredentialsProvider.create();
      case NONE_PROVIDER:
        return AnonymousCredentialsProvider.create();

      case ASSUME_ROLE_PROVIDER:
        return new STSCredentialProviderV2(config);

      default:
        return DefaultCredentialsProvider.create();
    }
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
          final String targetEndpoint;
          Optional<String> endpoint = getEndpoint();

          if(isCompatMode() && endpoint.isPresent()) {
            // if this is compatibility mode and we have an endpoint, just use that.
            targetEndpoint = endpoint.get();
          } else {
            final String bucketRegion = s3.getBucketLocation(bucketName);
            final String fallbackEndpoint = endpoint.orElseGet(() -> String.format("%ss3.%s.amazonaws.com", getHttpScheme(), bucketRegion));

            String regionEndpoint = fallbackEndpoint;
            try {
              Region region = Region.fromValue(bucketRegion);
              com.amazonaws.regions.Region awsRegion = region.toAWSRegion();
              if (awsRegion != null) {
                regionEndpoint = awsRegion.getServiceEndpoint("s3");
              }
            } catch (IllegalArgumentException iae) {
              // try heuristic mapping if not found
              regionEndpoint = fallbackEndpoint;
              logger.warn("Unknown or unmapped region {} for bucket {}. Will use following endpoint: {}",
                bucketRegion, bucketName, regionEndpoint);
            }
            // it could be null because no mapping from Region to aws region or there is no such region is the map of endpoints
            // not sure if latter is possible
            if (regionEndpoint == null) {
              logger.error("Could not get AWSRegion for bucket {}. Will use following fs.s3a.endpoint: " + "{} ",
                bucketName, fallbackEndpoint);
            }
            targetEndpoint = (regionEndpoint != null) ? regionEndpoint : fallbackEndpoint;
          }

          String location = S3_URI_SCHEMA + bucketName + "/";
          final Configuration bucketConf = new Configuration(parentConf);
          bucketConf.set(ENDPOINT, targetEndpoint);
          return fsCache.get(new Path(location).toUri(), bucketConf, S3ClientKey.UNIQUE_PROPS);
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
        Constants.AWS_CREDENTIALS_PROVIDER,
        Constants.MAXIMUM_CONNECTIONS,
        Constants.MAX_ERROR_RETRIES,
        Constants.ESTABLISH_TIMEOUT,
        Constants.SOCKET_TIMEOUT,
        Constants.SOCKET_SEND_BUFFER,
        Constants.SOCKET_RECV_BUFFER,
        Constants.SIGNING_ALGORITHM,
        Constants.USER_AGENT_PREFIX,
        Constants.PROXY_HOST,
        Constants.PROXY_PORT,
        Constants.PROXY_DOMAIN,
        Constants.PROXY_USERNAME,
        Constants.PROXY_PASSWORD,
        Constants.PROXY_WORKSTATION,
        Constants.PATH_STYLE_ACCESS,
        S3FileSystem.COMPATIBILITY_MODE,
        S3FileSystem.REGION_OVERRIDE,
        Constants.ASSUMED_ROLE_ARN,
        Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER
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

  private <T extends AwsClientBuilder<?,?>> T configClientBuilder(T builder, String bucket) {
    final Configuration conf = getConf();
    builder.credentialsProvider(getAsync2Provider(conf));
    Optional<String> endpoint = getEndpoint();
    if (endpoint.isPresent()) {
      try {
        builder.endpointOverride(new URI(endpoint.get()));
      } catch (URISyntaxException use) {
        throw UserException.sourceInBadState(use).build(logger);
      }
    }

    if(!isCompatMode()) {
      // normal s3/govcloud mode.
      builder.region(getAWSBucketRegion(bucket));
    } else
    {
      builder.region(getAwsRegionFromConfigurationOrDefault(conf));
    }
    return builder;
  }

  static software.amazon.awssdk.regions.Region getAwsRegionFromConfigurationOrDefault(Configuration conf) {
    if (conf.get(REGION_OVERRIDE) != null) {
      // a region override is set.
      String regionOverride = conf.getTrimmed(REGION_OVERRIDE);
      if (!regionOverride.isEmpty()) {
        // set the region to what the user provided unless they provided an empty string.
        return software.amazon.awssdk.regions.Region.of(regionOverride);
      }
    }
    // default to the US_EAST_1 if no region is set.
    return software.amazon.awssdk.regions.Region.US_EAST_1;
  }

  private Optional<String> getEndpoint() {
    return Optional.ofNullable(getConf().getTrimmed(ENDPOINT))
        .map(s -> getHttpScheme() + s);
  }

  private String getHttpScheme() {
    return getConf().getBoolean(SECURE_CONNECTIONS, true) ? "https://" : "http://";
  }

  private boolean isCompatMode() {
    return getConf().getBoolean(COMPATIBILITY_MODE, false);
  }
}
