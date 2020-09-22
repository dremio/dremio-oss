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
import static org.apache.hadoop.fs.s3a.Constants.ALLOW_REQUESTER_PAYS;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.SECURE_CONNECTIONS;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.Region;
import com.dremio.aws.SharedInstanceProfileCredentialsProvider;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.Retryer;
import com.dremio.exec.hadoop.MayProvideAsyncStream;
import com.dremio.exec.store.dfs.DremioFileSystemCache;
import com.dremio.exec.store.dfs.FileSystemConf;
import com.dremio.io.AsyncByteReader;
import com.dremio.plugins.util.ContainerFileSystem;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.FinalizablePhantomReference;
import com.google.common.base.FinalizableReference;
import com.google.common.base.FinalizableReferenceQueue;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityRequest;

/**
 * FileSystem implementation that treats multiple s3 buckets as a unified namespace
 */
public class S3FileSystem extends ContainerFileSystem implements MayProvideAsyncStream {
  public static final String S3_PERMISSION_ERROR_MSG = "Access was denied by S3";
  static final String COMPATIBILITY_MODE = "dremio.s3.compat";
  static final String REGION_OVERRIDE = "dremio.s3.region";

  private static final Logger logger = LoggerFactory.getLogger(S3FileSystem.class);
  private static final String S3_URI_SCHEMA = "s3a://";
  private static final URI S3_URI = URI.create("s3a://aws"); // authority doesn't matter here, it is just to avoid exceptions
  private static final String S3_ENDPOINT_END = ".amazonaws.com";
  private static final String S3_CN_ENDPOINT_END = S3_ENDPOINT_END + ".cn";

  private final Retryer retryer = new Retryer.Builder()
    .retryIfExceptionOfType(SdkClientException.class)
    .retryIfExceptionOfType(software.amazon.awssdk.core.exception.SdkClientException.class)
    .setWaitStrategy(Retryer.WaitStrategy.EXPONENTIAL, 250, 2500)
    .setMaxRetries(10).build();

  // Used to close the S3 client objects
  // Lifecycle of an S3AsyncClient object:
  // - created by the LoadingCache on first access
  //   - inserted in the set of client references, below, upon creation
  // - accessed by readers, and potentially held longer than it takes for the LoadingCache to evict
  // - evicted from the LoadingCache some time after last access
  // - once both the LoadingCache and the reader finish using the s3 clients, the only outstanding reference
  //   is the one in the client reference set
  // - GC is allowed to reap such objects. Upon finalize, the S3 client is closed, and removed from the
  //   client reference set
  private static final FinalizableReferenceQueue FINALIZABLE_REFERENCE_QUEUE = new FinalizableReferenceQueue();
  private static final Set<FinalizableReference> REFERENCES = Sets.newConcurrentHashSet(); // To hold references until queued

  private final DremioFileSystemCache fsCache = new DremioFileSystemCache();

  private final LoadingCache<String, S3Client> syncClientCache = CacheBuilder
    .newBuilder()
    .expireAfterAccess(1, TimeUnit.HOURS)
    .build(new CacheLoader<String, S3Client>() {
      @Override
      public S3Client load(String bucket) {
        final S3Client syncClient = configClientBuilder(S3Client.builder(), bucket).build();
        return registerReference(S3Client.class, syncClient, client -> {
          try {
            syncClient.close();
          } catch (Exception e) {
            logger.warn("Failed to close the S3 sync client for bucket {}", e);
          }
        });
      }
    });

  private final LoadingCache<S3ClientKey, AmazonS3> clientCache = CacheBuilder
    .newBuilder()
    .expireAfterAccess(1,TimeUnit.HOURS)
    .maximumSize(20)
    .build(new CacheLoader<S3ClientKey, AmazonS3>() {
      @Override
      public AmazonS3 load(S3ClientKey clientKey) throws Exception {
        logger.debug("Opening S3 client connection for {}", clientKey);
        DefaultS3ClientFactory clientFactory = new DefaultS3ClientFactory();
        clientFactory.setConf(clientKey.s3Config);
        final AWSCredentialProviderList credentialsProvider = S3AUtils.createAWSCredentialProviderSet(S3_URI, clientKey.s3Config);
        final AmazonS3 s3Client = clientFactory.createS3Client(S3_URI, "", credentialsProvider);

        return registerReference(AmazonS3.class, s3Client, client -> {
          client.shutdown();

          try {
            // Note that AWS SDKv1 client will NOT close the credentials provider when being closed so it has to be done ourselves
            // Because client still holds a reference to credentials provider, it won't be garbage collected until the client is garbage collected itself
            if (credentialsProvider instanceof AutoCloseable) {
              ((AutoCloseable) credentialsProvider).close();
            }
          } catch (Exception e) {
              logger.warn("Failed to close AWS credentials provider", e);
          }
        });
      }
    });

  @VisibleForTesting
  protected AmazonS3 s3;

  private boolean useWhitelistedBuckets;

  /**
   * Register an object with a callback to clean up the object once no hard reference to the object
   * exists and get a proxy to the object to be used for the cache
   *
   * @param <T>
   * @param iface the object interface to be proxied
   * @param value
   * @param finalizer method invoked when the value is ready to be finalized
   * @return a facade to the object to be used instead of value
   */
  private static <T> T registerReference(Class<T> iface, T value, Consumer<T> finalizer) {
    assert iface.isInterface();

    @SuppressWarnings("unchecked")
    final T proxy = (T) Proxy.newProxyInstance(iface.getClassLoader(), new Class<?>[] { iface },
        (object, method, args) -> {
          try {
            return method.invoke(value, args);
          } catch (InvocationTargetException e) {
            throw e.getCause();
          }
        });

    // Proxy object is intended to be cached and used by code.
    // Once proxy is finalized, reference will be added to the queue and finalizeReference will be called, giving us a chance
    // to finalize properly value
    // Note that value cannot be garbaged collected since there's a strong reference between value and reference (captured by
    // the anonymous instance
    final FinalizablePhantomReference<T> reference = new FinalizablePhantomReference<T>(proxy, FINALIZABLE_REFERENCE_QUEUE) {
      @Override
      public void finalizeReferent() {
        // remove reference from reference list (references) to avoid it to grow constantly over time
        // it will also allow for value to be garbage collected after the method returns
        REFERENCES.remove(this);
        finalizer.accept(value);
      }
    };

    // Register the reference -  important to make sure that reference is not garbaged collected
    REFERENCES.add(reference);

    return proxy;
  }

  public S3FileSystem() {
    super(FileSystemConf.CloudFileSystemScheme.S3_FILE_SYSTEM_SCHEME.getScheme(), "bucket", ELIMINATE_PARENT_DIRECTORY);
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
      if (!NONE_PROVIDER.equals(conf.get(Constants.AWS_CREDENTIALS_PROVIDER))
        && !conf.getBoolean(COMPATIBILITY_MODE, false)) {
        verifyCredentials(conf);
      }
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause == null) {
        throw new RuntimeException(e);
      }

      Throwables.throwIfInstanceOf(cause, IOException.class);
      Throwables.throwIfUnchecked(cause);

      throw new RuntimeException(cause);
    }
  }

  /**
   * Checks if credentials are valid using GetCallerIdentity API call.
   */
  protected void verifyCredentials(Configuration conf) throws RuntimeException {
      AwsCredentialsProvider awsCredentialsProvider = getAsync2Provider(conf);
      final StsClientBuilder stsClientBuilder = StsClient.builder()
        // Note that AWS SDKv2 client will close the credentials provider if needed when the client is closed
        .credentialsProvider(awsCredentialsProvider)
        .region(getAWSRegionFromConfigurationOrDefault(conf));
      try (StsClient stsClient = stsClientBuilder.build()) {
        retryer.call(() -> {
          GetCallerIdentityRequest request = GetCallerIdentityRequest.builder().build();
          stsClient.getCallerIdentity(request);
          return true;
        });
      } catch (Retryer.OperationFailedAfterRetriesException e) {
        throw new RuntimeException("Credential Verification failed.", e);
      }
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive) throws FileNotFoundException, IOException {
    return super.listFiles(f, recursive);
  }

  @Override
  protected Stream<ContainerCreator> getContainerCreators() throws IOException {
    Stream<String> buckets = getBucketNamesFromConfigurationProperty(S3StoragePlugin.EXTERNAL_BUCKETS);
    if (!NONE_PROVIDER.equals(getConf().get(Constants.AWS_CREDENTIALS_PROVIDER))) {
      if (!useWhitelistedBuckets) {
        // if we have authentication to access S3, add in owner buckets.
        buckets = Stream.concat(buckets, s3.listBuckets().stream().map(Bucket::getName));
      } else {
        // Only add the buckets provided in the configuration.
        buckets = Stream.concat(buckets, getBucketNamesFromConfigurationProperty(S3StoragePlugin.WHITELISTED_BUCKETS));
      }
    }
    return buckets.distinct() // Remove duplicate bucket names.
        .map(input -> new BucketCreator(getConf(), input));
  }

  private Stream<String> getBucketNamesFromConfigurationProperty(String bucketConfigurationProperty) {
    String bucketList = getConf().get(bucketConfigurationProperty,"");
    return Arrays.stream(bucketList.split(","))
      .map(String::trim)
      .filter(input -> !Strings.isNullOrEmpty(input));
  }

  @Override
  protected ContainerHolder getUnknownContainer(String containerName) throws IOException {
    // Per docs, if invalid security credentials are used to execute
    // AmazonS3#doesBucketExist method, the client is not able to distinguish
    // between bucket permission errors and invalid credential errors, and the
    // method could return an incorrect result.

    // Coordinator node gets the new bucket information by overall refresh in the containerMap
    // This method is implemented only for the cases when executor is falling behind.
    boolean containerFound = false;
    try {
      // getBucketLocation ensures that given user account has permissions for the bucket.
      containerFound = s3.doesBucketExistV2(containerName) &&
        s3.getBucketAcl(containerName).getGrantsAsList().stream()
          .anyMatch(g -> g.getGrantee().getIdentifier().equals(s3.getS3AccountOwner().getId()));
    } catch (AmazonS3Exception e) {
      if (e.getMessage().contains("Access Denied")) {
        // Ignorable because user doesn't have permissions. We'll omit this case.
        logger.info("Ignoring \"" + containerName + "\" because of logged in AWS account doesn't have access rights on this bucket." + e.getMessage());
      }
      logger.error("Error while looking up for the unknown container " + containerName, e);
    }
    return containerFound ? new BucketCreator(getConf(), containerName).toContainerHolder() : null;
  }

  private software.amazon.awssdk.regions.Region getAWSBucketRegion(String bucketName) throws SdkClientException {
    final String awsRegionName = Region.fromValue(s3.getBucketLocation(bucketName)).toAWSRegion().getName();
    return software.amazon.awssdk.regions.Region.of(awsRegionName);
  }

  @Override
  public boolean supportsAsync() {
    return true;
  }

  @Override
  public AsyncByteReader getAsyncByteReader(Path path, String version) throws IOException {
    final String bucket = ContainerFileSystem.getContainerName(path);
    String pathStr = ContainerFileSystem.pathWithoutContainer(path).toString();
    // The AWS HTTP client re-encodes a leading slash resulting in invalid keys, so strip them.
    pathStr = (pathStr.startsWith("/")) ? pathStr.substring(1) : pathStr;
    //return new S3AsyncByteReader(getAsyncClient(bucket), bucket, pathStr);
    return new S3AsyncByteReaderUsingSyncClient(getSyncClient(bucket), bucket, pathStr, version, isRequesterPays());
  }

  private S3Client getSyncClient(String bucket) throws IOException {
    try {
      return syncClientCache.get(bucket);
    } catch (ExecutionException | SdkClientException e ) {
      final Throwable cause = e.getCause();
      final Throwable toChain;
      if (cause == null) {
        toChain = e;
      } else {
        Throwables.throwIfInstanceOf(cause, UserException.class);
        Throwables.throwIfInstanceOf(cause, IOException.class);

        toChain = cause;
      }

      throw new IOException(String.format("Unable to create a sync S3 client for bucket %s", bucket), toChain);
    }
  }

  @Override
  public void close() throws IOException {
    fsCache.closeAll(true);

    // invalidating cache of clients
    // all clients (including this.s3) will be closed just before being evicted by GC.
    invalidateCache(clientCache);
    invalidateCache(syncClientCache);

    super.close();
  }

  private static final void invalidateCache(Cache<?, ?> cache) {
    cache.invalidateAll();
    cache.cleanUp();
  }

  // AwsCredentialsProvider might also implement SdkAutoCloseable
  // Make sure to close if using directly (or let client close it for you).
  @VisibleForTesting
  protected AwsCredentialsProvider getAsync2Provider(Configuration config) {
    switch(config.get(Constants.AWS_CREDENTIALS_PROVIDER)) {
      case ACCESS_KEY_PROVIDER:
        return StaticCredentialsProvider.create(AwsBasicCredentials.create(
          config.get(Constants.ACCESS_KEY), config.get(Constants.SECRET_KEY)));
      case EC2_METADATA_PROVIDER:
        return new SharedInstanceProfileCredentialsProvider();
      case NONE_PROVIDER:
        return AnonymousCredentialsProvider.create();
      case ASSUME_ROLE_PROVIDER:
        return new STSCredentialProviderV2(config);
      default:
        throw new IllegalStateException(config.get(Constants.AWS_CREDENTIALS_PROVIDER));
    }
  }

  private class BucketCreator extends ContainerCreator {
    private final Configuration parentConf;
    private final String bucketName;

    BucketCreator(Configuration parentConf, String bucketName) {
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
          Optional<String> endpoint = getEndpoint(getConf());

          if (isCompatMode() && endpoint.isPresent()) {
            // if this is compatibility mode and we have an endpoint, just use that.
            targetEndpoint = endpoint.get();
          } else {
            try {
              final String bucketRegion = s3.getBucketLocation(bucketName);
              final String fallbackEndpoint = endpoint.orElseGet(() -> String.format("%ss3.%s.amazonaws.com", getHttpScheme(getConf()), bucketRegion));

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
                logger.error("Could not get AWSRegion for bucket {}. Will use following fs.s3a.endpoint: {} ",
                  bucketName, fallbackEndpoint);
              }
              targetEndpoint = (regionEndpoint != null) ? regionEndpoint : fallbackEndpoint;

            } catch (AmazonS3Exception aex) {
              if (aex.getStatusCode() == 403) {
                throw UserException.permissionError(aex)
                  .message(S3_PERMISSION_ERROR_MSG)
                  .build(logger);
              }
              throw aex;
            }
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
        Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER,
        Constants.ALLOW_REQUESTER_PAYS
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
        Object value = s3Config.get(prop);
        hash = 31 * hash + (value != null ? value.hashCode() : 0);
      }

      return hash;
    }

    @Override
    public String toString() {
      return "[ Access Key=" + s3Config.get(Constants.ACCESS_KEY) + ", Secret Key =*****, isSecure=" +
          s3Config.get(SECURE_CONNECTIONS) + " ]";
    }
  }

  private <T extends AwsClientBuilder<?,?>> T configClientBuilder(T builder, String bucket) {
    final Configuration conf = getConf();
    // Note that AWS SDKv2 client will close the credentials provider if needed when the client is closed
    builder.credentialsProvider(getAsync2Provider(conf));
    Optional<String> endpoint = getEndpoint(conf);

    endpoint.ifPresent(e -> {
        try {
          builder.endpointOverride(new URI(e));
        } catch (URISyntaxException use) {
          throw UserException.sourceInBadState(use).build(logger);
        }
    });

    if (!isCompatMode()) {
      // normal s3/govcloud mode.
      builder.region(getAWSBucketRegion(bucket));
    } else {
      builder.region(getAWSRegionFromConfigurationOrDefault(conf));
    }
    return builder;
  }

  static software.amazon.awssdk.regions.Region getAWSRegionFromConfigurationOrDefault(Configuration conf) {
    final String regionOverride = conf.getTrimmed(REGION_OVERRIDE);
    if (!Strings.isNullOrEmpty(regionOverride)) {
      // set the region to what the user provided unless they provided an empty string.
      return software.amazon.awssdk.regions.Region.of(regionOverride);
    }

    return getAwsRegionFromEndpoint(conf.get(Constants.ENDPOINT));
  }

  static software.amazon.awssdk.regions.Region getAwsRegionFromEndpoint(String endpoint) {
    // Determine if one of the known AWS regions is contained within the given endpoint, and return that region if so.
    return Optional.ofNullable(endpoint)
      .map(e -> e.toLowerCase(Locale.ROOT)) // lower-case the endpoint for easy detection
      .filter(e -> e.endsWith(S3_ENDPOINT_END) || e.endsWith(S3_CN_ENDPOINT_END)) // omit any semi-malformed endpoints
      .flatMap(e -> software.amazon.awssdk.regions.Region.regions()
        .stream()
        .filter(region -> e.contains(region.id()))
        .findFirst()) // map the endpoint to the region contained within it, if any
      .orElse(software.amazon.awssdk.regions.Region.US_EAST_1); // default to US_EAST_1 if no regions are found.
  }

  static Optional<String> getEndpoint(Configuration conf) {
    return Optional.ofNullable(conf.getTrimmed(Constants.ENDPOINT))
      .map(s -> getHttpScheme(conf) + s);
  }

  static Optional<String> getStsEndpoint(Configuration conf) {
    return Optional.ofNullable(conf.getTrimmed(Constants.ASSUMED_ROLE_STS_ENDPOINT))
      .map(s -> "https://" + s);
  }

  private static String getHttpScheme(Configuration conf) {
    return conf.getBoolean(SECURE_CONNECTIONS, true) ? "https://" : "http://";
  }

  private boolean isCompatMode() {
    return getConf().getBoolean(COMPATIBILITY_MODE, false);
  }

  private boolean isRequesterPays() {
    return getConf().getBoolean(ALLOW_REQUESTER_PAYS, false);
  }
}
