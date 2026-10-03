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

import static com.dremio.common.utils.PathUtils.removeLeadingSlash;
import static com.dremio.exec.ExecConstants.ENABLE_STORE_PARQUET_ASYNC_TIMESTAMP_CHECK;
import static com.dremio.plugins.Constants.DREMIO_ENABLE_BUCKET_DISCOVERY;
import static com.dremio.plugins.s3.store.S3StoragePlugin.NONE_PROVIDER;
import static org.apache.hadoop.fs.s3a.Constants.ALLOW_REQUESTER_PAYS;
import static org.apache.hadoop.fs.s3a.Constants.AWS_REGION;
import static org.apache.hadoop.fs.s3a.Constants.CENTRAL_ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.SECURE_CONNECTIONS;
import static software.amazon.awssdk.http.HttpStatusCode.FORBIDDEN;
import static software.amazon.awssdk.http.HttpStatusCode.INTERNAL_SERVER_ERROR;
import static software.amazon.awssdk.http.HttpStatusCode.SERVICE_UNAVAILABLE;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.NamedThreadFactory;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.Retryer;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.hadoop.DremioHadoopUtils;
import com.dremio.exec.hadoop.MayProvideAsyncStream;
import com.dremio.exec.store.dfs.DremioFileSystemCache;
import com.dremio.exec.store.dfs.FileSystemConf;
import com.dremio.io.AsyncByteReader;
import com.dremio.io.FSOutputStream;
import com.dremio.plugins.s3.store.S3PluginMetrics.OperationType;
import com.dremio.plugins.util.AwsCredentialProviderUtils;
import com.dremio.plugins.util.CloseableRef;
import com.dremio.plugins.util.CloseableResource;
import com.dremio.plugins.util.ContainerAccessDeniedException;
import com.dremio.plugins.util.ContainerFileSystem;
import com.dremio.plugins.util.ContainerNotFoundException;
import com.dremio.plugins.util.S3PluginUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityRequest;

/** FileSystem implementation that treats multiple s3 buckets as a unified namespace */
public class S3FileSystem extends ContainerFileSystem implements MayProvideAsyncStream {
  public static final String S3_PERMISSION_ERROR_MSG = "Access was denied by S3";
  public static final String COMPATIBILITY_MODE = "dremio.s3.compat";
  static final String REGION_OVERRIDE = "dremio.s3.region";

  static final String ACCESS_DENIED_ERROR_CODE_V1 = "AccessDenied";
  static final String NO_SUCH_BUCKET_V1 = "NoSuchBucket";

  private static final Logger logger = LoggerFactory.getLogger(S3FileSystem.class);
  private static final String S3_URI_SCHEMA = "s3a://";
  private static final URI S3_URI =
      URI.create("s3a://aws"); // authority doesn't matter here, it is just to avoid exceptions
  private static final String S3_ENDPOINT_END = ".amazonaws.com";
  private static final String S3_CN_ENDPOINT_END = S3_ENDPOINT_END + ".cn";
  private static final ExecutorService threadPool =
      Executors.newCachedThreadPool(new NamedThreadFactory("s3-async-read-"));

  private static final List<String> UNIQUE_PROPS =
      ImmutableList.of(
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
          Constants.ASSUMED_ROLE_ARN,
          Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER,
          Constants.ALLOW_REQUESTER_PAYS,
          Constants.AWS_REGION,
          S3FileSystem.COMPATIBILITY_MODE,
          S3FileSystem.REGION_OVERRIDE);

  private final Retryer retryer =
      Retryer.newBuilder()
          .retryIfExceptionOfType(SdkClientException.class)
          .retryIfExceptionOfType(software.amazon.awssdk.core.exception.SdkClientException.class)
          .setWaitStrategy(Retryer.WaitStrategy.EXPONENTIAL, 250, 2500)
          .setMaxRetries(10)
          .build();

  @SuppressWarnings("NoGuavaCacheUsage") // TODO: fix as part of DX-51884
  private final LoadingCache<String, CloseableRef<S3AsyncClient>> asyncClientCache =
      CacheBuilder.newBuilder()
          .expireAfterAccess(1, TimeUnit.HOURS)
          .removalListener(
              notification ->
                  AutoCloseables.close(
                      RuntimeException.class, (AutoCloseable) notification.getValue()))
          .build(
              new CacheLoader<String, CloseableRef<S3AsyncClient>>() {
                @Override
                public CloseableRef<S3AsyncClient> load(String bucket) {
                  Optional<Region> region = getRegionForS3AsyncClient(bucket);
                  return s3ClientFactory.createS3AsyncClient(threadPool, region);
                }
              });

  /** Get (or create if one doesn't already exist) an async client for accessing a given bucket */
  private CloseableRef<S3AsyncClient> getAsyncClient(String bucket) throws IOException {
    try {
      return asyncClientCache.get(bucket);
    } catch (ExecutionException | SdkClientException e) {
      if (e.getCause() != null && e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new IOException(
          String.format("Unable to create an async S3 client for bucket %s", bucket), e);
    }
  }

  // Usage of fsS3Client should be done through getFsS3Client
  private CloseableResource<AmazonS3> fsS3ClientV1 = null;
  private Instant expirationFsS3ClientV1 = null;
  private CloseableRef<S3Client> fsS3Client = null;
  private Instant expirationFsS3Client = null;

  @VisibleForTesting S3ClientFactory s3ClientFactory = null;
  private S3ClientProperties s3ClientProperties = null;

  private final DremioFileSystemCache fsCache = new DremioFileSystemCache();
  private boolean useWhitelistedBuckets;

  public S3FileSystem() {
    super(
        FileSystemConf.CloudFileSystemScheme.S3_FILE_SYSTEM_SCHEME.getScheme(),
        "bucket",
        ELIMINATE_PARENT_DIRECTORY);
  }

  // Work around bug in s3a filesystem where the parent directory is included in list. Similar to
  // HADOOP-12169
  private static final Predicate<CorrectableFileStatus> ELIMINATE_PARENT_DIRECTORY =
      (input -> {
        final FileStatus status = input.getStatus();
        if (!status.isDirectory()) {
          return true;
        }
        return !Path.getPathWithoutSchemeAndAuthority(input.getPathWithoutContainerName())
            .equals(Path.getPathWithoutSchemeAndAuthority(status.getPath()));
      });

  public static CloseableResource<AmazonS3> createS3V1Client(Configuration s3Config)
      throws IOException {
    org.apache.hadoop.fs.s3a.DefaultS3ClientFactory clientFactory =
        new org.apache.hadoop.fs.s3a.DefaultS3ClientFactory();
    clientFactory.setConf(s3Config);
    final AWSCredentialProviderList credentialsProvider =
        S3AUtils.createAWSCredentialProviderSet(S3_URI, s3Config);
    // Use builder pattern for S3Client(AWS SDK1.x) initialization.
    org.apache.hadoop.fs.s3a.S3ClientFactory.S3ClientCreationParameters parameters =
        new org.apache.hadoop.fs.s3a.S3ClientFactory.S3ClientCreationParameters()
            .withCredentialSet(credentialsProvider)
            .withPathStyleAccess(usePathStyleAccess(s3Config))
            .withEndpoint(buildEndpoint(s3Config));
    final AmazonS3 s3Client = clientFactory.createS3Client(S3_URI, parameters);

    final AutoCloseable closeableCredProvider =
        (credentialsProvider instanceof AutoCloseable) ? credentialsProvider : () -> {};
    final Consumer<AmazonS3> closeFunc =
        s3 ->
            AutoCloseables.close(
                RuntimeException.class, () -> s3.shutdown(), closeableCredProvider);
    final CloseableResource<AmazonS3> closeableS3 = new CloseableResource<>(s3Client, closeFunc);
    return closeableS3;
  }

  private static String buildEndpoint(Configuration s3Config) {
    String endpointOverride = s3Config.get(ENDPOINT);
    if (StringUtils.isNotEmpty(endpointOverride)) {
      return endpointOverride;
    }

    String regionStr = s3Config.getTrimmed(REGION_OVERRIDE);
    if (StringUtils.isNotEmpty(regionStr)) {
      return buildRegionEndpoint(regionStr);
    }

    return CENTRAL_ENDPOINT;
  }

  @Override
  protected void setup(Configuration conf) throws IOException {
    logger.info("Setting up S3FileSystem:{}", System.identityHashCode(this));

    useWhitelistedBuckets = !conf.get(S3StoragePlugin.WHITELISTED_BUCKETS, "").isEmpty();
    if (!NONE_PROVIDER.equals(conf.get(Constants.AWS_CREDENTIALS_PROVIDER))
        && !isCompatMode(conf)) {
      verifyCredentials(conf);
    }

    configureS3ClientFactory(conf);
  }

  /** Checks if credentials are valid using GetCallerIdentity API call. */
  protected void verifyCredentials(Configuration conf) {
    final AwsCredentialsProvider awsCredentialsProvider = getAsync2Provider(conf);

    final StsClientBuilder stsClientBuilder =
        StsClient.builder()
            // Note that AWS SDKv2 client will close the credentials provider if needed when the
            // client is closed
            .credentialsProvider(awsCredentialsProvider)
            .region(getAWSRegionFromConfigurationOrDefault(conf));
    try (StsClient stsClient = stsClientBuilder.build()) {
      retryer.call(
          () -> {
            GetCallerIdentityRequest request = GetCallerIdentityRequest.builder().build();
            stsClient.getCallerIdentity(request);
            return true;
          });
    } catch (Retryer.OperationFailedAfterRetriesException e) {
      throw new RuntimeException("Credential Verification failed.", e);
    }
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive)
      throws FileNotFoundException, IOException {
    return super.listFiles(f, recursive);
  }

  Stream<String> listBucketsV1() throws IOException {
    logger.info("Using AWS SDK V1 in S3FileSystem - listBucketsV1");
    Stream<String> buckets;

    try (CloseableResource<AmazonS3> s3Ref = getS3V1Client()) {
      final AmazonS3 s3 = s3Ref.getResource();
      buckets = s3.listBuckets().stream().map(com.amazonaws.services.s3.model.Bucket::getName);
    } catch (AmazonS3Exception e) {
      if (e.getStatusCode() == 503 || e.getStatusCode() == 500) {
        S3PluginMetrics.incrementS3ClientThrottlingErrorCounter(OperationType.LIST_BUCKETS);
      }
      S3PluginMetrics.incrementS3V1ClientThrewCounter();
      logger.error("Error while listing S3 buckets", e);
      throw new IOException(e);
    } catch (Exception e) {
      S3PluginMetrics.incrementS3V1ClientThrewCounter();
      logger.error("Error while listing S3 buckets", e);
      throw new IOException(e);
    }

    return buckets;
  }

  Stream<String> listBucketsV2() throws IOException {
    Stream<String> buckets;

    try (CloseableRef<S3Client> s3Ref = getFsS3Client()) {
      final S3Client s3 = s3Ref.acquireRef();
      buckets = s3.listBuckets().buckets().stream().map(Bucket::name);
    } catch (S3Exception e) {
      if (e.statusCode() == 503 || e.statusCode() == 500) {
        S3PluginMetrics.incrementS3ClientThrottlingErrorCounter(OperationType.LIST_BUCKETS);
      }
      S3PluginMetrics.incrementS3V1ClientThrewCounter();
      logger.error("Error while listing S3 buckets", e);
      throw new IOException(e);
    } catch (Exception e) {
      S3PluginMetrics.incrementS3V2ClientThrewCounter();
      logger.error("Error while listing S3 buckets", e);
      throw new IOException(e);
    }

    return buckets;
  }

  @Override
  protected Stream<ContainerCreator> getContainerCreators() throws IOException {
    Stream<String> buckets =
        getBucketNamesFromConfigurationProperty(S3StoragePlugin.EXTERNAL_BUCKETS);

    if (!NONE_PROVIDER.equals(getConf().get(Constants.AWS_CREDENTIALS_PROVIDER))
        && useBucketDiscovery(getConf())) {
      if (!useWhitelistedBuckets) {
        // if we have authentication to access S3, add in owner buckets.
        buckets =
            Stream.concat(buckets, useFsS3V2Client(getConf()) ? listBucketsV2() : listBucketsV1());
      } else {
        // Only add the buckets provided in the configuration.
        buckets =
            Stream.concat(
                buckets,
                getBucketNamesFromConfigurationProperty(S3StoragePlugin.WHITELISTED_BUCKETS));
      }
    }
    return buckets
        .distinct() // Remove duplicate bucket names.S3FileSystem.java
        .map(input -> new BucketCreator(getConf(), input));
  }

  private Stream<String> getBucketNamesFromConfigurationProperty(
      String bucketConfigurationProperty) {
    String bucketList = getConf().get(bucketConfigurationProperty, "");
    return Arrays.stream(bucketList.split(","))
        .map(String::trim)
        .filter(input -> !Strings.isNullOrEmpty(input));
  }

  void tryFindContainerV1(String containerName) throws IOException {
    logger.info("Using AWS SDK V1 in S3FileSystem - tryFindContainerV1");
    boolean containerFound = false;
    try (CloseableResource<AmazonS3> s3Ref = getS3V1Client()) {
      final AmazonS3 s3 = s3Ref.getResource();
      if (s3.doesBucketExistV2(containerName)) {
        // Listing one object to ascertain read permissions on the bucket.
        final ListObjectsV2Request req =
            new ListObjectsV2Request()
                .withMaxKeys(1)
                .withRequesterPays(isRequesterPays())
                .withEncodingType("url")
                .withBucketName(containerName);
        containerFound =
            s3.listObjectsV2(req)
                .getBucketName()
                .equals(containerName); // Exception if this account doesn't have access.
      }
    } catch (AmazonS3Exception e) {
      switch (e.getErrorCode()) {
        case NO_SUCH_BUCKET_V1:
          throw new ContainerNotFoundException(
              String.format("Cannot find bucket: %s does not exist", containerName));
        case ACCESS_DENIED_ERROR_CODE_V1:
          throw new ContainerAccessDeniedException(
              String.format("Cannot find bucket: Access denied to %s", containerName));
        default:
          break;
      }

      if (e.getStatusCode() == 503 || e.getStatusCode() == 500) {
        S3PluginMetrics.incrementS3ClientThrottlingErrorCounter(OperationType.GET_BUCKET);
      }

      S3PluginMetrics.incrementS3V1ClientThrewCounter();
      logger.error(String.format("S3 error while trying to find bucket (V1) %s", containerName), e);
    } catch (Exception e) {
      S3PluginMetrics.incrementS3V1ClientThrewCounter();
      logger.error(String.format("Error while trying to find bucket (V1) %s", containerName), e);
    }

    logger.debug("Unknown container (V1) '{}' found ? {}", containerName, containerFound);
    if (!containerFound) {
      throw new ContainerNotFoundException(
          String.format("Error while trying to find bucket: %s", containerName));
    }
  }

  void tryFindContainerV2(String containerName) throws IOException {
    boolean containerFound = false;
    try (CloseableRef<S3Client> s3Ref = getFsS3Client()) {
      final S3Client s3 = s3Ref.acquireRef();

      HeadBucketResponse response =
          s3.headBucket(HeadBucketRequest.builder().bucket(containerName).build());

      containerFound = true;
    } catch (NoSuchBucketException e) {
      throw new ContainerNotFoundException(
          String.format("Cannot find bucket: %s does not exist", containerName));
    } catch (S3Exception e) {
      switch (e.statusCode()) {
        case FORBIDDEN:
          throw new ContainerAccessDeniedException(
              String.format("Cannot find bucket: Access denied to %s", containerName));
        case INTERNAL_SERVER_ERROR:
        case SERVICE_UNAVAILABLE:
          S3PluginMetrics.incrementS3ClientThrottlingErrorCounter(OperationType.GET_BUCKET);
          break;
        default:
          break;
      }

      logger.error(String.format("S3 error while trying to find bucket %s", containerName), e);
      S3PluginMetrics.incrementS3V2ClientThrewCounter();
    } catch (Exception e) {
      logger.error(String.format("Error while trying to find bucket %s", containerName), e);
      S3PluginMetrics.incrementS3V2ClientThrewCounter();
    }

    logger.debug("Unknown container '{}' found ? {}", containerName, containerFound);
    if (!containerFound) {
      throw new ContainerNotFoundException(
          String.format("Error while trying to find bucket: %s", containerName));
    }
  }

  @Override
  protected ContainerHolder getUnknownContainer(String containerName) throws IOException {
    // Coordinator node gets the new bucket information by overall refresh in the containerMap
    // This method is implemented only for the cases when executor is falling behind.

    // In case useBucketDiscovery is turned off, we avoid doing is check as we can't expect
    // to have permissions to carry out listings in or existence checks of buckets.
    if (useBucketDiscovery(getConf())) {
      if (useFsS3V2Client(getConf())) {
        tryFindContainerV2(containerName);
      } else {
        tryFindContainerV1(containerName);
      }
    }
    return new BucketCreator(getConf(), containerName).toContainerHolder();
  }

  private Region getAWSBucketRegionV1(String bucketName) throws SdkClientException {
    logger.info("Using AWS SDK V1 in S3FileSystem - getAWSBucketRegionV1");
    try (CloseableResource<AmazonS3> s3Ref = getS3V1Client()) {
      final AmazonS3 s3 = s3Ref.getResource();
      final String awsRegionName =
          com.amazonaws.services.s3.model.Region.fromValue(s3.getBucketLocation(bucketName))
              .toAWSRegion()
              .getName();
      return Region.of(awsRegionName);
    } catch (AmazonS3Exception e) {
      switch (e.getErrorCode()) {
        case NO_SUCH_BUCKET_V1:
          throw UserException.validationError()
              .message(String.format("Cannot get bucket region: %s does not exist", bucketName))
              .buildSilently();
        case ACCESS_DENIED_ERROR_CODE_V1:
          throw UserException.permissionError()
              .message(String.format("Cannot get bucket region: Access denied to %s", bucketName))
              .buildSilently();
        default:
          break;
      }

      if (e.getStatusCode() == 503 || e.getStatusCode() == 500) {
        S3PluginMetrics.incrementS3ClientThrottlingErrorCounter(OperationType.GET_BUCKET_REGION);
      }

      S3PluginMetrics.incrementS3V1ClientThrewCounter();
      throw new RuntimeException(
          String.format("S3 error while trying to get region of %s: %s", bucketName, e));
    } catch (Exception e) {
      S3PluginMetrics.incrementS3V1ClientThrewCounter();
      throw new RuntimeException(
          String.format("Error while trying to get region of %s: %s", bucketName, e));
    }
  }

  private Region getAWSBucketRegionV2(String bucketName) {
    try (CloseableRef<S3Client> s3Ref = getFsS3Client()) {
      final S3Client s3 = s3Ref.acquireRef();
      final String bucketRegion =
          s3.headBucket(HeadBucketRequest.builder().bucket(bucketName).build()).bucketRegion();

      return Region.of(bucketRegion);
    } catch (NoSuchBucketException e) {
      String msg = String.format("Cannot get bucket region: %s does not exist", bucketName);
      throw UserException.validationError().message(msg).build(logger);
    } catch (S3Exception e) {

      switch (e.statusCode()) {
        case FORBIDDEN:
          throw UserException.permissionError()
              .message(String.format("Cannot get bucket region: Access denied to %s", bucketName))
              .buildSilently();
        case INTERNAL_SERVER_ERROR:
        case SERVICE_UNAVAILABLE:
          S3PluginMetrics.incrementS3ClientThrottlingErrorCounter(OperationType.GET_BUCKET_REGION);
          break;
        default:
          break;
      }

      S3PluginMetrics.incrementS3V2ClientThrewCounter();
      throw new RuntimeException(
          String.format("S3 error while trying to get region of %s: %s", bucketName, e));
    } catch (Exception e) {
      S3PluginMetrics.incrementS3V2ClientThrewCounter();
      throw new RuntimeException(
          String.format("Error while trying to get region of %s: %s", bucketName, e));
    }
  }

  @Override
  public boolean supportsAsync() {
    return true;
  }

  @Override
  public AsyncByteReader getAsyncByteReader(Path path, String version, Map<String, String> options)
      throws IOException {
    final String bucket = DremioHadoopUtils.getContainerName(path);
    String pathStr = DremioHadoopUtils.pathWithoutContainer(path).toString();
    // The AWS HTTP client re-encodes a leading slash resulting in invalid keys, so strip them.
    pathStr = (pathStr.startsWith("/")) ? pathStr.substring(1) : pathStr;
    boolean ssecUsed = isSsecUsed();
    String sseCustomerKey = getCustomerSSEKey(ssecUsed);
    return new S3AsyncByteReader(
        getAsyncClient(bucket),
        bucket,
        pathStr,
        version,
        ssecUsed,
        sseCustomerKey,
        "true".equals(options.get(ENABLE_STORE_PARQUET_ASYNC_TIMESTAMP_CHECK.getOptionName())));
  }

  @Override
  public long getTTL(com.dremio.io.file.FileSystem fileSystem, com.dremio.io.file.Path path) {
    Path hadoopPath = new org.apache.hadoop.fs.Path(path.toString());
    final String bucket = DremioHadoopUtils.getContainerName(hadoopPath);
    final String onlyPath =
        removeLeadingSlash(DremioHadoopUtils.pathWithoutContainer(hadoopPath).toString());
    HeadObjectRequest reqHeadObject =
        HeadObjectRequest.builder().bucket(bucket).key(onlyPath).build();

    final HeadObjectResponse[] respHeadObject = new HeadObjectResponse[1];
    try (FSOutputStream fos = fileSystem.create(fileSystem.canonicalizePath(path), true);
        CloseableRef<S3AsyncClient> asyncClient = getAsyncClient(bucket)) {
      fos.close();

      retryer.call(
          () -> {
            respHeadObject[0] =
                asyncClient
                    .acquireRef()
                    .headObject(reqHeadObject)
                    .get(getRequestTimeoutInMillis(getConf()), TimeUnit.MILLISECONDS);
            return true;
          });
      fileSystem.delete(path, false);
    } catch (Exception ex) {
      logger.info("Failed to get head object for {}", path, ex);
      return -1;
    }

    if (respHeadObject[0] == null) {
      logger.info("Unable to retrieve head object for {}", path.getParent());
      return -1;
    }

    if (respHeadObject[0].expiration() == null) {
      logger.info("No expiration lifecycle rules set for {}", path.getParent());
      return -1;
    }

    String[] parts = respHeadObject[0].expiration().split("\"");
    if (parts.length != 4) {
      logger.error("Unexpected expiration metadata:" + respHeadObject[0].expiration());
      return -1;
    }

    logger.info("TTL based on{}{}", parts[2], parts[3]);
    Instant expireInstant = Instant.from(DateTimeFormatter.RFC_1123_DATE_TIME.parse(parts[1]));

    return Duration.between(respHeadObject[0].lastModified(), expireInstant).toDays();
  }

  @Override
  public void close() throws IOException {
    logger.info("Closing S3FileSystem:{}", System.identityHashCode(this));

    // invalidating cache of clients
    // all clients (including this.s3) will be closed just before being evicted by GC.
    AutoCloseables.close(
        IOException.class,
        super::close,
        fsCache::closeAll,
        fsS3ClientV1,
        fsS3Client,
        () -> invalidateCache(asyncClientCache));
  }

  private static void invalidateCache(Cache<?, ?> cache) {
    cache.invalidateAll();
    cache.cleanUp();
  }

  // AwsCredentialsProvider might also implement SdkAutoCloseable
  // Make sure to close if using directly (or let client close it for you).
  @VisibleForTesting
  protected AwsCredentialsProvider getAsync2Provider(Configuration config) {
    return AwsCredentialProviderUtils.getCredentialsProvider(config);
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

      return new ContainerHolder(
          bucketName,
          new FileSystemSupplier() {
            @Override
            public FileSystem create() throws IOException {
              final String targetEndpoint;
              Optional<String> endpoint = getEndpoint(getConf());

              if (endpoint.isPresent()
                  && (isCompatMode(getConf()) || getConf().get(AWS_REGION) != null)) {
                // if this is compatibility mode and we have an endpoint, just use that.
                targetEndpoint = endpoint.get();
              } else if (!useBucketDiscovery(getConf())) {
                targetEndpoint = null;
              } else {
                final String bucketRegion =
                    useFsS3V2Client(getConf())
                        ? getAWSBucketRegionV2(bucketName).toString()
                        : getAWSBucketRegionV1(bucketName).toString();

                final String fallbackEndpoint =
                    endpoint.orElseGet(
                        () ->
                            String.format(
                                "%ss3.%s.amazonaws.com", getHttpScheme(getConf()), bucketRegion));

                String regionEndpoint;
                try {
                  regionEndpoint = buildRegionEndpoint(bucketRegion);
                } catch (IllegalArgumentException iae) {
                  // try heuristic mapping if not found
                  regionEndpoint = fallbackEndpoint;
                  logger.warn(
                      "Unknown or unmapped region {} for bucket {}. Will use following endpoint: {}",
                      bucketRegion,
                      bucketName,
                      regionEndpoint);
                }
                // it could be null because no mapping from Region to aws region or there is no
                // such region is the map of endpoints
                // not sure if latter is possible
                if (regionEndpoint == null) {
                  logger.error(
                      "Could not get AWSRegion for bucket {}. Will use following fs.s3a.endpoint: {} ",
                      bucketName,
                      fallbackEndpoint);
                }
                targetEndpoint = (regionEndpoint != null) ? regionEndpoint : fallbackEndpoint;
              }

              String location = S3_URI_SCHEMA + bucketName + "/";
              final Configuration bucketConf = new Configuration(parentConf);
              if (targetEndpoint != null) {
                bucketConf.set(ENDPOINT, targetEndpoint);
              }
              return fsCache.get(new Path(location).toUri(), bucketConf, UNIQUE_PROPS);
            }
          });
    }
  }

  static String buildRegionEndpoint(String regionString) {
    S3PluginUtils.checkIfValidAwsRegion(regionString);

    if (regionString.equals(Region.US_EAST_1.id())) {
      return CENTRAL_ENDPOINT;
    }

    return String.format("s3.%s.amazonaws.com", regionString);
  }

  private Optional<Region> getRegionForS3AsyncClient(String bucket) {
    final Configuration conf = getConf();
    if (!isCompatMode(conf) && useBucketDiscovery(conf)) {
      // normal s3/govcloud mode.
      return useFsS3V2Client(conf)
          ? Optional.of(getAWSBucketRegionV2(bucket))
          : Optional.of(getAWSBucketRegionV1(bucket));
    }

    return Optional.empty();
  }

  static Region getAWSRegionFromConfigurationOrDefault(Configuration conf) {
    final String regionOverride = conf.getTrimmed(REGION_OVERRIDE);
    if (!Strings.isNullOrEmpty(regionOverride)) {
      // set the region to what the user provided unless they provided an empty string.
      S3PluginUtils.checkIfValidAwsRegion(regionOverride);
      return Region.of(regionOverride);
    }

    return getAwsRegionFromEndpoint(conf.get(Constants.ENDPOINT));
  }

  static Region getAwsRegionFromEndpoint(String endpoint) {
    // Determine if one of the known AWS regions is contained within the given endpoint, and return
    // that region if so.
    return Optional.ofNullable(endpoint)
        .map(e -> e.toLowerCase(Locale.ROOT)) // lower-case the endpoint for easy detection
        .filter(
            e ->
                e.endsWith(S3_ENDPOINT_END)
                    || e.endsWith(S3_CN_ENDPOINT_END)) // omit any semi-malformed endpoints
        .flatMap(
            e ->
                Region.regions().stream()
                    .filter(region -> e.contains(region.id()))
                    .findFirst()) // map the endpoint to the region contained within it, if any
        .orElse(Region.US_EAST_1); // default to US_EAST_1 if no regions are found.
  }

  /**
   * The endpoint with a scheme. An endpoint given without one ({@code host:port}) gets the scheme
   * of {@code fs.s3a.connection.ssl.enabled}; one given with a scheme is kept as is, as {@code
   * S3ClientProperties.createUriFromEndpoint} does for the SDK v2 clients.
   */
  static Optional<String> getEndpoint(Configuration conf) {
    return Optional.ofNullable(conf.getTrimmed(Constants.ENDPOINT))
        .map(s -> s.contains("://") ? s : getHttpScheme(conf) + s);
  }

  static Optional<String> getStsEndpoint(Configuration conf) {
    return Optional.ofNullable(conf.getTrimmed(Constants.ASSUMED_ROLE_STS_ENDPOINT))
        .map(
            s -> {
              if (s.startsWith("https://")) {
                return s;
              }

              return "https://" + s;
            });
  }

  private static String getHttpScheme(Configuration conf) {
    return conf.getBoolean(SECURE_CONNECTIONS, true) ? "https://" : "http://";
  }

  private boolean isCompatMode(Configuration conf) {
    return conf.getBoolean(COMPATIBILITY_MODE, false);
  }

  @VisibleForTesting
  static boolean usePathStyleAccess(Configuration conf) {
    return conf.getBoolean(Constants.PATH_STYLE_ACCESS, false);
  }

  private static boolean useBucketDiscovery(Configuration conf) {
    return conf.getBoolean(DREMIO_ENABLE_BUCKET_DISCOVERY, true);
  }

  private static boolean useFsS3V2Client(Configuration conf) {
    return conf.getBoolean(ExecConstants.ENABLE_S3_V2_CLIENT.getOptionName(), true);
  }

  private static long getRequestTimeoutInMillis(Configuration conf) {
    return conf.getTimeDuration(
        Constants.REQUEST_TIMEOUT,
        S3ClientProperties.API_CALL_TIMEOUT_IN_MILLIS_DEFAULT,
        TimeUnit.MILLISECONDS);
  }

  private boolean isSsecUsed() {
    String ssecAlgorithm = getConf().get("fs.s3a.server-side-encryption-algorithm", "");
    return "sse-c".equalsIgnoreCase(ssecAlgorithm);
  }

  private String getCustomerSSEKey(boolean ssecUsed) {
    if (!ssecUsed) {
      return "";
    }
    return getConf().get("fs.s3a.server-side-encryption.key", "");
  }

  @VisibleForTesting
  protected boolean isRequesterPays() {
    return getConf().getBoolean(ALLOW_REQUESTER_PAYS, false);
  }

  @VisibleForTesting
  Instant getInstantNowNonStatic() {
    return Instant.now();
  }

  @VisibleForTesting
  CloseableResource<AmazonS3> createS3V1ClientNonStatic() throws Exception {
    return createS3V1Client(getConf());
  }

  @VisibleForTesting
  synchronized CloseableResource<AmazonS3> getS3V1Client() throws Exception {
    logger.info("Getting an S3V1Client in S3FileSystem:{}", System.identityHashCode(this));
    S3PluginMetrics.incrementGetS3V1ClientCallsCounter();

    final Instant instantNow = getInstantNowNonStatic();
    final Instant newExpiration = instantNow.plus(1, ChronoUnit.HOURS);

    if (fsS3ClientV1 == null && expirationFsS3ClientV1 == null) {
      fsS3ClientV1 = createS3V1ClientNonStatic();
      expirationFsS3ClientV1 = newExpiration;
      return fsS3ClientV1;
    }

    if (instantNow.isAfter(expirationFsS3ClientV1)) {
      fsS3ClientV1.close();
      fsS3ClientV1 = createS3V1ClientNonStatic();
    }

    expirationFsS3ClientV1 = newExpiration;
    return fsS3ClientV1;
  }

  @VisibleForTesting
  synchronized CloseableRef<S3Client> getFsS3Client() throws Exception {
    logger.info("Getting an S3V2Client in S3FileSystem:{}", System.identityHashCode(this));
    S3PluginMetrics.incrementGetS3V2ClientCallsCounter();

    final Instant instantNow = getInstantNowNonStatic();
    final Instant newExpiration = instantNow.plus(1, ChronoUnit.HOURS);

    if (fsS3Client == null && expirationFsS3Client == null) {
      fsS3Client = s3ClientFactory.createS3Client();
      expirationFsS3Client = newExpiration;
      return fsS3Client;
    }

    if (instantNow.isAfter(expirationFsS3Client)) {
      fsS3Client.close();
      fsS3Client = s3ClientFactory.createS3Client();
    }

    expirationFsS3Client = newExpiration;
    return fsS3Client;
  }

  @VisibleForTesting
  void configureS3ClientFactory(Configuration config) {
    boolean isStateValid =
        s3ClientProperties == null
            && s3ClientFactory == null
            && fsS3Client == null
            && asyncClientCache.size() == 0;
    Preconditions.checkArgument(
        isStateValid,
        "The S3ClientFactory should only be configured once. "
            + "(s3ClientProperties == null) -> %s, (s3ClientFactory == null) -> %s, "
            + "(fsS3Client == null) -> %s, (asyncClientCache.size()) -> %s",
        s3ClientProperties == null,
        s3ClientFactory == null,
        fsS3Client == null,
        asyncClientCache.size());

    s3ClientProperties = new S3ClientProperties(config);
    s3ClientFactory = new S3ClientFactory(s3ClientProperties);
  }
}
