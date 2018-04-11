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

import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.directory.api.util.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.Region;
import com.dremio.plugins.util.ContainerFileSystem;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.FluentIterable;

/**
 * FileSystem implementation that treats multiple s3 buckets as a unified namespace
 */
public class S3FileSystem extends ContainerFileSystem {

  private static final Logger logger = LoggerFactory.getLogger(S3FileSystem.class);
  private static final String S3_URI_SCHEMA = "s3a://";

  private AmazonS3 s3;

  // TODO: why static?
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
              // Proxy settings (if configured)
              clientConf.setProxyHost(clientKey.s3Config.get(Constants.PROXY_HOST));
              if (clientKey.s3Config.get(Constants.PROXY_PORT) != null) {
                clientConf.setProxyPort(Integer.valueOf(clientKey.s3Config.get(Constants.PROXY_PORT)));
              }
              clientConf.setProxyDomain(clientKey.s3Config.get(Constants.PROXY_DOMAIN));
              clientConf.setProxyUsername(clientKey.s3Config.get(Constants.PROXY_USERNAME));
              clientConf.setProxyPassword(clientKey.s3Config.get(Constants.PROXY_PASSWORD));
              clientConf.setProxyWorkstation(clientKey.s3Config.get(Constants.PROXY_WORKSTATION));

              if (clientKey.accessKey == null){
                return new AmazonS3Client(new AnonymousAWSCredentialsProvider(), clientConf);
              } else {
                return new AmazonS3Client(new BasicAWSCredentials(clientKey.accessKey, clientKey.secretKey), clientConf);
              }
            }
          }); // Looks like there is no close/cleanup for AmazonS3Client


  public S3FileSystem() {
    super("dremioS3", "bucket", ELIMINATE_PARENT_DIRECTORY);
  }

  // Work around bug in s3a filesystem where the parent directory is included in list. Similar to HADOOP-12169
  private static final Predicate<CorrectableFileStatus> ELIMINATE_PARENT_DIRECTORY = new Predicate<CorrectableFileStatus>() {
    @Override
    public boolean apply(@Nullable CorrectableFileStatus input) {
      final FileStatus status = input.getStatus();

      if (!status.isDirectory()) {
        return true;
      }
      return !Path.getPathWithoutSchemeAndAuthority(input.getPathWithoutContainerName()).equals(Path.getPathWithoutSchemeAndAuthority(status.getPath()));
    }
  };

  @Override
  protected void setup(Configuration conf) throws IOException {
    try {
      s3 = clientCache.get(S3ClientKey.create(conf));
    } catch (ExecutionException e) {
      if(e.getCause() != null && e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
    }
  }

  @Override
  protected Iterable<ContainerCreator> getContainerCreators() throws IOException {

    String externalBucketList = getConf().get(S3PluginConfig.EXTERNAL_BUCKETS);
    FluentIterable<String> buckets = externalBucketList == null ? FluentIterable.of(new String[0]) :
      FluentIterable.of(externalBucketList.split(","))
      .transform(new Function<String, String>(){

        @Override
        public String apply(String input) {
          return input.trim();
        }})

      .filter(new Predicate<String>() {
          @Override
          public boolean apply(String input) {
            return !Strings.isEmpty(input.trim());
          }});


    if(getConf().get(Constants.ACCESS_KEY) != null){
      // if we have an access key, add in owner buckets.
      buckets = buckets.append(FluentIterable.from(s3.listBuckets())
          .transform(new Function<Bucket, String>(){
            @Override
            public String apply(Bucket input) {
              return input.getName();
            }}));
    }

    return FluentIterable.from(buckets.toSet()).transform(new Function<String, ContainerCreator>(){
      @Override
      public ContainerCreator apply(String input) {
        return new BucketCreator(getConf(), input);
      }});
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
    private final String accessKey;
    private final String secretKey;
    private final boolean isSecure;
    private final Configuration s3Config;

    public static S3ClientKey create(final Configuration fsConf) {
      return new S3ClientKey(
              fsConf.get(Constants.ACCESS_KEY),
              fsConf.get(Constants.SECRET_KEY),
              Boolean.valueOf(fsConf.get(Constants.SECURE_CONNECTIONS, "true" /*default is true*/)),
              fsConf);
    }

    private S3ClientKey(final String accessKey, final String secretKey, final boolean isSecure, Configuration s3Config) {
      this.accessKey = accessKey;
      this.secretKey = secretKey;
      this.isSecure = isSecure;
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
      return Objects.equals(accessKey, that.accessKey) &&
              Objects.equals(secretKey, that.secretKey) &&
              isSecure == that.isSecure &&
              s3Config.get(Constants.PROXY_HOST) == that.s3Config.get(Constants.PROXY_HOST) &&
              s3Config.get(Constants.PROXY_PORT) == that.s3Config.get(Constants.PROXY_PORT) &&
              s3Config.get(Constants.PROXY_DOMAIN) == that.s3Config.get(Constants.PROXY_DOMAIN) &&
              s3Config.get(Constants.PROXY_USERNAME) == that.s3Config.get(Constants.PROXY_USERNAME) &&
              s3Config.get(Constants.PROXY_PASSWORD) == that.s3Config.get(Constants.PROXY_PASSWORD) &&
              s3Config.get(Constants.PROXY_WORKSTATION) == that.s3Config.get(Constants.PROXY_WORKSTATION);
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


}
