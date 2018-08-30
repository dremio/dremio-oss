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
package com.dremio.plugins.s3.store.copy;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.dremio.plugins.s3.store.S3FileSystem;
import com.google.common.base.Preconditions;

/**
 *
 * (Copied from Hadoop 2.8.3, move to using Hadoop version once hadoop version in MapR profile is upgraded to 2.8.3+)
 *
 * Factory for creation of S3 client instances to be used by {@link S3FileSystem}.
 */
public interface S3ClientFactory {

  /**
   * Creates a new {@link AmazonS3} client.  This method accepts the S3A file
   * system URI both in raw input form and validated form as separate arguments,
   * because both values may be useful in logging.
   *
   * @param name raw input S3A file system URI
   * @return S3 client
   * @throws IOException IO problem
   */
  AmazonS3 createS3Client(URI name) throws IOException;

  /**
   * The default factory implementation, which calls the AWS SDK to configure
   * and create an {@link AmazonS3Client} that communicates with the S3 service.
   */
  class DefaultS3ClientFactory extends Configured implements S3ClientFactory {

    private static final Logger logger = LoggerFactory.getLogger(S3FileSystem.class);

    @Override
    public AmazonS3 createS3Client(URI name) throws IOException {
      Configuration conf = getConf();
      AWSCredentialsProvider credentials =
          S3AUtils.createAWSCredentialProviderSet(name, conf);
      ClientConfiguration awsConf = new ClientConfiguration();
      initConnectionSettings(conf, awsConf);
      initProxySupport(conf, awsConf);
      initUserAgent(conf, awsConf);
      return createAmazonS3Client(conf, credentials, awsConf);
    }

    /**
     * Initializes all AWS SDK settings related to connection management.
     *
     * @param conf Hadoop configuration
     * @param awsConf AWS SDK configuration
     */
    private static void initConnectionSettings(Configuration conf,
        ClientConfiguration awsConf) {
      awsConf.setMaxConnections(intOption(conf, Constants.MAXIMUM_CONNECTIONS,
          Constants.DEFAULT_MAXIMUM_CONNECTIONS, 1));
      boolean secureConnections = conf.getBoolean(Constants.SECURE_CONNECTIONS,
          Constants.DEFAULT_SECURE_CONNECTIONS);
      awsConf.setProtocol(secureConnections ?  Protocol.HTTPS : Protocol.HTTP);
      awsConf.setMaxErrorRetry(intOption(conf, Constants.MAX_ERROR_RETRIES,
          Constants.DEFAULT_MAX_ERROR_RETRIES, 0));
      awsConf.setConnectionTimeout(intOption(conf, Constants.ESTABLISH_TIMEOUT,
          Constants.DEFAULT_ESTABLISH_TIMEOUT, 0));
      awsConf.setSocketTimeout(intOption(conf, Constants.SOCKET_TIMEOUT,
          Constants.DEFAULT_SOCKET_TIMEOUT, 0));
      int sockSendBuffer = intOption(conf, S3Constants.SOCKET_SEND_BUFFER,
          S3Constants.DEFAULT_SOCKET_SEND_BUFFER, 2048);
      int sockRecvBuffer = intOption(conf, S3Constants.SOCKET_RECV_BUFFER,
          S3Constants.DEFAULT_SOCKET_RECV_BUFFER, 2048);
      awsConf.setSocketBufferSizeHints(sockSendBuffer, sockRecvBuffer);
      String signerOverride = conf.getTrimmed(S3Constants.SIGNING_ALGORITHM, "");
      if (!signerOverride.isEmpty()) {
        logger.debug("Signer override = {}", signerOverride);
        awsConf.setSignerOverride(signerOverride);
      }
    }

    /**
     * Initializes AWS SDK proxy support if configured.
     *
     * @param conf Hadoop configuration
     * @param awsConf AWS SDK configuration
     * @throws IllegalArgumentException if misconfigured
     */
    private static void initProxySupport(Configuration conf,
        ClientConfiguration awsConf)
        throws IllegalArgumentException, IOException {
      String proxyHost = conf.getTrimmed(Constants.PROXY_HOST, "");
      int proxyPort = conf.getInt(Constants.PROXY_PORT, -1);
      if (!proxyHost.isEmpty()) {
        awsConf.setProxyHost(proxyHost);
        if (proxyPort >= 0) {
          awsConf.setProxyPort(proxyPort);
        } else {
          if (conf.getBoolean(Constants.SECURE_CONNECTIONS, Constants.DEFAULT_SECURE_CONNECTIONS)) {
            logger.warn("Proxy host set without port. Using HTTPS default 443");
            awsConf.setProxyPort(443);
          } else {
            logger.warn("Proxy host set without port. Using HTTP default 80");
            awsConf.setProxyPort(80);
          }
        }
        String proxyUsername = conf.getTrimmed(Constants.PROXY_USERNAME);
        String proxyPassword = null;
        char[] proxyPass = conf.getPassword(Constants.PROXY_PASSWORD);
        if (proxyPass != null) {
          proxyPassword = new String(proxyPass).trim();
        }
        if ((proxyUsername == null) != (proxyPassword == null)) {
          String msg = "Proxy error: " + Constants.PROXY_USERNAME + " or " +
              Constants.PROXY_PASSWORD + " set without the other.";
          logger.error(msg);
          throw new IllegalArgumentException(msg);
        }
        awsConf.setProxyUsername(proxyUsername);
        awsConf.setProxyPassword(proxyPassword);
        awsConf.setProxyDomain(conf.getTrimmed(Constants.PROXY_DOMAIN));
        awsConf.setProxyWorkstation(conf.getTrimmed(Constants.PROXY_WORKSTATION));
        if (logger.isDebugEnabled()) {
          logger.debug("Using proxy server {}:{} as user {} on " +
                  "domain {} as workstation {}", awsConf.getProxyHost(),
              awsConf.getProxyPort(),
              String.valueOf(awsConf.getProxyUsername()),
              awsConf.getProxyDomain(),
              awsConf.getProxyWorkstation());
        }
      } else if (proxyPort >= 0) {
        String msg =
            "Proxy error: " + Constants.PROXY_PORT + " set without " + Constants.PROXY_HOST;
        logger.error(msg);
        throw new IllegalArgumentException(msg);
      }
    }

    /**
     * Initializes the User-Agent header to send in HTTP requests to the S3
     * back-end.  We always include the Hadoop version number.  The user also
     * may set an optional custom prefix to put in front of the Hadoop version
     * number.  The AWS SDK interally appends its own information, which seems
     * to include the AWS SDK version, OS and JVM version.
     *
     * @param conf Hadoop configuration
     * @param awsConf AWS SDK configuration
     */
    private static void initUserAgent(Configuration conf,
        ClientConfiguration awsConf) {
      String userAgent = "Hadoop " + VersionInfo.getVersion();
      String userAgentPrefix = conf.getTrimmed(S3Constants.USER_AGENT_PREFIX, "");
      if (!userAgentPrefix.isEmpty()) {
        userAgent = userAgentPrefix + ", " + userAgent;
      }
      logger.debug("Using User-Agent: {}", userAgent);
      awsConf.setUserAgent(userAgent);
    }

    /**
     * Creates an {@link AmazonS3Client} from the established configuration.
     *
     * @param conf Hadoop configuration
     * @param credentials AWS credentials
     * @param awsConf AWS SDK configuration
     * @return S3 client
     * @throws IllegalArgumentException if misconfigured
     */
    private static AmazonS3 createAmazonS3Client(Configuration conf,
        AWSCredentialsProvider credentials, ClientConfiguration awsConf)
        throws IllegalArgumentException {
      AmazonS3 s3 = new AmazonS3Client(credentials, awsConf);
      String endPoint = conf.getTrimmed(Constants.ENDPOINT, "");
      if (!endPoint.isEmpty()) {
        try {
          s3.setEndpoint(endPoint);
        } catch (IllegalArgumentException e) {
          String msg = "Incorrect endpoint: "  + e.getMessage();
          logger.error(msg);
          throw new IllegalArgumentException(msg, e);
        }
      }
      enablePathStyleAccessIfRequired(s3, conf);
      return s3;
    }

    /**
     * Enables path-style access to S3 buckets if configured.  By default, the
     * behavior is to use virtual hosted-style access with URIs of the form
     * http://bucketname.s3.amazonaws.com.  Enabling path-style access and a
     * region-specific endpoint switches the behavior to use URIs of the form
     * http://s3-eu-west-1.amazonaws.com/bucketname.
     *
     * @param s3 S3 client
     * @param conf Hadoop configuration
     */
    private static void enablePathStyleAccessIfRequired(AmazonS3 s3,
        Configuration conf) {
      final boolean pathStyleAccess = conf.getBoolean(S3Constants.PATH_STYLE_ACCESS, false);
      if (pathStyleAccess) {
        logger.debug("Enabling path style access!");
        s3.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
      }
    }

    /**
     * Get a integer option >= the minimum allowed value.
     * @param conf configuration
     * @param key key to look up
     * @param defVal default value
     * @param min minimum value
     * @return the value
     * @throws IllegalArgumentException if the value is below the minimum
     */
    private static int intOption(Configuration conf, String key, int defVal, int min) {
      int v = conf.getInt(key, defVal);
      Preconditions.checkArgument(v >= min,
          String.format("Value of %s: %d is below the minimum value %d",
              key, v, min));
      logger.debug("Value of {} is {}", key, v);
      return v;
    }
  }
}
