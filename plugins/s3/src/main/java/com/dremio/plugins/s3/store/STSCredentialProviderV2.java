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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.util.VersionInfo;

import com.dremio.common.exceptions.UserException;
import com.google.common.base.Preconditions;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

/**
 * Assume role credential provider that supports aws sdk 2.X
 */
public class STSCredentialProviderV2 implements AwsCredentialsProvider {
  private StsAssumeRoleCredentialsProvider stsAssumeRoleCredentialsProvider;

  public STSCredentialProviderV2(Configuration conf) {
    AwsCredentialsProvider awsCredentialsProvider = null;

    if (S3StoragePlugin.ACCESS_KEY_PROVIDER.equals(conf.get(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER))) {
      awsCredentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(
        conf.get(Constants.ACCESS_KEY), conf.get(Constants.SECRET_KEY)));
    } else if (S3StoragePlugin.EC2_METADATA_PROVIDER.equals(conf.get(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER))) {
      awsCredentialsProvider = InstanceProfileCredentialsProvider.create();
    }

    final StsClientBuilder builder = StsClient.builder()
      .credentialsProvider(awsCredentialsProvider)
      .region(S3FileSystem.getAWSRegionFromConfigurationOrDefault(conf))
      .httpClientBuilder(initConnectionSettings(conf));
    S3FileSystem.getStsEndpoint(conf).ifPresent(e -> {
      try {
        builder.endpointOverride(new URI(e));
      } catch (URISyntaxException use) {
        throw UserException.sourceInBadState(use).buildSilently();
      }
    });

    initUserAgent(builder, conf);

    final AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder()
      .roleArn(conf.get(Constants.ASSUMED_ROLE_ARN))
      .roleSessionName(UUID.randomUUID().toString())
      .build();

    this.stsAssumeRoleCredentialsProvider = StsAssumeRoleCredentialsProvider.builder()
      .refreshRequest(assumeRoleRequest)
      .stsClient(builder.build())
      .build();
  }

  @Override
  public AwsCredentials resolveCredentials() {
    return this.stsAssumeRoleCredentialsProvider.resolveCredentials();
  }

  public static SdkHttpClient.Builder initConnectionSettings(Configuration conf) {
    final ApacheHttpClient.Builder httpBuilder = ApacheHttpClient.builder();
    httpBuilder.maxConnections(intOption(conf, Constants.MAXIMUM_CONNECTIONS, Constants.DEFAULT_MAXIMUM_CONNECTIONS, 1));
    httpBuilder.connectionTimeout(
      Duration.ofSeconds(intOption(conf, Constants.ESTABLISH_TIMEOUT, Constants.DEFAULT_ESTABLISH_TIMEOUT, 0)));
    httpBuilder.socketTimeout(
      Duration.ofSeconds(intOption(conf, Constants.SOCKET_TIMEOUT, Constants.DEFAULT_SOCKET_TIMEOUT, 0)));
    httpBuilder.proxyConfiguration(initProxySupport(conf));

    return httpBuilder;
  }

  public static ProxyConfiguration initProxySupport(Configuration conf) throws IllegalArgumentException {
    final ProxyConfiguration.Builder builder = ProxyConfiguration.builder();

    final String proxyHost = conf.getTrimmed(Constants.PROXY_HOST, "");
    int proxyPort = conf.getInt(Constants.PROXY_PORT, -1);
    if (!proxyHost.isEmpty()) {
      if (proxyPort < 0) {
        if (conf.getBoolean(Constants.SECURE_CONNECTIONS, Constants.DEFAULT_SECURE_CONNECTIONS)) {
          proxyPort = 443;
        } else {
          proxyPort = 80;
        }
      }

      builder.endpoint(URI.create(proxyHost + ":" + proxyPort));

      try {
        final String proxyUsername = lookupPassword(conf, Constants.PROXY_USERNAME);
        final String proxyPassword = lookupPassword(conf, Constants.PROXY_PASSWORD);
        if ((proxyUsername == null) != (proxyPassword == null)) {
          throw new IllegalArgumentException(String.format("Proxy error: %s or %s set without the other.",
            Constants.PROXY_USERNAME, Constants.PROXY_PASSWORD));
        }

        builder.username(proxyUsername);
        builder.password(proxyPassword);
        builder.ntlmDomain(conf.getTrimmed(Constants.PROXY_DOMAIN));
        builder.ntlmWorkstation(conf.getTrimmed(Constants.PROXY_WORKSTATION));
      } catch (IOException e) {
        throw UserException.sourceInBadState(e).buildSilently();
      }
    } else if (proxyPort >= 0) {
      throw new IllegalArgumentException(String.format("Proxy error: %s set without %s",
        Constants.PROXY_HOST, Constants.PROXY_PORT));
    }

    return builder.build();
  }

  private static void initUserAgent(StsClientBuilder builder, Configuration conf) {
    String userAgent = "Hadoop " + VersionInfo.getVersion();
    final String userAgentPrefix = conf.getTrimmed(Constants.USER_AGENT_PREFIX, "");
    if (!userAgentPrefix.isEmpty()) {
      userAgent = userAgentPrefix + ", " + userAgent;
    }

    builder.overrideConfiguration(ClientOverrideConfiguration.builder()
      .putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, userAgent)
      .build());
  }

  static int intOption(Configuration conf, String key, int defVal, int min) {
    final int v = conf.getInt(key, defVal);
    Preconditions.checkArgument(v >= min, "Value of %s: %d is below the minimum value %d", key, v, min);
    return v;
  }

  static String lookupPassword(Configuration conf, String key) throws IOException {
    try {
      final char[] pass = conf.getPassword(key);
      return pass != null ? (new String(pass)).trim() : null;
    } catch (IOException ioe) {
      throw new IOException("Cannot find password option " + key, ioe);
    }
  }
}
