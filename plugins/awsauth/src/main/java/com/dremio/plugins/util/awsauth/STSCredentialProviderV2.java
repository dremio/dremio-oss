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
package com.dremio.plugins.util.awsauth;

import static com.dremio.plugins.util.awsauth.DremioAWSCredentialsProviderFactoryV2.ACCESS_KEY_PROVIDER;
import static com.dremio.plugins.util.awsauth.DremioAWSCredentialsProviderFactoryV2.DREMIO_ASSUME_ROLE_PROVIDER;
import static com.dremio.plugins.util.awsauth.DremioAWSCredentialsProviderFactoryV2.EC2_METADATA_PROVIDER;
import static com.dremio.plugins.util.awsauth.DremioAWSCredentialsProviderFactoryV2.GLUE_DREMIO_ASSUME_ROLE_PROVIDER;

import com.dremio.aws.SharedInstanceProfileCredentialsProvider;
import com.dremio.common.exceptions.UserException;
import com.dremio.service.coordinator.DremioAssumeRoleCredentialsProviderV2;
import com.google.common.base.Strings;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.utils.SdkAutoCloseable;

/** Assume role credential provider that supports aws sdk 2.X */
public class STSCredentialProviderV2 implements AwsCredentialsProvider, SdkAutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(STSCredentialProviderV2.class);
  private final StsAssumeRoleCredentialsProvider stsAssumeRoleCredentialsProvider;
  static final String REGION_OVERRIDE = "dremio.s3.region";
  private static final String S3_ENDPOINT_END = ".amazonaws.com";
  private static final String S3_CN_ENDPOINT_END = S3_ENDPOINT_END + ".cn";

  public STSCredentialProviderV2(Configuration conf) {
    AwsCredentialsProvider awsCredentialsProvider = null;

    String assumeRoleProvider = conf.get(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER);
    logger.debug("assumed_role_credentials_provider: {}", assumeRoleProvider);
    switch (assumeRoleProvider) {
      case ACCESS_KEY_PROVIDER:
        awsCredentialsProvider =
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(
                    conf.get(Constants.ACCESS_KEY), conf.get(Constants.SECRET_KEY)));
        break;
      case EC2_METADATA_PROVIDER:
        awsCredentialsProvider = new SharedInstanceProfileCredentialsProvider();
        break;

      case GLUE_DREMIO_ASSUME_ROLE_PROVIDER:
        awsCredentialsProvider = new GlueDremioAssumeRoleCredentialsProviderV2();
        break;

      case DREMIO_ASSUME_ROLE_PROVIDER:
        awsCredentialsProvider = new DremioAssumeRoleCredentialsProviderV2();
        break;
      default:
        throw new IllegalArgumentException(
            "Assumed role credentials provided " + assumeRoleProvider + " is not supported.");
    }
    if (ACCESS_KEY_PROVIDER.equals(conf.get(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER))) {
      awsCredentialsProvider =
          StaticCredentialsProvider.create(
              AwsBasicCredentials.create(
                  conf.get(Constants.ACCESS_KEY), conf.get(Constants.SECRET_KEY)));
    } else if (EC2_METADATA_PROVIDER.equals(
        conf.get(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER))) {
      awsCredentialsProvider = new SharedInstanceProfileCredentialsProvider();
    }

    final StsClientBuilder builder =
        StsClient.builder()
            .credentialsProvider(awsCredentialsProvider)
            .region(getAWSRegionFromConfigurationOrDefault(conf))
            .httpClientBuilder(ApacheHttpConnectionUtil.initConnectionSettings(conf));
    getStsEndpoint(conf)
        .ifPresent(
            e -> {
              try {
                builder.endpointOverride(new URI(e));
              } catch (URISyntaxException use) {
                throw UserException.sourceInBadState(use).buildSilently();
              }
            });

    initUserAgent(builder, conf);

    final AssumeRoleRequest assumeRoleRequest =
        AssumeRoleRequest.builder()
            .roleArn(conf.get(Constants.ASSUMED_ROLE_ARN))
            .roleSessionName(UUID.randomUUID().toString())
            .build();

    this.stsAssumeRoleCredentialsProvider =
        StsAssumeRoleCredentialsProvider.builder()
            .refreshRequest(assumeRoleRequest)
            .stsClient(builder.build())
            .build();
  }

  static Optional<String> getStsEndpoint(Configuration conf) {
    return Optional.ofNullable(conf.getTrimmed(Constants.ASSUMED_ROLE_STS_ENDPOINT))
        .map(s -> "https://" + s);
  }

  @Override
  public AwsCredentials resolveCredentials() {
    return this.stsAssumeRoleCredentialsProvider.resolveCredentials();
  }

  static software.amazon.awssdk.regions.Region getAWSRegionFromConfigurationOrDefault(
      Configuration conf) {
    final String regionOverride = conf.getTrimmed(REGION_OVERRIDE);
    if (!Strings.isNullOrEmpty(regionOverride)) {
      // set the region to what the user provided unless they provided an empty string.
      return software.amazon.awssdk.regions.Region.of(regionOverride);
    }

    return getAwsRegionFromEndpoint(conf.get(Constants.ENDPOINT));
  }

  static software.amazon.awssdk.regions.Region getAwsRegionFromEndpoint(String endpoint) {
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
                software.amazon.awssdk.regions.Region.regions().stream()
                    .filter(region -> e.contains(region.id()))
                    .findFirst()) // map the endpoint to the region contained within it, if any
        .orElse(
            software.amazon.awssdk.regions.Region
                .US_EAST_1); // default to US_EAST_1 if no regions are found.
  }

  private static void initUserAgent(StsClientBuilder builder, Configuration conf) {
    String userAgent = "Hadoop " + VersionInfo.getVersion();
    final String userAgentPrefix = conf.getTrimmed(Constants.USER_AGENT_PREFIX, "");
    if (!userAgentPrefix.isEmpty()) {
      userAgent = userAgentPrefix + ", " + userAgent;
    }

    builder.overrideConfiguration(
        ClientOverrideConfiguration.builder()
            .putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, userAgent)
            .build());
  }

  @Override
  public void close() {
    stsAssumeRoleCredentialsProvider.close();
  }
}
