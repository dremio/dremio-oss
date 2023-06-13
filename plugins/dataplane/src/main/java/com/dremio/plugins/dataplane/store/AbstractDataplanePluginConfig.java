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
package com.dremio.plugins.dataplane.store;

import static com.dremio.plugins.dataplane.CredentialsProviderConstants.ACCESS_KEY_PROVIDER;
import static com.dremio.plugins.dataplane.CredentialsProviderConstants.ASSUME_ROLE_PROVIDER;
import static com.dremio.plugins.dataplane.CredentialsProviderConstants.NONE_PROVIDER;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Provider;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import org.apache.hadoop.fs.s3a.Constants;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.auth.BearerAuthenticationProvider;
import org.projectnessie.client.http.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.DefaultCtasFormatSelection;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.DoNotDisplay;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.VersionedStoragePluginConfig;
import com.dremio.exec.store.dfs.CacheProperties;
import com.dremio.exec.store.dfs.FileSystemConf;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.dremio.plugins.NessieClient;
import com.dremio.plugins.util.awsauth.AWSCredentialsConfigurator;
import com.dremio.service.namespace.SourceState;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

import io.protostuff.Tag;

public abstract class AbstractDataplanePluginConfig
    extends FileSystemConf<AbstractDataplanePluginConfig, DataplanePlugin>
    implements VersionedStoragePluginConfig {
  // @Tag(1) is used for nessieEndpoint only in Nessie
  // @Tag(2) is used for nessieAccessToken only in Nessie
  @Tag(3)
  @DisplayMetadata(label = "AWS Access Key")
  @NotMetadataImpacting
  public String awsAccessKey = "";

  @Tag(4)
  @Secret
  @DisplayMetadata(label = "AWS Access Secret")
  @NotMetadataImpacting
  public String awsAccessSecret = "";

  @Tag(5)
  @DisplayMetadata(label = "AWS Root Path")
  @NotMetadataImpacting
  public String awsRootPath = "";

  @Tag(6)
  @DisplayMetadata(label = "Connection Properties")
  @NotMetadataImpacting
  public List<Property> propertyList;

  @Tag(7)
  @DisplayMetadata(label = "IAM Role to Assume")
  public String assumedRoleARN;

  // @Tag(8) is used for credentialType in subclasses

  @Tag(9)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Enable asynchronous access when possible")
  public boolean asyncEnabled = true;

  @Tag(10)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Enable local caching when possible")
  public boolean isCachingEnabled = true;

  @Tag(11)
  @NotMetadataImpacting
  @Min(value = 1, message = "Max percent of total available cache space must be between 1 and 100")
  @Max(value = 100, message = "Max percent of total available cache space must be between 1 and 100")
  @DisplayMetadata(label = "Max percent of total available cache space to use when possible")
  public int maxCacheSpacePct = 100;

  @Tag(12)
  @NotMetadataImpacting
  @DoNotDisplay
  @DisplayMetadata(label = "Default CTAS Format")
  public DefaultCtasFormatSelection defaultCtasFormat = DefaultCtasFormatSelection.ICEBERG;
  // @Tag(13) is reserved

  // @Tag(14) is used for nessieAuthType only in Nessie

  // @Tag(15) is used for awsProfile only in Nessie

  // @Tag(16) is used for secure only in Nessie
  @Override
  public CacheProperties getCacheProperties() {
    return new CacheProperties() {
      @Override
      public boolean isCachingEnabled(OptionManager optionManager) {
        return isCachingEnabled;
      }

      @Override
      public int cacheMaxSpaceLimitPct() {
        return maxCacheSpacePct;
      }
    };
  }

  private static final Logger logger = LoggerFactory.getLogger(AbstractDataplanePluginConfig.class);

  @Override
  public abstract DataplanePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider);

  @Override
  public boolean isAsyncEnabled() {
    return asyncEnabled;
  }

  @Override
  public Path getPath() {
    validateAWSRootPath(awsRootPath);
    return Path.of(awsRootPath);
  }

  @Override
  public boolean isImpersonationEnabled() {
    return false;
  }

  @Override
  public String getConnection() {
    return CloudFileSystemScheme.S3_FILE_SYSTEM_SCHEME.getScheme() + ":///";
  }

  @Override
  public boolean isPartitionInferenceEnabled() {
    return false;
  }

  @Override
  public SchemaMutability getSchemaMutability() {
    return SchemaMutability.USER_TABLE;
  }

  @Override
  public List<Property> getProperties() {
    return propertyList != null ? propertyList : Collections.emptyList();
  }

  @Override
  public String getDefaultCtasFormat() {
    return defaultCtasFormat.getDefaultCtasFormat();
  }

  protected static String getAccessKeyProvider(List<Property> properties,
                                               String accessKey,
                                               String accessSecret) {
    if (("".equals(accessKey)) || ("".equals(accessSecret))) {
      throw UserException.validationError()
        .message("Failure creating S3 connection. You must provide AWS Access Key and AWS Access Secret.")
        .build(logger);
    }
    properties.add(new Property(Constants.ACCESS_KEY, accessKey));
    properties.add(new Property(Constants.SECRET_KEY, accessSecret));
    return ACCESS_KEY_PROVIDER;
  }

  protected NessieApiV1 getNessieRestClient(String name, String nessieEndpoint, String nessieAccessToken) {
    final HttpClientBuilder builder = HttpClientBuilder.builder()
      .withUri(URI.create(nessieEndpoint));

    if (!Strings.isNullOrEmpty(nessieAccessToken)) {
      builder.withAuthentication(BearerAuthenticationProvider.create(nessieAccessToken));
    }

    try {
      return builder.withTracing(true).build(NessieApiV1.class);
    } catch (IllegalArgumentException e) {
      throw UserException.resourceError().message("Unable to create source [%s], " +
        "%s must be a valid http or https address", name, nessieEndpoint).build();
    }
  }

  public boolean hasAssumedRoleARN() {
    return !Strings.isNullOrEmpty(assumedRoleARN);
  }

  public AWSCredentialsConfigurator wrapAssumedRoleToProvider(AWSCredentialsConfigurator configurator)
  {
    return properties -> {
      String mainAWSCredProvider = configurator.configureCredentials(properties);
      if (hasAssumedRoleARN() && !NONE_PROVIDER.equals(mainAWSCredProvider)) {
        properties.add(new Property(Constants.ASSUMED_ROLE_ARN, assumedRoleARN));
        properties.add(new Property(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER, mainAWSCredProvider));
        mainAWSCredProvider = ASSUME_ROLE_PROVIDER;
      }
      return mainAWSCredProvider;
    };
  }

  public void validateAWSRootPath(String path) {
    if (!isValidAWSRootPath(path)) {
      throw UserException.validationError()
        .message("Failure creating or updating Nessie source. Invalid AWS Root Path. You must provide a valid AWS S3 path." +
          " Example: /bucket-name/path")
        .build(logger);
    }
  }

  @VisibleForTesting
  protected boolean isValidAWSRootPath(String rootLocation) {
    //TODO: DX-64664 - Verify the regex used for AWS Path.
    // Refer to https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html for bucket naming rules
    Pattern pathPattern = Pattern.compile("^(/?[a-z0-9.-]+)(/[^/]+)*/?$");
    Matcher m = pathPattern.matcher(rootLocation);
    return m.find();
  }

  public abstract void validateNessieAuthSettings(String name);

  public abstract void validatePluginEnabled(SabotContext context);

  public abstract void validateConnectionToNessieRepository(NessieClient nessieClient, String name, SabotContext context);

  public abstract void validateNessieSpecificationVersion(NessieClient nessieClient, String name);

  public abstract Optional<Property> encryptConnection();

  public abstract SourceState getState(NessieClient nessieClient, String name, SabotContext context);
}
