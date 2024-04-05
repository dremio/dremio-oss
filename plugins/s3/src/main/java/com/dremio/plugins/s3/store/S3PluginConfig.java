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

import static com.dremio.plugins.s3.store.S3StoragePlugin.AWS_PROFILE_PROVIDER;
import static com.dremio.plugins.s3.store.S3StoragePlugin.EC2_METADATA_PROVIDER;
import static com.dremio.plugins.s3.store.S3StoragePlugin.NONE_PROVIDER;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.AWSAuthenticationType;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.plugins.util.awsauth.AWSCredentialsConfigurator;
import io.protostuff.Tag;
import javax.inject.Provider;

/** Connection Configuration for S3. */
@SourceType(value = "S3", label = "Amazon S3", uiConfig = "s3-layout.json")
public class S3PluginConfig extends AbstractS3PluginConfig {
  @Tag(8)
  public AWSAuthenticationType credentialType = AWSAuthenticationType.ACCESS_KEY;

  @Tag(19)
  @DisplayMetadata(label = "AWS Profile")
  public String awsProfile;

  @Override
  public S3StoragePlugin newPlugin(
      SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    return new S3StoragePlugin(
        this,
        context,
        name,
        pluginIdProvider,
        getS3CredentialsProvider(),
        AWSAuthenticationType.NONE.equals(credentialType));
  }

  public AWSAuthenticationType getCredentialType() {
    return credentialType;
  }

  public String getAwsProfile() {
    return awsProfile;
  }

  private AWSCredentialsConfigurator getS3CredentialsProvider() {
    switch (credentialType) {
      case ACCESS_KEY:
        return properties -> getAccessKeyProvider(properties, accessKey, accessSecret);
      case AWS_PROFILE:
        return properties -> {
          if (awsProfile != null) {
            properties.add(new Property("com.dremio.awsProfile", awsProfile));
          }
          return AWS_PROFILE_PROVIDER;
        };
      case EC2_METADATA:
        return properties -> EC2_METADATA_PROVIDER;
      case NONE:
        return properties -> NONE_PROVIDER;
      default:
        throw new UnsupportedOperationException(
            "Failure creating S3 connection. Unsupported credential type:" + credentialType);
    }
  }
}
