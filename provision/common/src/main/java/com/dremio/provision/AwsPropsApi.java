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
package com.dremio.provision;

import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.URL;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Redacted;

import com.dremio.common.SentinelSecure;
import com.dremio.provision.resource.ProvisioningResource;
import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils;

/**
 * AWS Props
 */
@JsonDeserialize(builder = ImmutableAwsPropsApi.Builder.class)
@Immutable
public interface AwsPropsApi {

  String getVpc();
  String getSubnetId();
  @NotNull String getNodeIamInstanceProfile(); // role for the started ec2 instances, required
  String getAmiId(); // optional, used to override the default ami
  @Default default boolean getUseClusterPlacementGroup() {return true;}
  @NotNull String getSecurityGroupId();
  @NotNull String getSshKeyName();

  @NotNull String getInstanceType();
  @NotNull AwsConnectionPropsApi getConnectionProps();
  String getExtraConfProps();

  /**
   * Type of AWS auth
   */
  public enum AuthModeApi {
    UNKNOWN,
    AUTO,
    SECRET;
  }

  public static ImmutableAwsPropsApi.Builder builder() {
    return new ImmutableAwsPropsApi.Builder();
  }

  /**
   * AWS Connection Properties
   */
  @JsonDeserialize(builder = ImmutableAwsConnectionPropsApi.Builder.class)
  @JsonFilter(SentinelSecure.FILTER_NAME)
  @Immutable
  public interface AwsConnectionPropsApi {
    @NotNull AuthModeApi getAuthMode();
    String getAssumeRole(); // optional, role for the ec2 commands
    String getAccessKey();

    @Redacted @SentinelSecure(ProvisioningResource.USE_EXISTING_SECRET_VALUE) String getSecretKey();
    @Default default String getRegion() { return getEc2Region(); }

    @URL
    String getEndpoint();

    @URL
    String getStsEndpoint();

    public static ImmutableAwsConnectionPropsApi.Builder builder() {
      return new ImmutableAwsConnectionPropsApi.Builder();
    }

    static String getEc2Region() {
      String region = null;
      try {
        region = EC2MetadataUtils.getEC2InstanceRegion();
      } catch (Exception ignored) {}
      if (region == null) {
        region = Region.US_EAST_1.id();
      }
      return region;
    }
  }

}
