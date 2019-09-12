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
package com.dremio.plugins.elastic;

import static com.google.common.base.Strings.isNullOrEmpty;

import java.util.List;

import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.regions.RegionUtils;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.conf.Host;

public class ElasticsearchAuthentication {

  private static final Logger logger = LoggerFactory.getLogger(ElasticsearchAuthentication.class);
  private final ElasticsearchConf.AuthenticationType authenticationType;
  private final String username;
  private final String password;
  private final String regionName;
  private final AWSCredentialsProvider awsCredentialsProvider;

  public ElasticsearchAuthentication(List<Host> hosts, ElasticsearchConf.AuthenticationType authenticationType,
                                     String username, String password, String accessKey, String accessSecret,
                                     String regionName) {
    this.authenticationType = authenticationType;
    switch (authenticationType) {
      case ES_ACCOUNT:
        this.username = username;
        this.password = password;
        this.awsCredentialsProvider = null;
        this.regionName = null;
        break;
      case ACCESS_KEY:
        this.username = null;
        this.password = null;
        if (("".equals(accessKey)) || ("".equals(accessSecret))) {
          throw UserException.validationError()
            .message("Failure creating Amazon Elasticsearch Service connection. You must provide AWS Access Key and AWS Access Secret.")
            .build(logger);
        }
        this.awsCredentialsProvider = new BasicAWSCredentialsProvider(accessKey, accessSecret);
        this.regionName = getRegionName(regionName, hosts.get(0).hostname);
        break;
      case EC2_METADATA:
        this.username = null;
        this.password = null;
        this.awsCredentialsProvider = new InstanceProfileCredentialsProvider();
        this.regionName = getRegionName(regionName, hosts.get(0).hostname);
        break;
      case NONE:
        this.username = null;
        this.password = null;
        this.awsCredentialsProvider = null;
        this.regionName = null;
        break;
      default:
        throw new RuntimeException("Failure creating Elasticsearch connection. Invalid credential type.");
    }
  }

  /**
   * Get region name and check the validity of it.
   * @param regionName region name provided in config
   * @param endpoint endpoint provided in config
   * @return region name
   */
  protected static String getRegionName(String regionName, String endpoint) {
    if (isNullOrEmpty(regionName)) {
      String[] splits = endpoint.split("\\.");
      int count = splits.length;
      if ((count < 5) || (!"com".equals(splits[count - 1])) || (!"amazonaws".equals(splits[count - 2])) || (!"es".equals(splits[count - 3]))) {
        throw UserException.validationError()
          .message("Failure creating Amazon Elasticsearch Service connection. You must provide hostname like *.[region name].es.amazonaws.com")
          .build(logger);
      }
      regionName = splits[count - 4];
    }
    try {
      Regions region = Regions.fromName(regionName);
    }
    catch (IllegalArgumentException iae) {
      throw UserException.validationError()
        .message("Failure creating Amazon Elasticsearch Service connection. The region is incorrect. You can change region in Advanced Options")
        .build(logger);
    }
    return regionName;
  }

  public String getUsername() {
    return username;
  }

  public HttpAuthenticationFeature getHttpAuthenticationFeature() {
    if (username == null) {
      return null;
    }
    return HttpAuthenticationFeature.basic(username, password);
  }

  public String getRegionName() {
    return regionName;
  }

  public AWSCredentialsProvider getAwsCredentialsProvider() {
    return awsCredentialsProvider;
  }
}
