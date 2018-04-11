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

import static org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY;
import static org.apache.hadoop.fs.s3a.Constants.MAXIMUM_CONNECTIONS;
import static org.apache.hadoop.fs.s3a.Constants.SECRET_KEY;
import static org.apache.hadoop.fs.s3a.Constants.SECURE_CONNECTIONS;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Provider;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.dfs.FileSystemConf;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;

import io.protostuff.Tag;

/**
 * Connection Configuration for S3.
 */
@SourceType("S3")
public class S3PluginConfig extends FileSystemConf<S3PluginConfig, S3StoragePlugin> {

  private static final Logger logger = LoggerFactory.getLogger(S3PluginConfig.class);
  /**
   * Controls how many parallel connections HttpClient spawns.
   * Hadoop configuration property {@link org.apache.hadoop.fs.s3a.Constants#MAXIMUM_CONNECTIONS}.
   */
  public static final int DEFAULT_MAX_CONNECTIONS = 1000;
  public static final String EXTERNAL_BUCKETS = "dremio.s3.external.buckets";

  //  optional string access_key = 1;
  //  optional string access_secret = 2;
  //  optional bool secure = 3;
  //  repeated string external_bucket = 4;
  //  repeated Property property = 5;

  @Tag(1)
  public String accessKey;

  @Tag(2)
  @Secret
  public String accessSecret;

  @Tag(3)
  public boolean secure;

  @JsonProperty("externalBucketList")
  @Tag(4)
  public List<String> externalBuckets;

  @JsonProperty("propertyList")
  @Tag(5)
  public List<Property> properties;

  @Override
  public S3StoragePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    return new S3StoragePlugin(this, context, name, pluginIdProvider);
  }

  @Override
  public Path getPath() {
    return new Path("/");
  }

  @Override
  public boolean isImpersonationEnabled() {
    return false;
  }

  @Override
  public String getConnection() {
    return "dremioS3:///";
  }

  @Override
  public SchemaMutability getSchemaMutability() {
    return SchemaMutability.NONE;
  }

  @Override
  public List<Property> getProperties() {
    final List<Property> finalProperties = new ArrayList<>();
    finalProperties.add(new Property(FileSystem.FS_DEFAULT_NAME_KEY, "dremioS3:///"));
    finalProperties.add(new Property("fs.dremioS3.impl", S3FileSystem.class.getName()));
    finalProperties.add(new Property(MAXIMUM_CONNECTIONS, String.valueOf(DEFAULT_MAX_CONNECTIONS)));
    finalProperties.add(new Property("fs.s3a.fast.upload", "true"));
    finalProperties.add(new Property("fs.s3a.fast.upload.buffer", "disk"));
    finalProperties.add(new Property("fs.s3a.fast.upload.active.blocks", "4")); // 256mb (so a single parquet file should be able to flush at once).
    finalProperties.add(new Property("fs.s3a.threads.max", "24"));
    finalProperties.add(new Property("fs.s3a.multipart.size", "67108864")); // 64mb
    finalProperties.add(new Property("fs.s3a.max.total.tasks", "30"));

    if(accessKey != null){
      finalProperties.add(new Property(ACCESS_KEY, accessKey));
      finalProperties.add(new Property(SECRET_KEY, accessSecret));
    } else {
      // don't use Constants here as it breaks when using older MapR/AWS files.
      finalProperties.add(new Property("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"));
    }

    if (properties != null && !properties.isEmpty()) {
      finalProperties.addAll(properties);
    }


    finalProperties.add(new Property(SECURE_CONNECTIONS, String.valueOf(secure)));
    if(externalBuckets != null && !externalBuckets.isEmpty()){
      finalProperties.add(new Property(EXTERNAL_BUCKETS, Joiner.on(",").join(externalBuckets)));
    }else {
      if(accessKey == null){
        throw UserException.validationError()
          .message("Failure creating S3 connection. You must provide at least one of: (1) access key and secret key or (2) one or more external buckets.")
          .build(logger);
      }
    }

    return finalProperties;
  }
}
