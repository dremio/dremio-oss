/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.exec.store.dfs.FileSystemConfig;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

/**
 * S3 FileSystem plugin config
 */
@JsonTypeName(S3PluginConfig.NAME)
public class S3PluginConfig extends FileSystemConfig {
  private static final Logger logger = LoggerFactory.getLogger(S3PluginConfig.class);

  public static final String NAME = "s3";

  /**
   * Controls how many parallel connections HttpClient spawns.
   * Hadoop configuration property {@link org.apache.hadoop.fs.s3a.Constants#MAXIMUM_CONNECTIONS}.
   */
  public static final int DEFAULT_MAX_CONNECTIONS = 100;
  public static final String EXTERNAL_BUCKETS = "dremio.s3.external.buckets";

  /**
   * Instantiate config object with given S3 plugin configs.
   * @param accessKey S3 access key.
   * @param accessSecret S3 access secret.
   * @param secure Is SSL enabled in connections to S3.
   * @param externalBuckets List of external buckets apart from the buckets owned by the account with given credentials.
   * @param properties Optional property map.
   * @param formats Map of supported (format name, format config) entries.
   */
  public S3PluginConfig(String accessKey, String accessSecret, boolean secure, List<String> externalBuckets,
      Map<String, String> properties, Map<String, FormatPluginConfig> formats) {
    super(null, "/", getConfig(
        accessKey,
        accessSecret,
        secure,
        externalBuckets,
        properties),
        formats,
        false /* S3 doesn't support impersonation */,
        SchemaMutability.NONE);
  }

  @JsonCreator
  public S3PluginConfig(
      @JsonProperty("connection") String connection,
      @JsonProperty("config") Map<String, String> config,
      @JsonProperty("formats") Map<String, FormatPluginConfig> formats) {
    super(connection, "/", config, formats, false /* S3 doesn't support impersonation */,
        SchemaMutability.NONE);
  }

  /**
   * Helper method to get configuration for S3 implementation of {@link FileSystem} from
   * given S3 access credentials and property list.
   */
  private static Map<String, String> getConfig(final String accessKey, final String accessSecret,
      final boolean secure, List<String> externalBuckets, final Map<String, String> properties) {
    final Map<String, String> finalProperties = Maps.newHashMap();
    finalProperties.put(FileSystem.FS_DEFAULT_NAME_KEY, "dremioS3:///");
    finalProperties.put("fs.dremioS3.impl", S3FileSystem.class.getName());
    finalProperties.put(MAXIMUM_CONNECTIONS, String.valueOf(DEFAULT_MAX_CONNECTIONS));
    if(accessKey != null){
      finalProperties.put(ACCESS_KEY, accessKey);
      finalProperties.put(SECRET_KEY, accessSecret);
    } else {
      // don't use Constants here as it breaks when using older MapR/AWS files.
      finalProperties.put("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider");
    }

    if (properties != null && !properties.isEmpty()) {
      for (Entry<String, String> property : properties.entrySet()) {
        finalProperties.put(property.getKey(), property.getValue());
      }
    }


    finalProperties.put(SECURE_CONNECTIONS, String.valueOf(secure));
    if(externalBuckets != null && !externalBuckets.isEmpty()){
      finalProperties.put(EXTERNAL_BUCKETS, Joiner.on(",").join(externalBuckets));
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
