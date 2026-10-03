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
package com.dremio.plugins.icebergcatalog.store;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

/**
 * S3 credentials that an Iceberg REST catalog vended for one table (access delegation {@code
 * vended-credentials}), mapped onto the Hadoop S3A properties that Dremio's S3 file system reads.
 *
 * <p>The catalog returns them in the {@code config} of its load table (or staged create) response,
 * using the Iceberg {@code S3FileIO} property names; the Iceberg REST client merges that config
 * into the properties of the table's FileIO. Only credentials are mapped: endpoint, path style
 * access, region and TLS keep coming from the source's storage properties ({@code fs.s3a.*}, {@code
 * dremio.s3.*}), which describe how Dremio reaches the storage. Bucket discovery is the exception:
 * it is turned off (see {@link #applyTo}).
 *
 * <p>The values are secrets: {@link #toString()} and every message built from this class only show
 * which kind of credentials it holds and when they expire.
 */
final class VendedStorageCredentials {

  // Iceberg S3FileIO / AwsClientProperties property names (as returned by the catalog).
  static final String S3_ACCESS_KEY_ID = "s3.access-key-id";
  static final String S3_SECRET_ACCESS_KEY = "s3.secret-access-key";
  static final String S3_SESSION_TOKEN = "s3.session-token";
  static final String S3_SESSION_TOKEN_EXPIRES_AT_MS = "s3.session-token-expires-at-ms";

  /** Expiry of the vended credentials as sent by Apache Polaris (epoch millis). */
  static final String EXPIRATION_TIME = "expiration-time";

  // Hadoop S3A properties read by Dremio's S3 file system (plugins/s3).
  static final String FS_S3A_CREDENTIALS_PROVIDER = "fs.s3a.aws.credentials.provider";
  static final String FS_S3A_ACCESS_KEY = "fs.s3a.access.key";
  static final String FS_S3A_SECRET_KEY = "fs.s3a.secret.key";
  static final String FS_S3A_SESSION_TOKEN = "fs.s3a.session.token";
  static final String SIMPLE_CREDENTIALS_PROVIDER =
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";
  static final String TEMPORARY_CREDENTIALS_PROVIDER =
      "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider";

  /** Prefix of the Hadoop S3A per-bucket settings, {@code fs.s3a.bucket.<bucket>.<setting>}. */
  static final String FS_S3A_BUCKET_PREFIX = "fs.s3a.bucket.";

  /** Per-bucket settings that would override the vended credentials. */
  private static final String[] PER_BUCKET_CREDENTIAL_SUFFIXES = {
    ".access.key", ".secret.key", ".session.token", ".aws.credentials.provider"
  };

  /** Dremio's S3 file system lists all buckets on start unless this is false. */
  static final String DREMIO_BUCKET_DISCOVERY = "dremio.bucket.discovery.enabled";

  private final String accessKeyId;
  private final String secretAccessKey;
  @Nullable private final String sessionToken;
  @Nullable private final Long expiresAtMillis;

  private VendedStorageCredentials(
      String accessKeyId,
      String secretAccessKey,
      @Nullable String sessionToken,
      @Nullable Long expiresAtMillis) {
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
    this.sessionToken = sessionToken;
    this.expiresAtMillis = expiresAtMillis;
  }

  /**
   * Extracts the S3 credentials from the properties of a table's FileIO (catalog properties merged
   * with the catalog's table config). Empty if they do not carry both an access key ID and a secret
   * access key.
   */
  static Optional<VendedStorageCredentials> fromTableProperties(
      @Nullable Map<String, String> properties) {
    if (properties == null) {
      return Optional.empty();
    }
    String accessKeyId = properties.get(S3_ACCESS_KEY_ID);
    String secretAccessKey = properties.get(S3_SECRET_ACCESS_KEY);
    if (StringUtils.isBlank(accessKeyId) || StringUtils.isBlank(secretAccessKey)) {
      return Optional.empty();
    }
    String sessionToken = properties.get(S3_SESSION_TOKEN);
    return Optional.of(
        new VendedStorageCredentials(
            accessKeyId,
            secretAccessKey,
            StringUtils.isBlank(sessionToken) ? null : sessionToken,
            earliest(
                parseMillis(properties.get(S3_SESSION_TOKEN_EXPIRES_AT_MS)),
                parseMillis(properties.get(EXPIRATION_TIME)))));
  }

  @Nullable
  private static Long parseMillis(@Nullable String value) {
    if (StringUtils.isBlank(value)) {
      return null;
    }
    try {
      long millis = Long.parseLong(value.trim());
      return millis > 0 ? millis : null;
    } catch (NumberFormatException e) {
      return null;
    }
  }

  @Nullable
  private static Long earliest(@Nullable Long a, @Nullable Long b) {
    if (a == null) {
      return b;
    }
    return b == null ? a : Math.min(a, b);
  }

  /**
   * Makes the given configuration use these credentials for S3: replaces the credentials provider
   * and the static keys of the source. Temporary (session) credentials use Hadoop's {@code
   * TemporaryAWSCredentialsProvider}, which Dremio's S3 file system supports for both of its S3
   * clients.
   *
   * <p>Per-bucket credential settings of the source ({@code fs.s3a.bucket.<bucket>.access.key},
   * {@code .secret.key}, {@code .session.token}, {@code .aws.credentials.provider}) are removed:
   * Dremio ({@code FileSystemConfUtil}) and Hadoop S3A copy them over the global ones, which would
   * mix the source's keys with the vended session token. Bucket discovery is turned off: vended
   * credentials are scoped to the table's location and normally may neither list all buckets nor
   * list a bucket's root, while the table's bucket is reached without that check.
   */
  void applyTo(Configuration conf) {
    conf.set(FS_S3A_ACCESS_KEY, accessKeyId);
    conf.set(FS_S3A_SECRET_KEY, secretAccessKey);
    if (sessionToken != null) {
      conf.set(FS_S3A_SESSION_TOKEN, sessionToken);
      conf.set(FS_S3A_CREDENTIALS_PROVIDER, TEMPORARY_CREDENTIALS_PROVIDER);
    } else {
      conf.unset(FS_S3A_SESSION_TOKEN);
      conf.set(FS_S3A_CREDENTIALS_PROVIDER, SIMPLE_CREDENTIALS_PROVIDER);
    }
    for (String bucketKey : conf.getPropsWithPrefix(FS_S3A_BUCKET_PREFIX).keySet()) {
      for (String suffix : PER_BUCKET_CREDENTIAL_SUFFIXES) {
        if (bucketKey.endsWith(suffix)) {
          conf.unset(FS_S3A_BUCKET_PREFIX + bucketKey);
          break;
        }
      }
    }
    conf.setBoolean(DREMIO_BUCKET_DISCOVERY, false);
  }

  boolean isSessionCredential() {
    return sessionToken != null;
  }

  /** When the credentials expire (epoch millis), or null if the catalog did not say. */
  @Nullable
  Long getExpiresAtMillis() {
    return expiresAtMillis;
  }

  /** Never shows credential values. */
  @Override
  public String toString() {
    return (isSessionCredential() ? "temporary S3 credentials" : "S3 access keys")
        + (expiresAtMillis == null
            ? " without expiry"
            : " expiring at " + Instant.ofEpochMilli(expiresAtMillis));
  }
}
