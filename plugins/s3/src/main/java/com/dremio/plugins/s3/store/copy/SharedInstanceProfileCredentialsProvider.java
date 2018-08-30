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

import org.apache.hadoop.fs.s3a.S3AFileSystem;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;

/**
 *
 * (Copied from Hadoop 2.8.3, move to using Hadoop version once hadoop version in MapR profile is upgraded to 2.8.3+)
 *
 * A subclass of {@link InstanceProfileCredentialsProvider} that enforces
 * instantiation of only a single instance.
 * This credential provider calls the EC2 instance metadata service to obtain
 * credentials.  For highly multi-threaded applications, it's possible that
 * multiple instances call the service simultaneously and overwhelm it with
 * load.  The service handles this by throttling the client with an HTTP 429
 * response or forcibly terminating the connection.  Forcing use of a single
 * instance reduces load on the metadata service by allowing all threads to
 * share the credentials.  The base class is thread-safe, and there is nothing
 * that varies in the credentials across different instances of
 * {@link S3AFileSystem} connecting to different buckets, so sharing a singleton
 * instance is safe.
 *
 * As of AWS SDK 1.11.39, the SDK code internally enforces a singleton.  After
 * Hadoop upgrades to that version or higher, it's likely that we can remove
 * this class.
 */
public final class SharedInstanceProfileCredentialsProvider
    extends InstanceProfileCredentialsProvider {

  private static final SharedInstanceProfileCredentialsProvider INSTANCE =
      new SharedInstanceProfileCredentialsProvider();

  /**
   * Returns the singleton instance.
   *
   * @return singleton instance
   */
  public static SharedInstanceProfileCredentialsProvider getInstance() {
    return INSTANCE;
  }

  /**
   * Default constructor, defined explicitly as private to enforce singleton.
   */
  private SharedInstanceProfileCredentialsProvider() {
    super();
  }
}
