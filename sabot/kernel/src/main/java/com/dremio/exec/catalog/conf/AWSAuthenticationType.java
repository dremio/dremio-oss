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
package com.dremio.exec.catalog.conf;

import io.protostuff.Tag;

/**
 * Types of authentication to use with AWS.
 */
public enum AWSAuthenticationType {
  /**
   * Uses raw credentials.
   */
  @Tag(1) @DisplayMetadata(label = "AWS Access Key") ACCESS_KEY,

  /**
   * Uses IAM roles from an EC2 instance.
   */
  @Tag(2) @DisplayMetadata(label = "EC2 Metadata") EC2_METADATA,

  /**
   * Access S3 as anonymous.
   */
  @Tag(3) @DisplayMetadata(label = "No Authentication") NONE,

  /**
   * Use files from a AWS named profile
   */
  @Tag(4) @DisplayMetadata(label = "AWS Profile") AWS_PROFILE,

  /**
   * Access from web Identity Token (EKS service Account)
   */
  @Tag(5) @DisplayMetadata(label = "Web Identity Token") WEB_IDENTITY_TOKEN;
}
