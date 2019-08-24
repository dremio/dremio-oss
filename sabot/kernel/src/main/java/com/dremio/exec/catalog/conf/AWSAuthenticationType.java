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
 * Authentication type for Amazon Services.
 * ACCESS_KEY uses credentials, EC2_METADATA uses IAM roles in EC2 instance and NONE access S3 as anonymous.
 * Date : 24-Aug-2019, Author : Sathish Senathi
 * Added Default Auth Chain option from AWS SDK which allows Credentials to be retreived from Properties, Env Variables,
 * Default profile in user home directory and Instance Metadata in that order
 */
public enum AWSAuthenticationType {
  @Tag(1) @DisplayMetadata(label = "AWS Access Key") ACCESS_KEY,
  @Tag(2) @DisplayMetadata(label = "Default Chain") TEMP_CREDENTIALS,
  @Tag(2) @DisplayMetadata(label = "EC2 Metadata") EC2_METADATA,
  @Tag(3) @DisplayMetadata(label = "No Authentication") NONE;
}
