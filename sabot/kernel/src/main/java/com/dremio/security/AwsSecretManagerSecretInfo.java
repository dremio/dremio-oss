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
package com.dremio.security;

/**
 * Secret information related to Aws Secrets manager
 */
public final class AwsSecretManagerSecretInfo implements SecretInfo {
  private final String secretArn;

  public AwsSecretManagerSecretInfo(String secretArn) {
    this.secretArn = secretArn;
  }

  public String getSecretArn() { return secretArn; }
}
