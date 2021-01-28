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
package com.dremio.service.coordinator;


import java.net.URI;

import com.dremio.service.coordinator.proto.AwsKeys;

/**
 * Interface to define project config
 */
public interface ProjectConfig {
  /**
   *
   * @return
   */
  DistPathConfig getAcceleratorConfig();

  /**
   *
   * @return
   */
  DistPathConfig getScratchConfig();

  /**
   *
   * @return
   */
  DistPathConfig getDownloadsConfig();


  /**
   *
   * @return
   */
  String getOrgId();

  /**
   * Defines dist path config.
   */
  public static class DistPathConfig {
    private final URI uri;
    private final AwsKeys awsKeys;

    public DistPathConfig(URI uri, AwsKeys awsKeys) {
      this.uri = uri;
      this.awsKeys = awsKeys;
    }

    public URI getUri() {
      return uri;
    }

    public AwsKeys getAwsKeys() {
      return awsKeys;
    }
  }
}
