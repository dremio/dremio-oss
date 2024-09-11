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
package com.dremio.exec.catalog.dataplane.test;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class FakeGcsServerContainer extends GenericContainer<FakeGcsServerContainer> {

  private static final DockerImageName IMAGE_NAME =
      DockerImageName.parse(
          "docker.io/fsouza/fake-gcs-server:1.48.0@sha256:48053edaf5c2ebfb37580512755788354a8b4da6c746c6d84973cde5698d970d");
  private final int httpPort;

  public FakeGcsServerContainer(int httpPort) {
    super(IMAGE_NAME);

    this.httpPort = httpPort;

    // We need to configure the internal port (and have it match the external port) because
    // fake-gcs-server reports its own internal port in the "location" property of HTTP responses.
    this.addFixedExposedPort(httpPort, httpPort);
    this.setCommand("-scheme http -port " + httpPort);
  }

  public int getHttpPort() {
    return this.getMappedPort(httpPort);
  }
}
