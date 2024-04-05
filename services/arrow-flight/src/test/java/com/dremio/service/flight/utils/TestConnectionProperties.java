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

package com.dremio.service.flight.utils;

import com.dremio.service.flight.FlightClientUtils.FlightClientWrapper.FlightClientType;
import java.io.InputStream;

public class TestConnectionProperties {
  private final FlightClientType clientType;
  private final String host;
  private final Integer port;
  private final String user;
  private final String password;
  private final InputStream trustedCerts;

  public TestConnectionProperties(
      FlightClientType clientType,
      String host,
      Integer port,
      String user,
      String password,
      InputStream trustedCerts) {
    this.clientType = clientType;
    this.host = host;
    this.port = port;
    this.user = user;
    this.password = password;
    this.trustedCerts = trustedCerts;
  }

  public FlightClientType getClientType() {
    return clientType;
  }

  public String getHost() {
    return host;
  }

  public Integer getPort() {
    return port;
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }

  public InputStream getTrustedCerts() {
    return trustedCerts;
  }
}
