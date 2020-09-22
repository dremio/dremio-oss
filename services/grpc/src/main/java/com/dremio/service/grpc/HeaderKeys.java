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
package com.dremio.service.grpc;

import io.grpc.Metadata;

/**
 * grpc header keys used across dremio services.
 */
public class HeaderKeys {
  public static final Metadata.Key<String> TENANT_ID_HEADER_KEY =
    Metadata.Key.of("tenant_id_key", Metadata.ASCII_STRING_MARSHALLER);

  public static final Metadata.Key<String> USER_HEADER_KEY =
    Metadata.Key.of("user_key", Metadata.ASCII_STRING_MARSHALLER);

  public static final Metadata.Key<String> RELEASE_NAME_HEADER_KEY =
    Metadata.Key.of("x-dremio-control-plane-service-release-name",
      Metadata.ASCII_STRING_MARSHALLER);

  public static final Metadata.Key<String> SERVICE_NAME_HEADER_KEY =
    Metadata.Key.of("x-dremio-control-plane-service", Metadata.ASCII_STRING_MARSHALLER);
}
