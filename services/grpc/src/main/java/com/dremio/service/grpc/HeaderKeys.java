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
  public static final Metadata.Key<String> PROJECT_ID_HEADER_KEY =
    Metadata.Key.of("x-dremio-project-id-key", Metadata.ASCII_STRING_MARSHALLER);

  public static final Metadata.Key<String> TOKEN_HEADER_KEY =
    Metadata.Key.of("x-dremio-token-key", Metadata.ASCII_STRING_MARSHALLER);

  public static final Metadata.Key<String> ORG_ID_HEADER_KEY =
    Metadata.Key.of("x-dremio-org-id-key", Metadata.ASCII_STRING_MARSHALLER);

  public static final Metadata.Key<String> USER_HEADER_KEY =
    Metadata.Key.of("x-dremio-user-key", Metadata.ASCII_STRING_MARSHALLER);

  public static final Metadata.Key<String> RELEASE_NAME_HEADER_KEY =
    Metadata.Key.of("x-dremio-control-plane-service-release-name",
      Metadata.ASCII_STRING_MARSHALLER);

  public static final Metadata.Key<String> SERVICE_NAME_HEADER_KEY =
    Metadata.Key.of("x-dremio-control-plane-service", Metadata.ASCII_STRING_MARSHALLER);

  public static final Metadata.Key<String> SUPPORT_TICKET_HEADER_KEY =
    Metadata.Key.of("x-dremio-support-ticket-key", Metadata.ASCII_STRING_MARSHALLER);

  public static final Metadata.Key<String> SUPPORT_EMAIL_HEADER_KEY =
    Metadata.Key.of("x-dremio-support-email-key", Metadata.ASCII_STRING_MARSHALLER);

}
