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

type Protocol = "http://" | "https://";

export type Config = {
  /**
   * Custom fetch provider
   * @default global.fetch
   */
  fetch?: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

  /**
   * Dremio origin that requests are made to
   */
  origin: `${Protocol}${string}`;

  /**
   * Dremio API token. If not provided, session token cookie will be attempted instead
   */
  token?: string | (() => string);
};

export type ResourceConfig = {
  request: (path: string, init?: RequestInit) => Promise<Response>;
};

export type SonarResourceConfig = ResourceConfig & {
  sonarV2Request: (path: string, init?: RequestInit) => Promise<Response>;
  sonarV3Request: (path: string, init?: RequestInit) => Promise<Response>;
};
