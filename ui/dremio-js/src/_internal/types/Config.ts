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

export interface LogFn {
  <T extends object>(obj: T, msg?: string, ...args: any[]): void;
  (obj: unknown, msg?: string, ...args: any[]): void;
  (msg: string, ...args: any[]): void;
}

export type RequestFn = (
  input: string,
  init?: RequestInit | undefined,
) => Promise<Response>;

export type Config = {
  /**
   * Custom fetch provider
   * @default globalThis.fetch
   */
  fetch?: RequestFn;

  /**
   * Dremio API base origin
   */
  origin: string;

  /**
   * Dremio API token
   */
  token?: string | (() => string);

  logger?: {
    debug: LogFn;
    info: LogFn;
  };
};

export type ResourceConfig = {
  logger?: Config["logger"];
  request: RequestFn;
};

export type SonarV2Config = {
  sonarV2Request: RequestFn;
};

export type SonarV3Config = {
  sonarV3Request: RequestFn;
};

export type V3Config = {
  v3Request: RequestFn;
};
