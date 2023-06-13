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
/* eslint-disable */
/**
 *
 * @export
 * @interface AccessKeyCredentials
 */
export interface AccessKeyCredentials extends ConfigCredentials {
  /**
   *
   * @type {string}
   * @memberof AccessKeyCredentials
   */
  accessKeyId: string;
  /**
   *
   * @type {string}
   * @memberof AccessKeyCredentials
   */
  secretAccessKey: string;
}

/**
 * @export
 * @namespace AccessKeyCredentials
 */
export namespace AccessKeyCredentials {}
/**
 * Credentials used by all jobs and schedules on this catalog
 * @export
 * @interface ConfigCredentials
 */
export interface ConfigCredentials {
  /**
   *
   * @type {string}
   * @memberof ConfigCredentials
   */
  type: ConfigCredentials.TypeEnum;
}

/**
 * @export
 * @namespace ConfigCredentials
 */
export namespace ConfigCredentials {
  /**
   * @export
   * @enum {string}
   */
  export enum TypeEnum {
    ACCESSKEY = <any>"ACCESS_KEY",
    IAMROLE = <any>"IAM_ROLE",
  }
}
/**
 *
 * @export
 * @interface EngineInfoState
 */
export interface EngineInfoState {
  /**
   *
   * @type {EngineStateRead}
   * @memberof EngineInfoState
   */
  state: EngineStateRead;
}
/**
 *
 * @export
 * @interface EngineRead
 */
export interface EngineRead {
  /**
   * ID of the cloud in which to run jobs and schedules on this catalog
   * @type {string}
   * @memberof EngineRead
   */
  cloudId: string;
  /**
   *
   * @type {ConfigCredentials}
   * @memberof EngineRead
   */
  credentials?: ConfigCredentials;
  /**
   *
   * @type {EngineSize}
   * @memberof EngineRead
   */
  engineSize: EngineSize;
  /**
   * If this catalog configuration's creation was unsuccessful, then this property describes the problem
   * @type {string}
   * @memberof EngineRead
   */
  errorMessage?: string;
  /**
   * Location for runtime logs created Dremio engines
   * @type {string}
   * @memberof EngineRead
   */
  logStorageLocation: string;
  /**
   *
   * @type {EngineStateRead}
   * @memberof EngineRead
   */
  state: EngineStateRead;
}
/**
 * Engine size used by this job
 * @export
 * @enum {string}
 */
export enum EngineSize {
  XXSMALLV1 = <any>"XX_SMALL_V1",
  XSMALLV1 = <any>"X_SMALL_V1",
  SMALLV1 = <any>"SMALL_V1",
  MEDIUMV1 = <any>"MEDIUM_V1",
  LARGEV1 = <any>"LARGE_V1",
  XLARGEV1 = <any>"X_LARGE_V1",
  XXLARGEV1 = <any>"XX_LARGE_V1",
  XXXLARGEV1 = <any>"XXX_LARGE_V1",
}
/**
 * State of this configuration
 * @export
 * @enum {string}
 */
export enum EngineStateRead {
  CREATING = <any>"CREATING",
  ENABLED = <any>"ENABLED",
  DISABLED = <any>"DISABLED",
  UPDATING = <any>"UPDATING",
  FAILED = <any>"FAILED",
}
/**
 *
 * @export
 * @interface EngineWrite
 */
export interface EngineWrite {
  /**
   * ID of the cloud in which to run jobs and schedules on this catalog
   * @type {string}
   * @memberof EngineWrite
   */
  cloudId: string;
  /**
   *
   * @type {ConfigCredentials}
   * @memberof EngineWrite
   */
  credentials: ConfigCredentials;
  /**
   *
   * @type {EngineSize}
   * @memberof EngineWrite
   */
  engineSize: EngineSize;
  /**
   * Location for runtime logs created by Dremio engines
   * @type {string}
   * @memberof EngineWrite
   */
  logStorageLocation: string;
}
/**
 *
 * @export
 * @interface ErrorResponse
 */
export interface ErrorResponse {
  /**
   *
   * @type {number}
   * @memberof ErrorResponse
   */
  code: number;
  /**
   *
   * @type {string}
   * @memberof ErrorResponse
   */
  id: string;
  /**
   *
   * @type {string}
   * @memberof ErrorResponse
   */
  message: string;
  /**
   *
   * @type {string}
   * @memberof ErrorResponse
   */
  reason: string;
}

/**
 *
 * @export
 * @interface IamRoleCredentials
 */
export interface IamRoleCredentials extends ConfigCredentials {
  /**
   *
   * @type {string}
   * @memberof IamRoleCredentials
   */
  externalId?: string;
  /**
   *
   * @type {string}
   * @memberof IamRoleCredentials
   */
  instanceProfileArn: string;
  /**
   *
   * @type {string}
   * @memberof IamRoleCredentials
   */
  roleArn: string;
}

/**
 * @export
 * @namespace IamRoleCredentials
 */
export namespace IamRoleCredentials {}
