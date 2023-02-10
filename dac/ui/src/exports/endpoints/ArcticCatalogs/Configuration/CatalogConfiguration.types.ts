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
/**
 *
 * @export
 * @interface AccessKeyCredentialsDef
 */
export interface AccessKeyCredentialsDef extends ConfigCredentialsDef {
  /**
   *
   * @type {string}
   * @memberof AccessKeyCredentialsDef
   */
  accessKeyId: string;
  /**
   *
   * @type {string}
   * @memberof AccessKeyCredentialsDef
   */
  secretAccessKey: string;
}

/**
 * @export
 * @namespace AccessKeyCredentialsDef
 */
// eslint-disable-next-line @typescript-eslint/no-namespace
export namespace AccessKeyCredentialsDef {}
/**
 *
 * @export
 * @interface AccessKeyCredentialsInfo
 */
export interface AccessKeyCredentialsInfo extends ConfigCredentialsInfo {
  /**
   *
   * @type {string}
   * @memberof AccessKeyCredentialsInfo
   */
  accessKeyId: string;
}

/**
 * @export
 * @namespace AccessKeyCredentialsInfo
 */
// eslint-disable-next-line @typescript-eslint/no-namespace
export namespace AccessKeyCredentialsInfo {}
/**
 *
 * @export
 * @interface ConfigCredentialsDef
 */
export interface ConfigCredentialsDef {
  /**
   *
   * @type {string}
   * @memberof ConfigCredentialsDef
   */
  type: ConfigCredentialsDef.TypeEnum;
}

/**
 * @export
 * @namespace ConfigCredentialsDef
 */
// eslint-disable-next-line @typescript-eslint/no-namespace
export namespace ConfigCredentialsDef {
  /**
   * @export
   * @enum {string}
   */
  export enum TypeEnum {
    KEYS = <any>"DATA_KEYS",
    ROLE = <any>"DATA_ROLE",
  }
}
/**
 *
 * @export
 * @interface ConfigCredentialsInfo
 */
export interface ConfigCredentialsInfo {
  /**
   *
   * @type {string}
   * @memberof ConfigCredentialsInfo
   */
  type: ConfigCredentialsInfo.TypeEnum;
}

/**
 * @export
 * @namespace ConfigCredentialsInfo
 */
// eslint-disable-next-line @typescript-eslint/no-namespace
export namespace ConfigCredentialsInfo {
  /**
   * @export
   * @enum {string}
   */
  export enum TypeEnum {
    KEYS = <any>"DATA_KEYS",
    ROLE = <any>"DATA_ROLE",
  }
}
/**
 *
 * @export
 * @interface ConfigDef
 */
export interface ConfigDef {
  /**
   *
   * @type {ConfigCredentialsDef}
   * @memberof ConfigDef
   */
  credentials: ConfigCredentialsDef;
  /**
   *
   * @type {string}
   * @memberof ConfigDef
   */
  cloudId: string;
  /**
   *
   * @type {string}
   * @memberof ConfigDef
   */
  engineSize: ConfigDef.EngineSizeEnum;
  /**
   *
   * @type {string}
   * @memberof ConfigDef
   */
  logStorageLocation: string;
  /**
   *
   * @type {string}
   * @memberof ConfigDef
   */
  state?: ConfigDef.StateEnum;
}

/**
 * @export
 * @namespace ConfigDef
 */
// eslint-disable-next-line @typescript-eslint/no-namespace
export namespace ConfigDef {
  /**
   * @export
   * @enum {string}
   */
  export enum EngineSizeEnum {
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
   * @export
   * @enum {string}
   */
  export enum StateEnum {
    ENABLED = <any>"ENABLED",
    DISABLED = <any>"DISABLED",
  }
}
/**
 *
 * @export
 * @interface ConfigInfo
 */
export interface ConfigInfo {
  /**
   *
   * @type {ConfigCredentialsInfo}
   * @memberof ConfigInfo
   */
  credentials: ConfigCredentialsInfo;
  /**
   *
   * @type {string}
   * @memberof ConfigInfo
   */
  errorMessage?: string;
  /**
   *
   * @type {string}
   * @memberof ConfigInfo
   */
  cloudId: string;
  /**
   *
   * @type {string}
   * @memberof ConfigInfo
   */
  engineSize: ConfigInfo.EngineSizeEnum;
  /**
   *
   * @type {string}
   * @memberof ConfigInfo
   */
  logStorageLocation: string;
  /**
   *
   * @type {string}
   * @memberof ConfigInfo
   */
  state: ConfigInfo.StateEnum;
}

/**
 * @export
 * @namespace ConfigInfo
 */
// eslint-disable-next-line @typescript-eslint/no-namespace
export namespace ConfigInfo {
  /**
   * @export
   * @enum {string}
   */
  export enum EngineSizeEnum {
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
   * @export
   * @enum {string}
   */
  export enum StateEnum {
    CREATING = <any>"CREATING",
    ENABLED = <any>"ENABLED",
    DISABLED = <any>"DISABLED",
    FAILED = <any>"FAILED",
  }
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
  reason: string;
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
  id: string;
}
/**
 *
 * @export
 * @interface IamRoleCredentials
 */
export interface IamRoleCredentials {
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
  roleArn: string;
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
  type: IamRoleCredentials.TypeEnum;
}

/**
 * @export
 * @namespace IamRoleCredentials
 */
// eslint-disable-next-line @typescript-eslint/no-namespace
export namespace IamRoleCredentials {
  /**
   * @export
   * @enum {string}
   */
  export enum TypeEnum {
    KEYS = <any>"DATA_KEYS",
    ROLE = <any>"DATA_ROLE",
  }
}
