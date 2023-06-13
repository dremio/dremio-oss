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
 * The list of available privileges for the entity type.
 * @export
 */
export type AvailablePrivileges = Array<Privilege>;
/**
 * A set of privileges applied to a single grantee.
 * @export
 * @interface Grant
 */
export interface Grant {
  /**
   * The latest version tag is required when overwriting existing grant. The latest version tag can be fetched using the GET API.
   * @type {string}
   * @memberof Grant
   */
  tag?: string;
  /**
   *
   * @type {GranteeType}
   * @memberof Grant
   */
  granteeType: GranteeType;
  /**
   *
   * @type {GrantType}
   * @memberof Grant
   */
  grantType: GrantType;
  /**
   *
   * @type {PrincipalId}
   * @memberof Grant
   */
  granteeId: PrincipalId;
  /**
   *
   * @type {Privileges}
   * @memberof Grant
   */
  privileges: Privileges;
  /**
   *
   * @type {string}
   * @memberof Grant
   */
  name: string;
}
/**
 * The entity that the grant should have effects on.
 * @export
 * @enum {string}
 */
export enum GrantType {
  ORG = <any>"ORG",
  PROJECT = <any>"PROJECT",
  CLOUD = <any>"CLOUD",
  ENGINE = <any>"ENGINE",
  IDENTITYPROVIDER = <any>"IDENTITY_PROVIDER",
  OAUTHAPPLICATION = <any>"OAUTH_APPLICATION",
  EXTERNALTOKENSPROVIDER = <any>"EXTERNAL_TOKENS_PROVIDER",
  SCRIPT = <any>"SCRIPT",
  ARCTICCATALOG = <any>"ARCTIC_CATALOG",
}
/**
 * The grantee type of this grant. It can be either USER or ROLE.
 * @export
 * @enum {string}
 */
export enum GranteeType {
  USER = <any>"USER",
  ROLE = <any>"ROLE",
}
/**
 * The list of grants.
 * @export
 */
export type Grants = Array<Grant>;
/**
 * Response for getting and setting grants
 * @export
 * @interface GrantsResponse
 */
export interface GrantsResponse {
  /**
   *
   * @type {AvailablePrivileges}
   * @memberof GrantsResponse
   */
  availablePrivileges?: AvailablePrivileges;
  /**
   *
   * @type {PrincipalId}
   * @memberof GrantsResponse
   */
  owner: PrincipalId;
  /**
   * array of Grants
   * @type {Array<OutputGrants>}
   * @memberof GrantsResponse
   */
  grants: OutputGrants;
  /**
   * number of total grants returned
   * @type {number}
   * @memberof GrantsResponse
   */
  totalResults: number;
}
/**
 * A set of privileges applied to a single grantee returned from backend.
 * @export
 * @interface OutputGrant
 */
export interface OutputGrant {
  /**
   * The user email of the grantee
   * @type {string}
   * @memberof OutputGrant
   */
  name?: string;
  /**
   *
   * @type {GranteeType}
   * @memberof OutputGrant
   */
  granteeType?: GranteeType;
  /**
   *
   * @type {GrantType}
   * @memberof OutputGrant
   */
  grantType?: GrantType;
  /**
   *
   * @type {PrincipalId}
   * @memberof OutputGrant
   */
  granteeId: PrincipalId;
  /**
   *
   * @type {Privileges}
   * @memberof OutputGrant
   */
  privileges: Privileges;
  /**
   * The latest version tag is required when overwriting existing grant. The latest version tag can be fetched using the GET API.
   * @type {string}
   * @memberof OutputGrant
   */
  tag?: string;
}
/**
 * The list of grants.
 * @export
 */
export type OutputGrants = Array<OutputGrant>;
/**
 * represent for user Id
 * @export
 */
export type PrincipalId = string;
/**
 * The name of a Privilege.
 * @export
 * @enum {string}
 */
export enum Privilege {
  CREATECLOUD = <any>"CREATE_CLOUD",
  CREATEUSER = <any>"CREATE_USER",
  CREATEROLE = <any>"CREATE_ROLE",
  CREATEPROJECT = <any>"CREATE_PROJECT",
  MANAGEGRANTS = <any>"MANAGE_GRANTS",
  CREATECATALOG = <any>"CREATE_CATALOG",
  CREATE_OAUTH_APPLICATION = <any>"CREATE_OAUTH_APPLICATION",
  CREATE_EXTERNAL_TOKENS_PROVIDER = <any>"CREATE_EXTERNAL_TOKENS_PROVIDER",
  CREATE_IDENTITY_PROVIDER = <any>"CREATE_IDENTITY_PROVIDER",
  CONFIGURE_SECURITY = <any>"CONFIGURE_SECURITY",
}
/**
 * The list of privileges.
 * @export
 */
export type Privileges = Array<Privilege>;
/**
 * Request for setting grants
 * @export
 * @interface SetGrantsRequest
 */
export interface SetGrantsRequest {
  /**
   *
   * @type {Grants}
   * @memberof SetGrantsRequest
   */
  grants?: Grants;
}
