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
 * The list of available privileges for Arctic Catalogs.
 * @export
 */
export type AvailablePrivileges = Array<Privilege>;
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
 * A set of privileges applied to a single grantee.
 * @export
 * @interface GrantForGet
 */
export interface GrantForGet {
  /**
   *
   * @type {GranteeId}
   * @memberof GrantForGet
   */
  granteeId: GranteeId;
  /**
   *
   * @type {Privileges}
   * @memberof GrantForGet
   */
  privileges: Privileges;
  /**
   *
   * @type {Name}
   * @memberof GrantForGet
   */
  name: Name;
  /**
   *
   * @type {GranteeType}
   * @memberof GrantForGet
   */
  granteeType: GranteeType;
}
/**
 * A set of privileges applied to a single grantee.
 * @export
 * @interface GrantForSet
 */
export interface GrantForSet {
  /**
   *
   * @type {GranteeId}
   * @memberof GrantForSet
   */
  granteeId: GranteeId;
  /**
   *
   * @type {Privileges}
   * @memberof GrantForSet
   */
  privileges: Privileges;
}
/**
 * The ID of the Grantee.
 * @export
 */
export type GranteeId = string;
/**
 * The Type of the Grantee. USER or ROLE.
 * @export
 * @enum {string}
 */
export enum GranteeType {
  USER = <any>"USER",
  ROLE = <any>"ROLE",
}
/**
 * The list of grants used to GET.
 * @export
 */
export type GrantsForGet = Array<GrantForGet>;
/**
 * The list of grants used to PUT.
 * @export
 */
export type GrantsForSet = Array<GrantForSet>;
/**
 *
 * @export
 * @interface InlineResponse200
 */
export interface InlineResponse200 {
  /**
   *
   * @type {Tag}
   * @memberof InlineResponse200
   */
  tag: Tag;
  /**
   *
   * @type {AvailablePrivileges}
   * @memberof InlineResponse200
   */
  availablePrivileges: AvailablePrivileges;
  /**
   *
   * @type {GranteeId}
   * @memberof InlineResponse200
   */
  owner: GranteeId;
  /**
   *
   * @type {GrantsForGet}
   * @memberof InlineResponse200
   */
  grants: GrantsForGet;
}
/**
 *
 * @export
 * @interface InlineResponse2001
 */
export interface InlineResponse2001 {
  /**
   *
   * @type {Tag}
   * @memberof InlineResponse2001
   */
  tag: Tag;
  /**
   *
   * @type {GrantsForSet}
   * @memberof InlineResponse2001
   */
  grants: GrantsForSet;
}
/**
 * The Username or name of Role of the Grantee.
 * @export
 */
export type Name = string;
/**
 * Name of the Arctic Catalog Privilege.
 * @export
 * @enum {string}
 */
export enum Privilege {
  USAGE = <any>"USAGE",
  COMMIT = <any>"COMMIT",
  CREATEBRANCH = <any>"CREATE_BRANCH",
  CREATETAG = <any>"CREATE_TAG",
  WRITESETTINGS = <any>"WRITE_SETTINGS",
  READSETTINGS = <any>"READ_SETTINGS",
  MANAGEGRANTS = <any>"MANAGE_GRANTS",
}
/**
 * The list of Arctic Catalog Privileges.
 * @export
 */
export type Privileges = Array<Privilege>;
/**
 * Version tag is required when overwriting existing grant. Version tag can be fetched using GET API.
 * @export
 */
export type Tag = string;
/**
 * CatalogGrantsApi - fetch parameter creator
 * @export
 */
