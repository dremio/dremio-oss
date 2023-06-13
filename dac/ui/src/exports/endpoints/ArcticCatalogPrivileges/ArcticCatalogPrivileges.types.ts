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

export interface GetResponse {
  /**
   * can view the catalog, user must has USAGE privilege or is the owner/admin
   * @type {boolean}
   * @memberof GetResponse
   */
  canView?: boolean;
  /**
   * can manage the privilege page, user must has MANAGE_GRANT privilege or is the owner/admin
   * @type {boolean}
   * @memberof GetResponse
   */
  canManage?: boolean;
  /**
   * can delete the catalog, user must be the owner of the catalog
   * @type {boolean}
   * @memberof GetResponse
   */
  canDelete?: boolean;
  /**
   *
   * @type {GetResponseOptimization}
   * @memberof GetResponse
   */
  optimization?: GetResponseOptimization;
  /**
   *
   * @type {GetResponseBranch}
   * @memberof GetResponse
   */
  branch?: GetResponseBranch;
  /**
   *
   * @type {GetResponseTag}
   * @memberof GetResponse
   */
  tag?: GetResponseTag;
}
/**
 *
 * @export
 * @interface GetResponseBranch
 */
export interface GetResponseBranch {
  /**
   * have CREATE_BRANCH privilege
   * @type {boolean}
   * @memberof GetResponseBranch
   */
  canCreate?: boolean;
}
/**
 *
 * @export
 * @interface GetResponseOptimization
 */
export interface GetResponseOptimization {
  /**
   * can view the optimzation settings page
   * @type {boolean}
   * @memberof GetResponseOptimization
   */
  canView?: boolean;
  /**
   * can edit the optimzation settings page
   * @type {boolean}
   * @memberof GetResponseOptimization
   */
  canEdit?: boolean;
}
/**
 *
 * @export
 * @interface GetResponseTag
 */
export interface GetResponseTag {
  /**
   * have CREATE_TAG privilege
   * @type {boolean}
   * @memberof GetResponseTag
   */
  canCreate?: boolean;
}
