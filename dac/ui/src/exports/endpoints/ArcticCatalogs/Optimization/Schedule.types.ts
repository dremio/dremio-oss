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
 * Configuration of OPTIMIZE jobs started by this schedule
 * @export
 * @interface OptimizeConfigDef
 */
/**
 * Configuration of OPTIMIZE jobs started by this schedule
 * @export
 * @interface OptimizeConfigInfo
 */
export interface OptimizeConfigInfo {
  /**
   * Maximum file size threshold for OPTIMIZE.  Data size in the form \"`integer` `unit`\", where the case-insensitive unit may be B, KB, MB, or GB.
   * @type {string}
   * @memberof OptimizeConfigInfo
   */
  maxFileSize: string;
  /**
   * Minimum file size threshold for OPTIMIZE.  Data size in the form \"`integer` `unit`\", where the case-insensitive unit may be B, KB, MB, or GB.
   * @type {string}
   * @memberof OptimizeConfigInfo
   */
  minFileSize: string;
  /**
   *
   * @type {number}
   * @memberof OptimizeConfigInfo
   */
  minFiles: number;
  /**
   * Reference within which to resolve the table ID
   * @type {string}
   * @memberof OptimizeConfigInfo
   */
  reference: string;
  /**
   * Table name preceded by zero or more namespaces, with `.` as the separator
   * @type {string}
   * @memberof OptimizeConfigInfo
   */
  tableId: string;
  /**
   * Target file size for OPTIMIZE.  Data size in the form \"`integer` `unit`\", where the case-insensitive unit may be B, KB, MB, or GB.
   * @type {string}
   * @memberof OptimizeConfigInfo
   */
  targetFileSize: string;
}

/**
 *
 * @export
 * @interface JobScheduleInfo
 */
export interface JobScheduleInfo {
  /**
   * Server-assigned opaque schedule ID
   * @type {string}
   * @memberof JobScheduleInfo
   */
  id: string;
  /**
   * Cron expression, or one of the presets @hourly|@daily|@weekly|@monthly|@yearly
   * @type {string}
   * @memberof JobScheduleInfo
   */
  schedule: string;
  /**
   * Type of job to be run by this schedule
   * @type {string}
   * @memberof JobScheduleInfo
   */
  type: JobScheduleInfo.TypeEnum;
  /**
   * User who created this schedule
   * @type {string}
   * @memberof JobScheduleInfo
   */
  user: string;
}

/**
 * @export
 * @namespace JobScheduleInfo
 */
// eslint-disable-next-line @typescript-eslint/no-namespace
export namespace JobScheduleInfo {
  /**
   * @export
   * @enum {string}
   */
  export enum TypeEnum {
    OPTIMIZE = <any>"OPTIMIZE",
    VACUUM = <any>"VACUUM",
  }
}
export interface OptimizeConfigDef {
  /**
   * Maximum file size threshold for OPTIMIZE.  Data size in the form \"`integer` `unit`\", where the case-insensitive unit may be B, KB, MB, or GB.
   * @type {string}
   * @memberof OptimizeConfigDef
   */
  maxFileSize?: string;
  /**
   * Minimum file size threshold for OPTIMIZE.  Data size in the form \"`integer` `unit`\", where the case-insensitive unit may be B, KB, MB, or GB.
   * @type {string}
   * @memberof OptimizeConfigDef
   */
  minFileSize?: string;
  /**
   *
   * @type {number}
   * @memberof OptimizeConfigDef
   */
  minFiles?: number;
  /**
   * Reference within which to resolve the table ID
   * @type {string}
   * @memberof OptimizeConfigDef
   */
  reference: string;
  /**
   * Table name preceded by zero or more namespaces, with `.` as the separator
   * @type {string}
   * @memberof OptimizeConfigDef
   */
  tableId: string;
  /**
   * Target file size for OPTIMIZE.  Data size in the form \"`integer` `unit`\", where the case-insensitive unit may be B, KB, MB, or GB.
   * @type {string}
   * @memberof OptimizeConfigDef
   */
  targetFileSize?: string;
}

/**
 *
 * @export
 * @interface ScheduleDef
 */
export interface ScheduleDef {
  /**
   * Cron expression, or one of the presets @hourly|@daily|@weekly|@monthly|@yearly
   * @type {string}
   * @memberof ScheduleDef
   */
  schedule: string;
  /**
   * Type of job to be run by this schedule
   * @type {string}
   * @memberof ScheduleDef
   */
  type: ScheduleDef.TypeEnum;
}

/**
 * @export
 * @namespace ScheduleDef
 */
// eslint-disable-next-line @typescript-eslint/no-namespace
export namespace ScheduleDef {
  /**
   * @export
   * @enum {string}
   */
  export enum TypeEnum {
    OPTIMIZE = <any>"OPTIMIZE",
    VACUUM = <any>"VACUUM",
  }
}

/**
 *
 * @export
 * @interface OptimizeScheduleDef
 */
export interface OptimizeScheduleDef extends ScheduleDef {
  /**
   *
   * @type {OptimizeConfigDef}
   * @memberof OptimizeScheduleDef
   */
  config: OptimizeConfigDef;
}

/**
 *
 * @export
 * @interface OptimizeScheduleInfo
 */
export interface OptimizeScheduleInfo extends JobScheduleInfo {
  /**
   *
   * @type {OptimizeConfigInfo}
   * @memberof OptimizeScheduleInfo
   */
  config: OptimizeConfigInfo;
}

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
 * @interface ScheduleInfoPage
 */
export interface ScheduleInfoPage {
  /**
   *
   * @type {Array<JobScheduleInfo>}
   * @memberof ScheduleInfoPage
   */
  data: Array<JobScheduleInfo>;
  /**
   * Opaque pagination token
   * @type {string}
   * @memberof ScheduleInfoPage
   */
  nextPageToken?: string;
}

export interface OptimizeJobDef extends JobDef {
  /**
   *
   * @type {OptimizeConfigDef}
   * @memberof OptimizeJobDef
   */
  config: OptimizeConfigDef;
}

/**
 * @export
 * @namespace OptimizeJobDef
 */

/**
 *
 * @export
 * @interface JobDef
 */
export interface JobDef {
  /**
   *
   * @type {string}
   * @memberof JobDef
   */
  type: JobDef.TypeEnum;
}

/**
 * @export
 * @namespace JobDef
 */

// eslint-disable-next-line @typescript-eslint/no-namespace
export namespace JobDef {
  /**
   * @export
   * @enum {string}
   */
  export enum TypeEnum {
    OPTIMIZE = <any>"OPTIMIZE",
    VACUUM = <any>"VACUUM",
  }
}
