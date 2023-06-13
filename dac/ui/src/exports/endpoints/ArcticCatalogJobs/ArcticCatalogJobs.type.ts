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

export type ArcticCatalogJobsResponse = FullJobInfos | SummaryJobInfos;
export type GenericArcticCatalogJob =
  | VacuumJobInfo
  | OptimizeJobInfo
  | SummaryJobInfo;

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
 *
 * @export
 * @interface FullJobInfos
 */
export interface FullJobInfos {
  /**
   *
   * @type {Array<JobInfo>}
   * @memberof FullJobInfos
   */
  data: Array<JobInfo>;
  /**
   * Opaque pagination token
   * @type {string}
   * @memberof FullJobInfos
   */
  nextPageToken?: string;
}

/**
 *
 * @export
 * @interface JobDefinition
 */
export interface JobDefinition {
  /**
   *
   * @type {string}
   * @memberof JobDefinition
   */
  type: JobDefinitionTypeEnum;
}

/**
 * @export
 * @enum {string}
 */
export enum JobDefinitionTypeEnum {
  OPTIMIZE = <any>"OPTIMIZE",
  VACUUM = <any>"VACUUM",
}

/**
 *
 * @export
 * @interface JobInfo
 */
export interface JobInfo {
  /**
   *
   * @type {string}
   * @memberof JobInfo
   */
  catalogId: string;
  /**
   * End time of job (if present)
   * @type {Date}
   * @memberof JobInfo
   */
  endedAt?: Date;
  /**
   *
   * @type {EngineSize}
   * @memberof JobInfo
   */
  engineSize?: EngineSize;
  /**
   * If this job was unsuccessful, then this property describes the problem
   * @type {string}
   * @memberof JobInfo
   */
  errorMessage?: string;
  /**
   * This job's server-assigned opaque ID
   * @type {string}
   * @memberof JobInfo
   */
  id: string;
  /**
   * The server-assigned opaque ID of the schedule which created this job, if applicable.  For jobs started via a createJob operation instead of a schedule, this is null or absent.
   * @type {string}
   * @memberof JobInfo
   */
  scheduleId?: string;
  /**
   * Start time of job (if present)
   * @type {Date}
   * @memberof JobInfo
   */
  startedAt?: Date;
  /**
   *
   * @type {JobState}
   * @memberof JobInfo
   */
  state: JobState;
  /**
   *
   * @type {string}
   * @memberof JobInfo
   */
  type: JobInfoTypeEnum;
  /**
   * Username associated with this job.  The association depends on how the job was started.  - via schedule: the schedule's creator - via createJob: the user who made the createJob request
   * @type {string}
   * @memberof JobInfo
   */
  username: string;
}

/**
 * @export
 * @enum {string}
 */
export enum JobInfoTypeEnum {
  OPTIMIZE = <any>"OPTIMIZE",
  VACUUM = <any>"VACUUM",
}

/**
 *
 * @export
 * @enum {string}
 */
export enum JobState {
  SETUP = <any>"SETUP",
  QUEUED = <any>"QUEUED",
  STARTING = <any>"STARTING",
  RUNNING = <any>"RUNNING",
  COMPLETED = <any>"COMPLETED",
  CANCELLED = <any>"CANCELLED",
  FAILED = <any>"FAILED",
}
/**
 * Configuration of OPTIMIZE jobs started by this schedule
 * @export
 * @interface OptimizeConfigDefinition
 */
export interface OptimizeConfigDefinition {
  /**
   * Maximum file size threshold for OPTIMIZE.  Data size in the form \"`integer` MB\", where MB represents 2^20 bytes, and `integer` is a non-negative number.
   * @type {string}
   * @memberof OptimizeConfigDefinition
   */
  maxFileSize?: string;
  /**
   * Minimum file size threshold for OPTIMIZE.  Data size in the form \"`integer` MB\", where MB represents 2^20 bytes, and `integer` is a non-negative number.
   * @type {string}
   * @memberof OptimizeConfigDefinition
   */
  minFileSize?: string;
  /**
   *
   * @type {number}
   * @memberof OptimizeConfigDefinition
   */
  minFiles?: number;
  /**
   * Reference within which to resolve the table ID
   * @type {string}
   * @memberof OptimizeConfigDefinition
   */
  reference: string;
  /**
   * Table name preceded by zero or more namespaces, with `.` as the separator
   * @type {string}
   * @memberof OptimizeConfigDefinition
   */
  tableId: string;
  /**
   * Target file size for OPTIMIZE.  Data size in the form \"`integer` MB\", where MB represents 2^20 bytes, and `integer` is a non-negative number.
   * @type {string}
   * @memberof OptimizeConfigDefinition
   */
  targetFileSize?: string;
}
/**
 * Configuration of OPTIMIZE jobs started by this schedule
 * @export
 * @interface OptimizeConfigInfo
 */
export interface OptimizeConfigInfo {
  /**
   * Maximum file size threshold for OPTIMIZE.  Data size in the form \"`integer` MB\", where MB represents 2^20 bytes, and `integer` is a non-negative number.
   * @type {string}
   * @memberof OptimizeConfigInfo
   */
  maxFileSize: string;
  /**
   * Minimum file size threshold for OPTIMIZE.  Data size in the form \"`integer` MB\", where MB represents 2^20 bytes, and `integer` is a non-negative number.
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
   * Target file size for OPTIMIZE.  Data size in the form \"`integer` MB\", where MB represents 2^20 bytes, and `integer` is a non-negative number.
   * @type {string}
   * @memberof OptimizeConfigInfo
   */
  targetFileSize: string;
}
/**
 *
 * @export
 * @interface OptimizeJobDefinition
 */
export interface OptimizeJobDefinition extends JobDefinition {
  /**
   *
   * @type {OptimizeConfigDefinition}
   * @memberof OptimizeJobDefinition
   */
  config: OptimizeConfigDefinition;
}

/**
 *
 * @export
 * @interface OptimizeJobInfo
 */
export interface OptimizeJobInfo extends JobInfo {
  /**
   *
   * @type {OptimizeConfigInfo}
   * @memberof OptimizeJobInfo
   */
  config: OptimizeConfigInfo;
  /**
   *
   * @type {OptimizeJobMetrics}
   * @memberof OptimizeJobInfo
   */
  metrics?: OptimizeJobMetrics;
}

/**
 *
 * @export
 * @interface OptimizeJobMetrics
 */
export interface OptimizeJobMetrics {
  /**
   *
   * @type {number}
   * @memberof OptimizeJobMetrics
   */
  newDataFiles: number;
  /**
   *
   * @type {number}
   * @memberof OptimizeJobMetrics
   */
  rewrittenDataFiles: number;
}
/**
 *
 * @export
 * @interface OptimizeScheduleDefinition
 */
export interface OptimizeScheduleDefinition extends ScheduleDefinition {
  /**
   *
   * @type {OptimizeConfigDefinition}
   * @memberof OptimizeScheduleDefinition
   */
  config: OptimizeConfigDefinition;
}

/**
 *
 * @export
 * @interface OptimizeScheduleInfo
 */
export interface OptimizeScheduleInfo extends ScheduleInfo {
  /**
   *
   * @type {OptimizeConfigInfo}
   * @memberof OptimizeScheduleInfo
   */
  config: OptimizeConfigInfo;
}

/**
 *
 * @export
 * @interface ScheduleDefinition
 */
export interface ScheduleDefinition {
  /**
   * Cron expression, or one of the presets @hourly|@daily|@weekly|@monthly|@yearly
   * @type {string}
   * @memberof ScheduleDefinition
   */
  schedule: string;
  /**
   * Type of job to be run by this schedule
   * @type {string}
   * @memberof ScheduleDefinition
   */
  type: ScheduleDefinitionTypeEnum;
}

/**
 * @export
 * @enum {string}
 */
export enum ScheduleDefinitionTypeEnum {
  OPTIMIZE = <any>"OPTIMIZE",
  VACUUM = <any>"VACUUM",
}

/**
 *
 * @export
 * @interface ScheduleInfo
 */
export interface ScheduleInfo {
  /**
   * Server-assigned opaque schedule ID
   * @type {string}
   * @memberof ScheduleInfo
   */
  id: string;
  /**
   * Cron expression, or one of the presets @hourly|@daily|@weekly|@monthly|@yearly
   * @type {string}
   * @memberof ScheduleInfo
   */
  schedule: string;
  /**
   * Type of job to be run by this schedule
   * @type {string}
   * @memberof ScheduleInfo
   */
  type: ScheduleInfoTypeEnum;
  /**
   * User who created this schedule
   * @type {string}
   * @memberof ScheduleInfo
   */
  username: string;
}

/**
 * @export
 * @enum {string}
 */
export enum ScheduleInfoTypeEnum {
  OPTIMIZE = <any>"OPTIMIZE",
  VACUUM = <any>"VACUUM",
}

/**
 *
 * @export
 * @interface ScheduleInfos
 */
export interface ScheduleInfos {
  /**
   *
   * @type {Array<ScheduleInfo>}
   * @memberof ScheduleInfos
   */
  data: Array<ScheduleInfo>;
  /**
   * Opaque pagination token
   * @type {string}
   * @memberof ScheduleInfos
   */
  nextPageToken?: string;
}
/**
 *
 * @export
 * @interface SummaryJobInfo
 */
export interface SummaryJobInfo {
  /**
   *
   * @type {string}
   * @memberof SummaryJobInfo
   */
  catalogId: string;
  /**
   * End time of job (if present)
   * @type {Date}
   * @memberof SummaryJobInfo
   */
  endedAt?: Date;
  /**
   *
   * @type {EngineSize}
   * @memberof SummaryJobInfo
   */
  engineSize?: EngineSize;
  /**
   * If this job was unsuccessful, then this property describes the problem
   * @type {string}
   * @memberof SummaryJobInfo
   */
  errorMessage?: string;
  /**
   * This job's server-assigned opaque ID
   * @type {string}
   * @memberof SummaryJobInfo
   */
  id: string;
  /**
   *
   * @type {string}
   * @memberof SummaryJobInfo
   */
  scheduleId?: string;
  /**
   * Start time of job (if present)
   * @type {Date}
   * @memberof SummaryJobInfo
   */
  startedAt?: Date;
  /**
   *
   * @type {JobState}
   * @memberof SummaryJobInfo
   */
  state: JobState;
  /**
   *
   * @type {string}
   * @memberof SummaryJobInfo
   */
  type: SummaryJobInfoTypeEnum;
  /**
   * User associated with this job.The association depends on how the job was started.  - via schedule: the schedule's creator - via createJob: the user who made the createJob request
   * @type {string}
   * @memberof SummaryJobInfo
   */
  username: string;
}

/**
 * @export
 * @enum {string}
 */
export enum SummaryJobInfoTypeEnum {
  OPTIMIZE = <any>"OPTIMIZE",
  VACUUM = <any>"VACUUM",
}

/**
 *
 * @export
 * @interface SummaryJobInfos
 */
export interface SummaryJobInfos {
  /**
   *
   * @type {Array<SummaryJobInfo>}
   * @memberof SummaryJobInfos
   */
  data: Array<SummaryJobInfo>;
  /**
   * Opaque pagination token
   * @type {string}
   * @memberof SummaryJobInfos
   */
  nextPageToken?: string;
}
/**
 * Configuration of VACUUM jobs started by this schedule
 * @export
 * @interface VacuumConfig
 */
export interface VacuumConfig {
  /**
   *
   * @type {number}
   * @memberof VacuumConfig
   */
  retentionPeriodMinutes: number;
}
/**
 *
 * @export
 * @interface VacuumJobDefinition
 */
export interface VacuumJobDefinition extends JobDefinition {
  /**
   *
   * @type {VacuumConfig}
   * @memberof VacuumJobDefinition
   */
  config: VacuumConfig;
}

/**
 *
 * @export
 * @interface VacuumJobInfo
 */
export interface VacuumJobInfo extends JobInfo {
  /**
   *
   * @type {VacuumConfig}
   * @memberof VacuumJobInfo
   */
  config: VacuumConfig;
  /**
   *
   * @type {VacuumJobMetrics}
   * @memberof VacuumJobInfo
   */
  metrics?: VacuumJobMetrics;
}

/**
 *
 * @export
 * @interface VacuumJobMetrics
 */
export interface VacuumJobMetrics {
  /**
   *
   * @type {number}
   * @memberof VacuumJobMetrics
   */
  deletedDataFiles: number;
}
/**
 *
 * @export
 * @interface VacuumScheduleDefinition
 */
export interface VacuumScheduleDefinition extends ScheduleDefinition {
  /**
   *
   * @type {VacuumConfig}
   * @memberof VacuumScheduleDefinition
   */
  config: VacuumConfig;
}

/**
 *
 * @export
 * @interface VacuumScheduleInfo
 */
export interface VacuumScheduleInfo extends ScheduleInfo {
  /**
   *
   * @type {VacuumConfig}
   * @memberof VacuumScheduleInfo
   */
  config: VacuumConfig;
}
