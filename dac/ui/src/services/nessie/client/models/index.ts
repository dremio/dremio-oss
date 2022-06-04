/* tslint:disable */
/* eslint-disable */
/**
 * 
 * @export
 * @interface Branch
 */
export interface Branch {
    /**
     * 
     * @type {string}
     * @memberof Branch
     */
    name: string;
    /**
     * 
     * @type {string}
     * @memberof Branch
     */
    hash?: string;
    /**
     * 
     * @type {ReferenceMetadata}
     * @memberof Branch
     */
    metadata?: ReferenceMetadata;
}
/**
 * 
 * @export
 * @interface CommitMeta
 */
export interface CommitMeta {
    /**
     * 
     * @type {string}
     * @memberof CommitMeta
     */
    hash?: string;
    /**
     * 
     * @type {string}
     * @memberof CommitMeta
     */
    committer?: string;
    /**
     * 
     * @type {string}
     * @memberof CommitMeta
     */
    author?: string;
    /**
     * 
     * @type {string}
     * @memberof CommitMeta
     */
    signedOffBy?: string;
    /**
     * 
     * @type {string}
     * @memberof CommitMeta
     */
    message: string;
    /**
     * 
     * @type {Date}
     * @memberof CommitMeta
     */
    commitTime?: Date;
    /**
     * 
     * @type {Date}
     * @memberof CommitMeta
     */
    authorTime?: Date;
    /**
     * 
     * @type {{ [key: string]: string; }}
     * @memberof CommitMeta
     */
    properties: { [key: string]: string; };
}
/**
 * @type Content
 * 
 * @export
 */
export type Content = { type: 'DELTA_LAKE_TABLE' } & DeltaLakeTable | { type: 'ICEBERG_TABLE' } & IcebergTable | { type: 'VIEW' } & SqlView;
/**
 * 
 * @export
 * @interface ContentKey
 */
export interface ContentKey {
    /**
     * 
     * @type {Array<string>}
     * @memberof ContentKey
     */
    elements: Array<string>;
}
/**
 * 
 * @export
 * @interface ContentWithKey
 */
export interface ContentWithKey {
    /**
     * 
     * @type {ContentKey}
     * @memberof ContentWithKey
     */
    key: ContentKey | null;
    /**
     * 
     * @type {Content}
     * @memberof ContentWithKey
     */
    content: Content | null;
}
/**
 * 
 * @export
 * @interface Delete
 */
export interface Delete {
    /**
     * 
     * @type {ContentKey}
     * @memberof Delete
     */
    key: ContentKey | null;
}
/**
 * 
 * @export
 * @interface DeltaLakeTable
 */
export interface DeltaLakeTable {
    /**
     * 
     * @type {string}
     * @memberof DeltaLakeTable
     */
    id?: string;
    /**
     * 
     * @type {Array<string>}
     * @memberof DeltaLakeTable
     */
    metadataLocationHistory: Array<string>;
    /**
     * 
     * @type {Array<string>}
     * @memberof DeltaLakeTable
     */
    checkpointLocationHistory: Array<string>;
    /**
     * 
     * @type {string}
     * @memberof DeltaLakeTable
     */
    lastCheckpoint?: string;
}
/**
 * 
 * @export
 * @enum {string}
 */
export enum Dialect {
    Dremio = 'DREMIO',
    Hive = 'HIVE',
    Presto = 'PRESTO',
    Spark = 'SPARK'
}
/**
 * 
 * @export
 * @interface DiffEntry
 */
export interface DiffEntry {
    /**
     * 
     * @type {ContentKey}
     * @memberof DiffEntry
     */
    key?: ContentKey;
    /**
     * 
     * @type {Content}
     * @memberof DiffEntry
     */
    from?: Content;
    /**
     * 
     * @type {Content}
     * @memberof DiffEntry
     */
    to?: Content;
}
/**
 * 
 * @export
 * @interface DiffResponse
 */
export interface DiffResponse {
    /**
     * 
     * @type {Array<DiffEntry>}
     * @memberof DiffResponse
     */
    diffs?: Array<DiffEntry>;
}
/**
 * 
 * @export
 * @interface EntriesResponse
 */
export interface EntriesResponse {
    /**
     * 
     * @type {boolean}
     * @memberof EntriesResponse
     */
    hasMore?: boolean;
    /**
     * 
     * @type {string}
     * @memberof EntriesResponse
     */
    token?: string;
    /**
     * 
     * @type {Array<Entry>}
     * @memberof EntriesResponse
     */
    entries: Array<Entry>;
}
/**
 * 
 * @export
 * @interface Entry
 */
export interface Entry {
    /**
     * 
     * @type {Type}
     * @memberof Entry
     */
    type: Type | null;
    /**
     * 
     * @type {ContentKey}
     * @memberof Entry
     */
    name: ContentKey | null;
}
/**
 * 
 * @export
 * @enum {string}
 */
export enum FetchOption {
    All = 'ALL',
    Minimal = 'MINIMAL'
}
/**
 * 
 * @export
 * @interface GetMultipleContentsRequest
 */
export interface GetMultipleContentsRequest {
    /**
     * 
     * @type {Array<ContentKey>}
     * @memberof GetMultipleContentsRequest
     */
    requestedKeys: Array<ContentKey>;
}
/**
 * 
 * @export
 * @interface GetMultipleContentsResponse
 */
export interface GetMultipleContentsResponse {
    /**
     * 
     * @type {Array<ContentWithKey>}
     * @memberof GetMultipleContentsResponse
     */
    contents: Array<ContentWithKey>;
}
/**
 * Represents the global state of an Iceberg table in Nessie. An Iceberg table is globally identified via its unique 'Content.id'.
 * 
 * A Nessie commit-operation, performed via 'TreeApi.commitMultipleOperations', for Iceberg for Iceberg consists of a 'Operation.Put' with an 'IcebergTable' as in the 'content' field and the previous value of 'IcebergTable' in the 'expectedContent' field.
 * 
 * During a commit-operation, Nessie checks whether the known global state of the Iceberg table is compatible (think: equal) to 'Operation.Put.expectedContent'.
 * @export
 * @interface IcebergTable
 */
export interface IcebergTable {
    /**
     * 
     * @type {string}
     * @memberof IcebergTable
     */
    id?: string;
    /**
     * 
     * @type {string}
     * @memberof IcebergTable
     */
    metadataLocation: string;
    /**
     * 
     * @type {number}
     * @memberof IcebergTable
     */
    snapshotId?: number;
    /**
     * 
     * @type {number}
     * @memberof IcebergTable
     */
    schemaId?: number;
    /**
     * 
     * @type {number}
     * @memberof IcebergTable
     */
    specId?: number;
    /**
     * 
     * @type {number}
     * @memberof IcebergTable
     */
    sortOrderId?: number;
}
/**
 * 
 * @export
 * @interface LogEntry
 */
export interface LogEntry {
    /**
     * 
     * @type {CommitMeta}
     * @memberof LogEntry
     */
    commitMeta: CommitMeta | null;
    /**
     * 
     * @type {string}
     * @memberof LogEntry
     */
    parentCommitHash?: string;
    /**
     * 
     * @type {Array<Operation>}
     * @memberof LogEntry
     */
    operations?: Array<Operation>;
}
/**
 * 
 * @export
 * @interface LogResponse
 */
export interface LogResponse {
    /**
     * 
     * @type {boolean}
     * @memberof LogResponse
     */
    hasMore?: boolean;
    /**
     * 
     * @type {string}
     * @memberof LogResponse
     */
    token?: string;
    /**
     * 
     * @type {Array<LogEntry>}
     * @memberof LogResponse
     */
    logEntries: Array<LogEntry>;
}
/**
 * 
 * @export
 * @interface Merge
 */
export interface Merge {
    /**
     * 
     * @type {string}
     * @memberof Merge
     */
    fromRefName: string;
    /**
     * 
     * @type {string}
     * @memberof Merge
     */
    fromHash: string;
}
/**
 * 
 * @export
 * @interface NessieConfiguration
 */
export interface NessieConfiguration {
    /**
     * 
     * @type {string}
     * @memberof NessieConfiguration
     */
    defaultBranch?: string;
    /**
     * 
     * @type {number}
     * @memberof NessieConfiguration
     */
    maxSupportedApiVersion?: number;
}
/**
 * @type Operation
 * 
 * @export
 */
export type Operation = { type: 'DELETE' } & Delete | { type: 'PUT' } & Put | { type: 'UNCHANGED' } & Unchanged;
/**
 * 
 * @export
 * @interface Operations
 */
export interface Operations {
    /**
     * 
     * @type {CommitMeta}
     * @memberof Operations
     */
    commitMeta: CommitMeta | null;
    /**
     * 
     * @type {Array<Operation>}
     * @memberof Operations
     */
    operations: Array<Operation>;
}
/**
 * Add or replace (put) a 'Content' object for a 'ContentKey'. If the actual table type tracks the 'global state' of individual tables (Iceberg as of today), every 'Put'-operation must contain a non-null value for 'expectedContent'.
 * @export
 * @interface Put
 */
export interface Put {
    /**
     * 
     * @type {ContentKey}
     * @memberof Put
     */
    key: ContentKey | null;
    /**
     * 
     * @type {Content}
     * @memberof Put
     */
    content: Content | null;
    /**
     * 
     * @type {Content}
     * @memberof Put
     */
    expectedContent?: Content;
}
/**
 * 
 * @export
 * @interface RefLogResponse
 */
export interface RefLogResponse {
    /**
     * 
     * @type {boolean}
     * @memberof RefLogResponse
     */
    hasMore?: boolean;
    /**
     * 
     * @type {string}
     * @memberof RefLogResponse
     */
    token?: string;
    /**
     * 
     * @type {Array<RefLogResponseEntry>}
     * @memberof RefLogResponse
     */
    logEntries: Array<RefLogResponseEntry>;
}
/**
 * 
 * @export
 * @interface RefLogResponseEntry
 */
export interface RefLogResponseEntry {
    /**
     * 
     * @type {string}
     * @memberof RefLogResponseEntry
     */
    refLogId: string;
    /**
     * 
     * @type {string}
     * @memberof RefLogResponseEntry
     */
    refName: string;
    /**
     * 
     * @type {string}
     * @memberof RefLogResponseEntry
     */
    refType: string;
    /**
     * 
     * @type {string}
     * @memberof RefLogResponseEntry
     */
    commitHash: string;
    /**
     * 
     * @type {string}
     * @memberof RefLogResponseEntry
     */
    parentRefLogId: string;
    /**
     * 
     * @type {number}
     * @memberof RefLogResponseEntry
     */
    operationTime: number;
    /**
     * 
     * @type {string}
     * @memberof RefLogResponseEntry
     */
    operation: string;
    /**
     * 
     * @type {Array<string>}
     * @memberof RefLogResponseEntry
     */
    sourceHashes: Array<string>;
}
/**
 * @type Reference
 * 
 * @export
 */
export type Reference = { type: 'BRANCH' } & Branch | { type: 'TAG' } & Tag;
/**
 * Only returned by the server when explicitly requested by the client and contains the following information:
 * 
 * - numCommitsAhead (number of commits ahead of the default branch)
 * 
 * - numCommitsBehind (number of commits behind the default branch)
 * 
 * - commitMetaOfHEAD (the commit metadata of the HEAD commit)
 * 
 * - commonAncestorHash (the hash of the common ancestor in relation to the default branch).
 * 
 * - numTotalCommits (the total number of commits in this reference).
 * @export
 * @interface ReferenceMetadata
 */
export interface ReferenceMetadata {
    /**
     * 
     * @type {number}
     * @memberof ReferenceMetadata
     */
    numCommitsAhead?: number;
    /**
     * 
     * @type {number}
     * @memberof ReferenceMetadata
     */
    numCommitsBehind?: number;
    /**
     * 
     * @type {CommitMeta}
     * @memberof ReferenceMetadata
     */
    commitMetaOfHEAD?: CommitMeta;
    /**
     * 
     * @type {string}
     * @memberof ReferenceMetadata
     */
    commonAncestorHash?: string;
    /**
     * 
     * @type {number}
     * @memberof ReferenceMetadata
     */
    numTotalCommits?: number;
}
/**
 * 
 * @export
 * @interface ReferencesResponse
 */
export interface ReferencesResponse {
    /**
     * 
     * @type {boolean}
     * @memberof ReferencesResponse
     */
    hasMore?: boolean;
    /**
     * 
     * @type {string}
     * @memberof ReferencesResponse
     */
    token?: string;
    /**
     * 
     * @type {Array<Reference>}
     * @memberof ReferencesResponse
     */
    references: Array<Reference>;
}
/**
 * 
 * @export
 * @interface SqlView
 */
export interface SqlView {
    /**
     * 
     * @type {string}
     * @memberof SqlView
     */
    id?: string;
    /**
     * 
     * @type {string}
     * @memberof SqlView
     */
    sqlText: string;
    /**
     * 
     * @type {Dialect}
     * @memberof SqlView
     */
    dialect: Dialect | null;
}
/**
 * 
 * @export
 * @interface Tag
 */
export interface Tag {
    /**
     * 
     * @type {string}
     * @memberof Tag
     */
    name: string;
    /**
     * 
     * @type {string}
     * @memberof Tag
     */
    hash?: string;
    /**
     * 
     * @type {ReferenceMetadata}
     * @memberof Tag
     */
    metadata?: ReferenceMetadata;
}
/**
 * 
 * @export
 * @interface Transplant
 */
export interface Transplant {
    /**
     * 
     * @type {string}
     * @memberof Transplant
     */
    fromRefName: string;
    /**
     * 
     * @type {Set<string>}
     * @memberof Transplant
     */
    hashesToTransplant: Set<string>;
}
/**
 * 
 * @export
 * @enum {string}
 */
export enum Type {
    DeltaLakeTable = 'DELTA_LAKE_TABLE',
    IcebergTable = 'ICEBERG_TABLE',
    Unknown = 'UNKNOWN',
    View = 'VIEW'
}
/**
 * 
 * @export
 * @interface Unchanged
 */
export interface Unchanged {
    /**
     * 
     * @type {ContentKey}
     * @memberof Unchanged
     */
    key: ContentKey | null;
}
