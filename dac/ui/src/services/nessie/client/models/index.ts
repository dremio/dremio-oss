/* tslint:disable */

/**
 *
 * @export
 * @interface AddedContent
 */
export interface AddedContent {
  /**
   *
   * @type {ContentKey}
   * @memberof AddedContent
   */
  key: ContentKey;
}
/**
 *
 * @export
 * @interface AddedContentV2
 */
export interface AddedContentV2 {
  /**
   *
   * @type {ContentKeyV2}
   * @memberof AddedContentV2
   */
  key: ContentKeyV2;
}
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
   * @type {ReferenceMetadata}
   * @memberof Branch
   */
  metadata?: ReferenceMetadata;
  /**
   *
   * @type {string}
   * @memberof Branch
   */
  hash?: string;
}
/**
 *
 * @export
 * @interface BranchV1
 */
export interface BranchV1 {
  /**
   *
   * @type {string}
   * @memberof BranchV1
   */
  name: string;
  /**
   *
   * @type {ReferenceMetadataV1}
   * @memberof BranchV1
   */
  metadata?: ReferenceMetadataV1;
  /**
   *
   * @type {string}
   * @memberof BranchV1
   */
  hash?: string;
}
/**
 *
 * @export
 * @interface BranchV2
 */
export interface BranchV2 {
  /**
   *
   * @type {string}
   * @memberof BranchV2
   */
  name: string;
  /**
   *
   * @type {ReferenceMetadataV2}
   * @memberof BranchV2
   */
  metadata?: ReferenceMetadataV2;
  /**
   *
   * @type {string}
   * @memberof BranchV2
   */
  hash?: string;
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
   * @type {Array<string>}
   * @memberof CommitMeta
   */
  authors: Array<string>;
  /**
   *
   * @type {string}
   * @memberof CommitMeta
   */
  signedOffBy?: string;
  /**
   *
   * @type {Array<string>}
   * @memberof CommitMeta
   */
  allSignedOffBy: Array<string>;
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
  properties: { [key: string]: string };
  /**
   *
   * @type {{ [key: string]: Array<string>; }}
   * @memberof CommitMeta
   */
  allProperties: { [key: string]: Array<string> };
  /**
   *
   * @type {Array<string>}
   * @memberof CommitMeta
   */
  parentCommitHashes: Array<string>;
}
/**
 *
 * @export
 * @interface CommitMetaV1
 */
export interface CommitMetaV1 {
  /**
   *
   * @type {string}
   * @memberof CommitMetaV1
   */
  hash?: string;
  /**
   *
   * @type {string}
   * @memberof CommitMetaV1
   */
  committer?: string;
  /**
   *
   * @type {string}
   * @memberof CommitMetaV1
   */
  author?: string;
  /**
   *
   * @type {string}
   * @memberof CommitMetaV1
   */
  signedOffBy?: string;
  /**
   *
   * @type {string}
   * @memberof CommitMetaV1
   */
  message: string;
  /**
   *
   * @type {Date}
   * @memberof CommitMetaV1
   */
  commitTime?: Date;
  /**
   *
   * @type {Date}
   * @memberof CommitMetaV1
   */
  authorTime?: Date;
  /**
   *
   * @type {{ [key: string]: string; }}
   * @memberof CommitMetaV1
   */
  properties: { [key: string]: string };
}
/**
 *
 * @export
 * @interface CommitMetaV2
 */
export interface CommitMetaV2 {
  /**
   *
   * @type {string}
   * @memberof CommitMetaV2
   */
  hash?: string;
  /**
   *
   * @type {string}
   * @memberof CommitMetaV2
   */
  committer?: string;
  /**
   *
   * @type {Array<string>}
   * @memberof CommitMetaV2
   */
  authors: Array<string>;
  /**
   *
   * @type {Array<string>}
   * @memberof CommitMetaV2
   */
  allSignedOffBy: Array<string>;
  /**
   *
   * @type {string}
   * @memberof CommitMetaV2
   */
  message: string;
  /**
   *
   * @type {Date}
   * @memberof CommitMetaV2
   */
  commitTime?: Date;
  /**
   *
   * @type {Date}
   * @memberof CommitMetaV2
   */
  authorTime?: Date;
  /**
   *
   * @type {{ [key: string]: Array<string>; }}
   * @memberof CommitMetaV2
   */
  allProperties: { [key: string]: Array<string> };
  /**
   *
   * @type {Array<string>}
   * @memberof CommitMetaV2
   */
  parentCommitHashes: Array<string>;
}
/**
 *
 * @export
 * @interface CommitResponse
 */
export interface CommitResponse {
  /**
   *
   * @type {Branch}
   * @memberof CommitResponse
   */
  targetBranch: Branch;
  /**
   *
   * @type {Array<AddedContent>}
   * @memberof CommitResponse
   */
  addedContents?: Array<AddedContent>;
}
/**
 *
 * @export
 * @interface CommitResponseV2
 */
export interface CommitResponseV2 {
  /**
   *
   * @type {BranchV2}
   * @memberof CommitResponseV2
   */
  targetBranch: BranchV2;
  /**
   *
   * @type {Array<AddedContentV2>}
   * @memberof CommitResponseV2
   */
  addedContents?: Array<AddedContentV2>;
}
/**
 * @type Content
 *
 * @export
 */
export type Content =
  | ({ type: "DELTA_LAKE_TABLE" } & DeltaLakeTable)
  | ({ type: "ICEBERG_TABLE" } & IcebergTable)
  | ({ type: "ICEBERG_VIEW" } & IcebergView)
  | ({ type: "NAMESPACE" } & Namespace);
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
 * @enum {string}
 */
export enum ContentKeyConflict {
  None = "NONE",
  Unresolvable = "UNRESOLVABLE",
}
/**
 *
 * @export
 * @enum {string}
 */
export enum ContentKeyConflictV1 {
  None = "NONE",
  Unresolvable = "UNRESOLVABLE",
}
/**
 *
 * @export
 * @enum {string}
 */
export enum ContentKeyConflictV2 {
  None = "NONE",
  Unresolvable = "UNRESOLVABLE",
}
/**
 *
 * @export
 * @interface ContentKeyDetails
 */
export interface ContentKeyDetails {
  /**
   *
   * @type {ContentKey}
   * @memberof ContentKeyDetails
   */
  key?: ContentKey;
  /**
   *
   * @type {MergeBehavior}
   * @memberof ContentKeyDetails
   */
  mergeBehavior?: MergeBehavior;
  /**
   *
   * @type {ContentKeyConflict}
   * @memberof ContentKeyDetails
   */
  conflictType?: ContentKeyConflict;
}
/**
 *
 * @export
 * @interface ContentKeyDetailsV1
 */
export interface ContentKeyDetailsV1 {
  /**
   *
   * @type {ContentKeyV1}
   * @memberof ContentKeyDetailsV1
   */
  key?: ContentKeyV1;
  /**
   *
   * @type {MergeBehaviorV1}
   * @memberof ContentKeyDetailsV1
   */
  mergeBehavior?: MergeBehaviorV1;
  /**
   *
   * @type {ContentKeyConflictV1}
   * @memberof ContentKeyDetailsV1
   */
  conflictType?: ContentKeyConflictV1;
}
/**
 *
 * @export
 * @interface ContentKeyDetailsV2
 */
export interface ContentKeyDetailsV2 {
  /**
   *
   * @type {ContentKeyV2}
   * @memberof ContentKeyDetailsV2
   */
  key?: ContentKeyV2;
  /**
   *
   * @type {MergeBehaviorV2}
   * @memberof ContentKeyDetailsV2
   */
  mergeBehavior?: MergeBehaviorV2;
  /**
   *
   * @type {ContentKeyConflictV2}
   * @memberof ContentKeyDetailsV2
   */
  conflictType?: ContentKeyConflictV2;
}
/**
 *
 * @export
 * @interface ContentKeyV1
 */
export interface ContentKeyV1 {
  /**
   *
   * @type {Array<string>}
   * @memberof ContentKeyV1
   */
  elements: Array<string>;
}
/**
 *
 * @export
 * @interface ContentKeyV2
 */
export interface ContentKeyV2 {
  /**
   *
   * @type {Array<string>}
   * @memberof ContentKeyV2
   */
  elements: Array<string>;
}
/**
 *
 * @export
 * @interface ContentResponseV2
 */
export interface ContentResponseV2 {
  /**
   *
   * @type {ContentV2}
   * @memberof ContentResponseV2
   */
  content: ContentV2;
  /**
   *
   * @type {ReferenceV2}
   * @memberof ContentResponseV2
   */
  effectiveReference?: ReferenceV2;
}
/**
 * @type ContentV1
 *
 * @export
 */
export type ContentV1 =
  | ({ type: "DELTA_LAKE_TABLE" } & DeltaLakeTableV1)
  | ({ type: "ICEBERG_TABLE" } & IcebergTableV1)
  | ({ type: "ICEBERG_VIEW" } & IcebergViewV1)
  | ({ type: "NAMESPACE" } & NamespaceV1);
/**
 * @type ContentV2
 *
 * @export
 */
export type ContentV2 =
  | ({ type: "DELTA_LAKE_TABLE" } & DeltaLakeTableV2)
  | ({ type: "ICEBERG_TABLE" } & IcebergTableV2)
  | ({ type: "ICEBERG_VIEW" } & IcebergViewV2)
  | ({ type: "NAMESPACE" } & NamespaceV2);
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
  key: ContentKey;
  /**
   *
   * @type {Content}
   * @memberof ContentWithKey
   */
  content: Content;
}
/**
 *
 * @export
 * @interface ContentWithKeyV1
 */
export interface ContentWithKeyV1 {
  /**
   *
   * @type {ContentKeyV1}
   * @memberof ContentWithKeyV1
   */
  key: ContentKeyV1;
  /**
   *
   * @type {ContentV1}
   * @memberof ContentWithKeyV1
   */
  content: ContentV1;
}
/**
 *
 * @export
 * @interface ContentWithKeyV2
 */
export interface ContentWithKeyV2 {
  /**
   *
   * @type {ContentKeyV2}
   * @memberof ContentWithKeyV2
   */
  key: ContentKeyV2;
  /**
   *
   * @type {ContentV2}
   * @memberof ContentWithKeyV2
   */
  content: ContentV2;
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
  key: ContentKey;
}
/**
 *
 * @export
 * @interface DeleteV1
 */
export interface DeleteV1 {
  /**
   *
   * @type {ContentKeyV1}
   * @memberof DeleteV1
   */
  key: ContentKeyV1;
}
/**
 *
 * @export
 * @interface DeleteV2
 */
export interface DeleteV2 {
  /**
   *
   * @type {ContentKeyV2}
   * @memberof DeleteV2
   */
  key: ContentKeyV2;
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
 * @interface DeltaLakeTableV1
 */
export interface DeltaLakeTableV1 {
  /**
   *
   * @type {string}
   * @memberof DeltaLakeTableV1
   */
  id?: string;
  /**
   *
   * @type {Array<string>}
   * @memberof DeltaLakeTableV1
   */
  metadataLocationHistory: Array<string>;
  /**
   *
   * @type {Array<string>}
   * @memberof DeltaLakeTableV1
   */
  checkpointLocationHistory: Array<string>;
  /**
   *
   * @type {string}
   * @memberof DeltaLakeTableV1
   */
  lastCheckpoint?: string;
}
/**
 *
 * @export
 * @interface DeltaLakeTableV2
 */
export interface DeltaLakeTableV2 {
  /**
   *
   * @type {string}
   * @memberof DeltaLakeTableV2
   */
  id?: string;
  /**
   *
   * @type {Array<string>}
   * @memberof DeltaLakeTableV2
   */
  metadataLocationHistory: Array<string>;
  /**
   *
   * @type {Array<string>}
   * @memberof DeltaLakeTableV2
   */
  checkpointLocationHistory: Array<string>;
  /**
   *
   * @type {string}
   * @memberof DeltaLakeTableV2
   */
  lastCheckpoint?: string;
}
/**
 *
 * @export
 * @interface Detached
 */
export interface Detached {
  /**
   *
   * @type {string}
   * @memberof Detached
   */
  hash: string;
  /**
   *
   * @type {ReferenceMetadata}
   * @memberof Detached
   */
  metadata?: ReferenceMetadata;
}
/**
 *
 * @export
 * @interface DetachedV1
 */
export interface DetachedV1 {
  /**
   *
   * @type {string}
   * @memberof DetachedV1
   */
  hash: string;
  /**
   *
   * @type {ReferenceMetadataV1}
   * @memberof DetachedV1
   */
  metadata?: ReferenceMetadataV1;
}
/**
 *
 * @export
 * @interface DetachedV2
 */
export interface DetachedV2 {
  /**
   *
   * @type {string}
   * @memberof DetachedV2
   */
  hash: string;
  /**
   *
   * @type {ReferenceMetadataV2}
   * @memberof DetachedV2
   */
  metadata?: ReferenceMetadataV2;
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
 * @interface DiffEntryV1
 */
export interface DiffEntryV1 {
  /**
   *
   * @type {ContentKeyV1}
   * @memberof DiffEntryV1
   */
  key?: ContentKeyV1;
  /**
   *
   * @type {ContentV1}
   * @memberof DiffEntryV1
   */
  from?: ContentV1;
  /**
   *
   * @type {ContentV1}
   * @memberof DiffEntryV1
   */
  to?: ContentV1;
}
/**
 *
 * @export
 * @interface DiffEntryV2
 */
export interface DiffEntryV2 {
  /**
   *
   * @type {ContentKeyV2}
   * @memberof DiffEntryV2
   */
  key?: ContentKeyV2;
  /**
   *
   * @type {ContentV2}
   * @memberof DiffEntryV2
   */
  from?: ContentV2;
  /**
   *
   * @type {ContentV2}
   * @memberof DiffEntryV2
   */
  to?: ContentV2;
}
/**
 *
 * @export
 * @interface DiffResponse
 */
export interface DiffResponse {
  /**
   *
   * @type {boolean}
   * @memberof DiffResponse
   */
  hasMore?: boolean;
  /**
   *
   * @type {string}
   * @memberof DiffResponse
   */
  token?: string;
  /**
   *
   * @type {Array<DiffEntry>}
   * @memberof DiffResponse
   */
  diffs?: Array<DiffEntry>;
  /**
   *
   * @type {Reference}
   * @memberof DiffResponse
   */
  effectiveFromReference?: Reference;
  /**
   *
   * @type {Reference}
   * @memberof DiffResponse
   */
  effectiveToReference?: Reference;
}
/**
 *
 * @export
 * @interface DiffResponseV1
 */
export interface DiffResponseV1 {
  /**
   *
   * @type {boolean}
   * @memberof DiffResponseV1
   */
  hasMore?: boolean;
  /**
   *
   * @type {string}
   * @memberof DiffResponseV1
   */
  token?: string;
  /**
   *
   * @type {Array<DiffEntryV1>}
   * @memberof DiffResponseV1
   */
  diffs?: Array<DiffEntryV1>;
}
/**
 *
 * @export
 * @interface DiffResponseV2
 */
export interface DiffResponseV2 {
  /**
   *
   * @type {boolean}
   * @memberof DiffResponseV2
   */
  hasMore?: boolean;
  /**
   *
   * @type {string}
   * @memberof DiffResponseV2
   */
  token?: string;
  /**
   *
   * @type {Array<DiffEntryV2>}
   * @memberof DiffResponseV2
   */
  diffs?: Array<DiffEntryV2>;
  /**
   *
   * @type {ReferenceV2}
   * @memberof DiffResponseV2
   */
  effectiveFromReference?: ReferenceV2;
  /**
   *
   * @type {ReferenceV2}
   * @memberof DiffResponseV2
   */
  effectiveToReference?: ReferenceV2;
}
/**
 *
 * @export
 * @interface EntriesResponseV1
 */
export interface EntriesResponseV1 {
  /**
   *
   * @type {boolean}
   * @memberof EntriesResponseV1
   */
  hasMore?: boolean;
  /**
   *
   * @type {string}
   * @memberof EntriesResponseV1
   */
  token?: string;
  /**
   *
   * @type {Array<EntryV1>}
   * @memberof EntriesResponseV1
   */
  entries: Array<EntryV1>;
}
/**
 *
 * @export
 * @interface EntriesResponseV2
 */
export interface EntriesResponseV2 {
  /**
   *
   * @type {boolean}
   * @memberof EntriesResponseV2
   */
  hasMore?: boolean;
  /**
   *
   * @type {string}
   * @memberof EntriesResponseV2
   */
  token?: string;
  /**
   *
   * @type {Array<EntryV2>}
   * @memberof EntriesResponseV2
   */
  entries: Array<EntryV2>;
  /**
   *
   * @type {ReferenceV2}
   * @memberof EntriesResponseV2
   */
  effectiveReference?: ReferenceV2;
}
/**
 *
 * @export
 * @interface EntryV1
 */
export interface EntryV1 {
  /**
   * Declares the type of a Nessie content object, which is currently one of ICEBERG_TABLE, DELTA_LAKE_TABLE, ICEBERG_VIEW or NAMESPACE, which are the discriminator mapping values of the 'Content' type.
   * @type {string}
   * @memberof EntryV1
   */
  type: string;
  /**
   *
   * @type {ContentKeyV1}
   * @memberof EntryV1
   */
  name: ContentKeyV1;
}
/**
 *
 * @export
 * @interface EntryV2
 */
export interface EntryV2 {
  /**
   * Declares the type of a Nessie content object, which is currently one of ICEBERG_TABLE, DELTA_LAKE_TABLE, ICEBERG_VIEW or NAMESPACE, which are the discriminator mapping values of the 'Content' type.
   * @type {string}
   * @memberof EntryV2
   */
  type: string;
  /**
   *
   * @type {ContentKeyV2}
   * @memberof EntryV2
   */
  name: ContentKeyV2;
  /**
   *
   * @type {string}
   * @memberof EntryV2
   */
  contentId?: string;
  /**
   *
   * @type {ContentV2}
   * @memberof EntryV2
   */
  content?: ContentV2;
}
/**
 *
 * @export
 * @enum {string}
 */
export enum FetchOption {
  Minimal = "MINIMAL",
  All = "ALL",
}
/**
 *
 * @export
 * @interface GenericMetadata
 */
export interface GenericMetadata {
  /**
   *
   * @type {string}
   * @memberof GenericMetadata
   */
  variant: string;
  /**
   *
   * @type {JsonNode}
   * @memberof GenericMetadata
   */
  metadata?: JsonNode;
}
/**
 *
 * @export
 * @interface GenericMetadataV1
 */
export interface GenericMetadataV1 {
  /**
   *
   * @type {string}
   * @memberof GenericMetadataV1
   */
  variant: string;
  /**
   *
   * @type {JsonNodeV1}
   * @memberof GenericMetadataV1
   */
  metadata?: JsonNodeV1;
}
/**
 *
 * @export
 * @interface GenericMetadataV2
 */
export interface GenericMetadataV2 {
  /**
   *
   * @type {string}
   * @memberof GenericMetadataV2
   */
  variant: string;
  /**
   *
   * @type {JsonNodeV2}
   * @memberof GenericMetadataV2
   */
  metadata?: JsonNodeV2;
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
  /**
   *
   * @type {Reference}
   * @memberof GetMultipleContentsResponse
   */
  effectiveReference?: Reference;
}
/**
 *
 * @export
 * @interface GetMultipleContentsResponseV1
 */
export interface GetMultipleContentsResponseV1 {
  /**
   *
   * @type {Array<ContentWithKeyV1>}
   * @memberof GetMultipleContentsResponseV1
   */
  contents: Array<ContentWithKeyV1>;
}
/**
 *
 * @export
 * @interface GetMultipleContentsResponseV2
 */
export interface GetMultipleContentsResponseV2 {
  /**
   *
   * @type {Array<ContentWithKeyV2>}
   * @memberof GetMultipleContentsResponseV2
   */
  contents: Array<ContentWithKeyV2>;
  /**
   *
   * @type {ReferenceV2}
   * @memberof GetMultipleContentsResponseV2
   */
  effectiveReference?: ReferenceV2;
}
/**
 *
 * @export
 * @interface GetNamespacesResponseV1
 */
export interface GetNamespacesResponseV1 {
  /**
   *
   * @type {Array<NamespaceV1>}
   * @memberof GetNamespacesResponseV1
   */
  namespaces: Array<NamespaceV1>;
}
/**
 * Represents the state of an Iceberg table in Nessie. An Iceberg table is globally identified via its unique 'Content.id'.
 *
 * A Nessie commit-operation, performed via 'TreeApi.commitMultipleOperations',for Iceberg consists of a 'Operation.Put' with an 'IcebergTable' as in the 'content' field and the previous value of 'IcebergTable' in the 'expectedContent' field.
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
  /**
   *
   * @type {GenericMetadata}
   * @memberof IcebergTable
   */
  metadata?: GenericMetadata;
}
/**
 * Represents the state of an Iceberg table in Nessie. An Iceberg table is globally identified via its unique 'Content.id'.
 *
 * A Nessie commit-operation, performed via 'TreeApi.commitMultipleOperations',for Iceberg consists of a 'Operation.Put' with an 'IcebergTable' as in the 'content' field and the previous value of 'IcebergTable' in the 'expectedContent' field.
 * @export
 * @interface IcebergTableV1
 */
export interface IcebergTableV1 {
  /**
   *
   * @type {string}
   * @memberof IcebergTableV1
   */
  id?: string;
  /**
   *
   * @type {string}
   * @memberof IcebergTableV1
   */
  metadataLocation: string;
  /**
   *
   * @type {number}
   * @memberof IcebergTableV1
   */
  snapshotId?: number;
  /**
   *
   * @type {number}
   * @memberof IcebergTableV1
   */
  schemaId?: number;
  /**
   *
   * @type {number}
   * @memberof IcebergTableV1
   */
  specId?: number;
  /**
   *
   * @type {number}
   * @memberof IcebergTableV1
   */
  sortOrderId?: number;
  /**
   *
   * @type {GenericMetadataV1}
   * @memberof IcebergTableV1
   */
  metadata?: GenericMetadataV1;
}
/**
 * Represents the state of an Iceberg table in Nessie. An Iceberg table is globally identified via its unique 'Content.id'.
 *
 * A Nessie commit-operation, performed via 'TreeApi.commitMultipleOperations',for Iceberg consists of a 'Operation.Put' with an 'IcebergTable' as in the 'content' field and the previous value of 'IcebergTable' in the 'expectedContent' field.
 * @export
 * @interface IcebergTableV2
 */
export interface IcebergTableV2 {
  /**
   *
   * @type {string}
   * @memberof IcebergTableV2
   */
  id?: string;
  /**
   *
   * @type {string}
   * @memberof IcebergTableV2
   */
  metadataLocation: string;
  /**
   *
   * @type {number}
   * @memberof IcebergTableV2
   */
  snapshotId?: number;
  /**
   *
   * @type {number}
   * @memberof IcebergTableV2
   */
  schemaId?: number;
  /**
   *
   * @type {number}
   * @memberof IcebergTableV2
   */
  specId?: number;
  /**
   *
   * @type {number}
   * @memberof IcebergTableV2
   */
  sortOrderId?: number;
  /**
   *
   * @type {GenericMetadataV2}
   * @memberof IcebergTableV2
   */
  metadata?: GenericMetadataV2;
}
/**
 *
 * @export
 * @interface IcebergView
 */
export interface IcebergView {
  /**
   *
   * @type {string}
   * @memberof IcebergView
   */
  id?: string;
  /**
   *
   * @type {string}
   * @memberof IcebergView
   */
  metadataLocation: string;
  /**
   *
   * @type {number}
   * @memberof IcebergView
   */
  versionId?: number;
  /**
   *
   * @type {number}
   * @memberof IcebergView
   */
  schemaId?: number;
  /**
   *
   * @type {string}
   * @memberof IcebergView
   */
  sqlText: string;
  /**
   *
   * @type {string}
   * @memberof IcebergView
   */
  dialect?: string;
  /**
   *
   * @type {GenericMetadata}
   * @memberof IcebergView
   */
  metadata?: GenericMetadata;
}
/**
 *
 * @export
 * @interface IcebergViewV1
 */
export interface IcebergViewV1 {
  /**
   *
   * @type {string}
   * @memberof IcebergViewV1
   */
  id?: string;
  /**
   *
   * @type {string}
   * @memberof IcebergViewV1
   */
  metadataLocation: string;
  /**
   *
   * @type {number}
   * @memberof IcebergViewV1
   */
  versionId?: number;
  /**
   *
   * @type {number}
   * @memberof IcebergViewV1
   */
  schemaId?: number;
  /**
   *
   * @type {string}
   * @memberof IcebergViewV1
   */
  sqlText: string;
  /**
   *
   * @type {string}
   * @memberof IcebergViewV1
   */
  dialect?: string;
  /**
   *
   * @type {GenericMetadataV1}
   * @memberof IcebergViewV1
   */
  metadata?: GenericMetadataV1;
}
/**
 *
 * @export
 * @interface IcebergViewV2
 */
export interface IcebergViewV2 {
  /**
   *
   * @type {string}
   * @memberof IcebergViewV2
   */
  id?: string;
  /**
   *
   * @type {string}
   * @memberof IcebergViewV2
   */
  metadataLocation: string;
  /**
   *
   * @type {number}
   * @memberof IcebergViewV2
   */
  versionId?: number;
  /**
   *
   * @type {number}
   * @memberof IcebergViewV2
   */
  schemaId?: number;
  /**
   *
   * @type {string}
   * @memberof IcebergViewV2
   */
  sqlText: string;
  /**
   *
   * @type {string}
   * @memberof IcebergViewV2
   */
  dialect?: string;
  /**
   *
   * @type {GenericMetadataV2}
   * @memberof IcebergViewV2
   */
  metadata?: GenericMetadataV2;
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
  commitMeta: CommitMeta;
  /**
   *
   * @type {Array<string>}
   * @memberof LogEntry
   */
  additionalParents?: Array<string>;
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
 * @interface LogEntryV1
 */
export interface LogEntryV1 {
  /**
   *
   * @type {CommitMetaV1}
   * @memberof LogEntryV1
   */
  commitMeta: CommitMetaV1;
  /**
   *
   * @type {Array<string>}
   * @memberof LogEntryV1
   */
  additionalParents?: Array<string>;
  /**
   *
   * @type {string}
   * @memberof LogEntryV1
   */
  parentCommitHash?: string;
  /**
   *
   * @type {Array<OperationV1>}
   * @memberof LogEntryV1
   */
  operations?: Array<OperationV1>;
}
/**
 *
 * @export
 * @interface LogEntryV2
 */
export interface LogEntryV2 {
  /**
   *
   * @type {CommitMetaV2}
   * @memberof LogEntryV2
   */
  commitMeta: CommitMetaV2;
  /**
   *
   * @type {string}
   * @memberof LogEntryV2
   */
  parentCommitHash?: string;
  /**
   *
   * @type {Array<OperationV2>}
   * @memberof LogEntryV2
   */
  operations?: Array<OperationV2>;
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
 * @interface LogResponseV1
 */
export interface LogResponseV1 {
  /**
   *
   * @type {boolean}
   * @memberof LogResponseV1
   */
  hasMore?: boolean;
  /**
   *
   * @type {string}
   * @memberof LogResponseV1
   */
  token?: string;
  /**
   *
   * @type {Array<LogEntryV1>}
   * @memberof LogResponseV1
   */
  logEntries: Array<LogEntryV1>;
}
/**
 *
 * @export
 * @interface LogResponseV2
 */
export interface LogResponseV2 {
  /**
   *
   * @type {boolean}
   * @memberof LogResponseV2
   */
  hasMore?: boolean;
  /**
   *
   * @type {string}
   * @memberof LogResponseV2
   */
  token?: string;
  /**
   *
   * @type {Array<LogEntryV2>}
   * @memberof LogResponseV2
   */
  logEntries: Array<LogEntryV2>;
}
/**
 *
 * @export
 * @interface Merge
 */
export interface Merge {
  /**
   * Optional commit message for this merge request
   *
   * If not set, the server will generate a commit message automatically using metadata from the
   * merged commits.
   * @type {string}
   * @memberof Merge
   */
  message?: string;
  /**
   * The hash of the last commit to merge.
   *
   * This commit must be present in the history on 'fromRefName' before the first common parent with respect to the target branch.
   * @type {string}
   * @memberof Merge
   */
  fromHash: string;
  /**
   * The name of the reference that contains the 'source' commits for the requested merge or transplant operation.
   * @type {string}
   * @memberof Merge
   */
  fromRefName: string;
  /**
   * Specific merge behaviour requests by content key.
   *
   * The default is set by the `defaultKeyMergeMode` parameter.
   * @type {Array<MergeKeyBehavior>}
   * @memberof Merge
   */
  keyMergeModes?: Array<MergeKeyBehavior>;
  /**
   * The default merge mode. If not set, `NORMAL` is assumed.
   *
   * This settings applies to key thaWhen set to 'true' instructs the server to validate the request
   *         but to avoid committing any changes.t are not explicitly mentioned in the `keyMergeModes` property.
   * @type {MergeBehavior}
   * @memberof Merge
   */
  defaultKeyMergeMode?: MergeBehavior;
  /**
   * When set to 'true' instructs the server to validate the request but to avoid committing any changes.
   * @type {boolean}
   * @memberof Merge
   */
  dryRun?: boolean;
  /**
   * Whether to provide optional response data.
   * @type {boolean}
   * @memberof Merge
   */
  fetchAdditionalInfo?: boolean;
  /**
   * When set to 'true' instructs the server to produce normal (non-error) responses in case a conflict is detected and report conflict details in the response payload.
   * @type {boolean}
   * @memberof Merge
   */
  returnConflictAsResult?: boolean;
}
/**
 *
 * @export
 * @interface Merge1
 */
export interface Merge1 {
  /**
   *
   * @type {string}
   * @memberof Merge1
   */
  fromRefName: string;
  /**
   *
   * @type {string}
   * @memberof Merge1
   */
  fromHash: string;
  /**
   *
   * @type {Array<MergeKeyBehavior>}
   * @memberof Merge1
   */
  keyMergeModes?: Array<MergeKeyBehavior>;
  /**
   *
   * @type {MergeBehavior}
   * @memberof Merge1
   */
  defaultKeyMergeMode?: MergeBehavior;
  /**
   *
   * @type {boolean}
   * @memberof Merge1
   */
  dryRun?: boolean;
  /**
   *
   * @type {boolean}
   * @memberof Merge1
   */
  fetchAdditionalInfo?: boolean;
  /**
   *
   * @type {boolean}
   * @memberof Merge1
   */
  returnConflictAsResult?: boolean;
}
/**
 *
 * @export
 * @enum {string}
 */
export enum MergeBehavior {
  Normal = "NORMAL",
  Force = "FORCE",
  Drop = "DROP",
}
/**
 *
 * @export
 * @enum {string}
 */
export enum MergeBehaviorV1 {
  Normal = "NORMAL",
  Force = "FORCE",
  Drop = "DROP",
}
/**
 *
 * @export
 * @enum {string}
 */
export enum MergeBehaviorV2 {
  Normal = "NORMAL",
  Force = "FORCE",
  Drop = "DROP",
}
/**
 *
 * @export
 * @interface MergeKeyBehavior
 */
export interface MergeKeyBehavior {
  /**
   *
   * @type {ContentKey}
   * @memberof MergeKeyBehavior
   */
  key?: ContentKey;
  /**
   *
   * @type {MergeBehavior}
   * @memberof MergeKeyBehavior
   */
  mergeBehavior?: MergeBehavior;
}
/**
 *
 * @export
 * @interface MergeResponse
 */
export interface MergeResponse {
  /**
   *
   * @type {string}
   * @memberof MergeResponse
   */
  resultantTargetHash?: string;
  /**
   *
   * @type {string}
   * @memberof MergeResponse
   */
  commonAncestor?: string;
  /**
   *
   * @type {string}
   * @memberof MergeResponse
   */
  targetBranch?: string;
  /**
   *
   * @type {string}
   * @memberof MergeResponse
   */
  effectiveTargetHash?: string;
  /**
   *
   * @type {string}
   * @memberof MergeResponse
   */
  expectedHash?: string;
  /**
   *
   * @type {Array<ContentKeyDetails>}
   * @memberof MergeResponse
   */
  details?: Array<ContentKeyDetails>;
}
/**
 *
 * @export
 * @interface MergeResponseV1
 */
export interface MergeResponseV1 {
  /**
   *
   * @type {string}
   * @memberof MergeResponseV1
   */
  resultantTargetHash?: string;
  /**
   *
   * @type {string}
   * @memberof MergeResponseV1
   */
  commonAncestor?: string;
  /**
   *
   * @type {string}
   * @memberof MergeResponseV1
   */
  targetBranch?: string;
  /**
   *
   * @type {string}
   * @memberof MergeResponseV1
   */
  effectiveTargetHash?: string;
  /**
   *
   * @type {string}
   * @memberof MergeResponseV1
   */
  expectedHash?: string;
  /**
   *
   * @type {Array<ContentKeyDetailsV1>}
   * @memberof MergeResponseV1
   */
  details?: Array<ContentKeyDetailsV1>;
}
/**
 *
 * @export
 * @interface MergeResponseV2
 */
export interface MergeResponseV2 {
  /**
   *
   * @type {string}
   * @memberof MergeResponseV2
   */
  resultantTargetHash?: string;
  /**
   *
   * @type {string}
   * @memberof MergeResponseV2
   */
  commonAncestor?: string;
  /**
   *
   * @type {string}
   * @memberof MergeResponseV2
   */
  targetBranch?: string;
  /**
   *
   * @type {string}
   * @memberof MergeResponseV2
   */
  effectiveTargetHash?: string;
  /**
   *
   * @type {string}
   * @memberof MergeResponseV2
   */
  expectedHash?: string;
  /**
   *
   * @type {Array<ContentKeyDetailsV2>}
   * @memberof MergeResponseV2
   */
  details?: Array<ContentKeyDetailsV2>;
}
/**
 *
 * @export
 * @interface Namespace
 */
export interface Namespace {
  /**
   *
   * @type {string}
   * @memberof Namespace
   */
  id?: string;
  /**
   *
   * @type {Array<string>}
   * @memberof Namespace
   */
  elements: Array<string>;
  /**
   *
   * @type {{ [key: string]: string; }}
   * @memberof Namespace
   */
  properties: { [key: string]: string };
}
/**
 *
 * @export
 * @interface NamespaceUpdate
 */
export interface NamespaceUpdate {
  /**
   *
   * @type {{ [key: string]: string; }}
   * @memberof NamespaceUpdate
   */
  propertyUpdates?: { [key: string]: string };
  /**
   *
   * @type {Set<string>}
   * @memberof NamespaceUpdate
   */
  propertyRemovals?: Set<string>;
}
/**
 *
 * @export
 * @interface NamespaceV1
 */
export interface NamespaceV1 {
  /**
   *
   * @type {string}
   * @memberof NamespaceV1
   */
  id?: string;
  /**
   *
   * @type {Array<string>}
   * @memberof NamespaceV1
   */
  elements: Array<string>;
  /**
   *
   * @type {{ [key: string]: string; }}
   * @memberof NamespaceV1
   */
  properties: { [key: string]: string };
}
/**
 *
 * @export
 * @interface NamespaceV2
 */
export interface NamespaceV2 {
  /**
   *
   * @type {string}
   * @memberof NamespaceV2
   */
  id?: string;
  /**
   *
   * @type {Array<string>}
   * @memberof NamespaceV2
   */
  elements: Array<string>;
  /**
   *
   * @type {{ [key: string]: string; }}
   * @memberof NamespaceV2
   */
  properties: { [key: string]: string };
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
 *
 * @export
 * @interface NessieConfigurationV1
 */
export interface NessieConfigurationV1 {
  /**
   *
   * @type {string}
   * @memberof NessieConfigurationV1
   */
  defaultBranch?: string;
  /**
   *
   * @type {number}
   * @memberof NessieConfigurationV1
   */
  maxSupportedApiVersion?: number;
}
/**
 * @type Operation
 *
 * @export
 */
export type Operation =
  | ({ type: "DELETE" } & Delete)
  | ({ type: "PUT" } & Put)
  | ({ type: "UNCHANGED" } & Unchanged);
/**
 * @type OperationV1
 *
 * @export
 */
export type OperationV1 =
  | ({ type: "DELETE" } & DeleteV1)
  | ({ type: "PUT" } & PutV1)
  | ({ type: "UNCHANGED" } & UnchangedV1);
/**
 * @type OperationV2
 *
 * @export
 */
export type OperationV2 =
  | ({ type: "DELETE" } & DeleteV2)
  | ({ type: "PUT" } & PutV2)
  | ({ type: "UNCHANGED" } & UnchangedV2);
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
  commitMeta: CommitMeta;
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
  key: ContentKey;
  /**
   *
   * @type {Content}
   * @memberof Put
   */
  content: Content;
  /**
   *
   * @type {Content}
   * @memberof Put
   */
  expectedContent?: Content;
}
/**
 * Add or replace (put) a 'Content' object for a 'ContentKey'. If the actual table type tracks the 'global state' of individual tables (Iceberg as of today), every 'Put'-operation must contain a non-null value for 'expectedContent'.
 * @export
 * @interface PutV1
 */
export interface PutV1 {
  /**
   *
   * @type {ContentKeyV1}
   * @memberof PutV1
   */
  key: ContentKeyV1;
  /**
   *
   * @type {ContentV1}
   * @memberof PutV1
   */
  content: ContentV1;
  /**
   *
   * @type {ContentV1}
   * @memberof PutV1
   */
  expectedContent?: ContentV1;
}
/**
 * Add or replace (put) a 'Content' object for a 'ContentKey'. If the actual table type tracks the 'global state' of individual tables (Iceberg as of today), every 'Put'-operation must contain a non-null value for 'expectedContent'.
 * @export
 * @interface PutV2
 */
export interface PutV2 {
  /**
   *
   * @type {ContentKeyV2}
   * @memberof PutV2
   */
  key: ContentKeyV2;
  /**
   *
   * @type {ContentV2}
   * @memberof PutV2
   */
  content: ContentV2;
  /**
   *
   * @type {ContentV2}
   * @memberof PutV2
   */
  expectedContent?: ContentV2;
}
/**
 * @type Reference
 *
 * @export
 */
export type Reference =
  | ({ type: "BRANCH" } & Branch)
  | ({ type: "DETACHED" } & Detached)
  | ({ type: "TAG" } & Tag);
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
 * @interface ReferenceMetadataV1
 */
export interface ReferenceMetadataV1 {
  /**
   *
   * @type {number}
   * @memberof ReferenceMetadataV1
   */
  numCommitsAhead?: number;
  /**
   *
   * @type {number}
   * @memberof ReferenceMetadataV1
   */
  numCommitsBehind?: number;
  /**
   *
   * @type {CommitMetaV1}
   * @memberof ReferenceMetadataV1
   */
  commitMetaOfHEAD?: CommitMetaV1;
  /**
   *
   * @type {string}
   * @memberof ReferenceMetadataV1
   */
  commonAncestorHash?: string;
  /**
   *
   * @type {number}
   * @memberof ReferenceMetadataV1
   */
  numTotalCommits?: number;
}
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
 * @interface ReferenceMetadataV2
 */
export interface ReferenceMetadataV2 {
  /**
   *
   * @type {number}
   * @memberof ReferenceMetadataV2
   */
  numCommitsAhead?: number;
  /**
   *
   * @type {number}
   * @memberof ReferenceMetadataV2
   */
  numCommitsBehind?: number;
  /**
   *
   * @type {CommitMetaV2}
   * @memberof ReferenceMetadataV2
   */
  commitMetaOfHEAD?: CommitMetaV2;
  /**
   *
   * @type {string}
   * @memberof ReferenceMetadataV2
   */
  commonAncestorHash?: string;
  /**
   *
   * @type {number}
   * @memberof ReferenceMetadataV2
   */
  numTotalCommits?: number;
}
/**
 *
 * @export
 * @enum {string}
 */
export enum ReferenceType {
  Branch = "branch",
  Tag = "tag",
}
/**
 * @type ReferenceV1
 *
 * @export
 */
export type ReferenceV1 =
  | ({ type: "BRANCH" } & BranchV1)
  | ({ type: "DETACHED" } & DetachedV1)
  | ({ type: "TAG" } & TagV1);
/**
 * @type ReferenceV2
 *
 * @export
 */
export type ReferenceV2 =
  | ({ type: "BRANCH" } & BranchV2)
  | ({ type: "DETACHED" } & DetachedV2)
  | ({ type: "TAG" } & TagV2);
/**
 *
 * @export
 * @interface ReferencesResponseV1
 */
export interface ReferencesResponseV1 {
  /**
   *
   * @type {boolean}
   * @memberof ReferencesResponseV1
   */
  hasMore?: boolean;
  /**
   *
   * @type {string}
   * @memberof ReferencesResponseV1
   */
  token?: string;
  /**
   *
   * @type {Array<ReferenceV1>}
   * @memberof ReferencesResponseV1
   */
  references: Array<ReferenceV1>;
}
/**
 *
 * @export
 * @interface ReferencesResponseV2
 */
export interface ReferencesResponseV2 {
  /**
   *
   * @type {boolean}
   * @memberof ReferencesResponseV2
   */
  hasMore?: boolean;
  /**
   *
   * @type {string}
   * @memberof ReferencesResponseV2
   */
  token?: string;
  /**
   *
   * @type {Array<ReferenceV2>}
   * @memberof ReferencesResponseV2
   */
  references: Array<ReferenceV2>;
}
/**
 *
 * @export
 * @interface SingleReferenceResponse
 */
export interface SingleReferenceResponse {
  /**
   *
   * @type {Reference}
   * @memberof SingleReferenceResponse
   */
  reference: Reference;
}
/**
 *
 * @export
 * @interface SingleReferenceResponseV2
 */
export interface SingleReferenceResponseV2 {
  /**
   *
   * @type {ReferenceV2}
   * @memberof SingleReferenceResponseV2
   */
  reference: ReferenceV2;
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
   * @type {ReferenceMetadata}
   * @memberof Tag
   */
  metadata?: ReferenceMetadata;
  /**
   *
   * @type {string}
   * @memberof Tag
   */
  hash?: string;
}
/**
 *
 * @export
 * @interface TagV1
 */
export interface TagV1 {
  /**
   *
   * @type {string}
   * @memberof TagV1
   */
  name: string;
  /**
   *
   * @type {ReferenceMetadataV1}
   * @memberof TagV1
   */
  metadata?: ReferenceMetadataV1;
  /**
   *
   * @type {string}
   * @memberof TagV1
   */
  hash?: string;
}
/**
 *
 * @export
 * @interface TagV2
 */
export interface TagV2 {
  /**
   *
   * @type {string}
   * @memberof TagV2
   */
  name: string;
  /**
   *
   * @type {ReferenceMetadataV2}
   * @memberof TagV2
   */
  metadata?: ReferenceMetadataV2;
  /**
   *
   * @type {string}
   * @memberof TagV2
   */
  hash?: string;
}
/**
 *
 * @export
 * @interface Transplant
 */
export interface Transplant {
  /**
   * Commit message for this transplant request.
   * @type {string}
   * @memberof Transplant
   */
  message?: string;
  /**
   * Lists the hashes of commits that should be transplanted into the target branch.
   * @type {Set<string>}
   * @memberof Transplant
   */
  hashesToTransplant: Set<string>;
  /**
   * The name of the reference that contains the 'source' commits for the requested merge or transplant operation.
   * @type {string}
   * @memberof Transplant
   */
  fromRefName: string;
  /**
   * Specific merge behaviour requests by content key.
   *
   * The default is set by the `defaultKeyMergeMode` parameter.
   * @type {Array<MergeKeyBehavior>}
   * @memberof Transplant
   */
  keyMergeModes?: Array<MergeKeyBehavior>;
  /**
   * The default merge mode. If not set, `NORMAL` is assumed.
   *
   * This settings applies to key thaWhen set to 'true' instructs the server to validate the request
   *         but to avoid committing any changes.t are not explicitly mentioned in the `keyMergeModes` property.
   * @type {MergeBehavior}
   * @memberof Transplant
   */
  defaultKeyMergeMode?: MergeBehavior;
  /**
   * When set to 'true' instructs the server to validate the request but to avoid committing any changes.
   * @type {boolean}
   * @memberof Transplant
   */
  dryRun?: boolean;
  /**
   * Whether to provide optional response data.
   * @type {boolean}
   * @memberof Transplant
   */
  fetchAdditionalInfo?: boolean;
  /**
   * When set to 'true' instructs the server to produce normal (non-error) responses in case a conflict is detected and report conflict details in the response payload.
   * @type {boolean}
   * @memberof Transplant
   */
  returnConflictAsResult?: boolean;
}
/**
 *
 * @export
 * @interface Transplant1
 */
export interface Transplant1 {
  /**
   *
   * @type {string}
   * @memberof Transplant1
   */
  fromRefName: string;
  /**
   *
   * @type {Set<string>}
   * @memberof Transplant1
   */
  hashesToTransplant: Set<string>;
  /**
   *
   * @type {Array<MergeKeyBehavior>}
   * @memberof Transplant1
   */
  keyMergeModes?: Array<MergeKeyBehavior>;
  /**
   *
   * @type {MergeBehavior}
   * @memberof Transplant1
   */
  defaultKeyMergeMode?: MergeBehavior;
  /**
   *
   * @type {boolean}
   * @memberof Transplant1
   */
  dryRun?: boolean;
  /**
   *
   * @type {boolean}
   * @memberof Transplant1
   */
  fetchAdditionalInfo?: boolean;
  /**
   *
   * @type {boolean}
   * @memberof Transplant1
   */
  returnConflictAsResult?: boolean;
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
  key: ContentKey;
}
/**
 *
 * @export
 * @interface UnchangedV1
 */
export interface UnchangedV1 {
  /**
   *
   * @type {ContentKeyV1}
   * @memberof UnchangedV1
   */
  key: ContentKeyV1;
}
/**
 *
 * @export
 * @interface UnchangedV2
 */
export interface UnchangedV2 {
  /**
   *
   * @type {ContentKeyV2}
   * @memberof UnchangedV2
   */
  key: ContentKeyV2;
}
