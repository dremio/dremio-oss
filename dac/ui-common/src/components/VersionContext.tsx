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

import { getShortHash } from "../utilities/versionContext";

export enum VersionContextTypes {
  BRANCH = "BRANCH",
  TAG = "TAG",
  COMMIT_HASH_ONLY = "COMMIT_HASH_ONLY",
  SNAPSHOT_ID = "SNAPSHOT_ID",
  TIMESTAMP = "TIMESTAMP",
  LATEST_VERSION = "LATEST_VERSION",
  NOT_SPECIFIED = "NOT_SPECIFIED",
  REFERENCE = "REFERENCE",
}

const VERSION_CONTEXT = {
  [VersionContextTypes.BRANCH]: "branch",
  [VersionContextTypes.TAG]: "tag",
  [VersionContextTypes.COMMIT_HASH_ONLY]: "commit",
  [VersionContextTypes.SNAPSHOT_ID]: "snapshot",
  [VersionContextTypes.TIMESTAMP]: "timestamp",
  [VersionContextTypes.LATEST_VERSION]: "branch",
  [VersionContextTypes.NOT_SPECIFIED]: "branch",
  [VersionContextTypes.REFERENCE]: "branch",
};

export type VersionContextType = {
  type: keyof typeof VersionContextTypes;
  value: string;
};

const VersionContext = ({
  versionContext,
  className,
  withRefKeyword,
}: {
  versionContext: VersionContextType;
  className?: string;
  withRefKeyword?: boolean;
}) => {
  const { type, value } = versionContext;
  const prefix =
    type === VersionContextTypes.COMMIT_HASH_ONLY ||
    type === VersionContextTypes.BRANCH ||
    type === VersionContextTypes.TAG ||
    withRefKeyword
      ? "Ref:"
      : "";
  const refValue =
    type === VersionContextTypes.COMMIT_HASH_ONLY
      ? getShortHash(value || "")
      : value;

  return (
    <p className={className ?? ""}>
      {prefix}
      {/* @ts-ignore */}
      <dremio-icon
        name={`vcs/${VERSION_CONTEXT[type]}`}
        style={{
          height: 16,
          width: 16,
          marginRight: 2,
          color: "var(--dremio--color--text--faded)",
        }}
      />
      {refValue}
    </p>
  );
};

export default VersionContext;
