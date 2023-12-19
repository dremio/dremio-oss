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

import { TableVersionType } from "../arctic/datasets/TableVersionType.type";
import { getShortHash } from "../utilities/versionContext";

const VERSION_CONTEXT: {[V in TableVersionType]: string} = {
  [TableVersionType.BRANCH]: "branch",
  [TableVersionType.TAG]: "tag",
  [TableVersionType.COMMIT_HASH_ONLY]: "commit",
  [TableVersionType.SNAPSHOT_ID]: "snapshot",
  [TableVersionType.TIMESTAMP]: "timestamp",
  [TableVersionType.LATEST_VERSION]: "branch",
  [TableVersionType.NOT_SPECIFIED]: "branch",
  [TableVersionType.REFERENCE]: "branch",
};

export type VersionContextType = {
  type: keyof typeof TableVersionType;
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
    type === TableVersionType.COMMIT_HASH_ONLY ||
    type === TableVersionType.BRANCH ||
    type === TableVersionType.TAG ||
    withRefKeyword
      ? "Ref:"
      : "";
  const refValue =
    type === TableVersionType.COMMIT_HASH_ONLY
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
