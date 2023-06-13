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

import FontIcon from "@app/components/Icon/FontIcon";
import { EntryV1 as Entry } from "@app/services/nessie/client";
import NessieLink from "../NessieLink/NessieLink";

import "./RepoHeader.less";

function RepoHeader({
  entry,
  isLoading,
}: {
  entry: Entry;
  isLoading: boolean;
}) {
  if (!entry.name) return null;
  const [repo, branch] = entry.name.elements;
  return (
    <NessieLink
      to="/"
      className="repoHeader"
      title={isLoading ? "" : `${repo} / ${branch}`}
    >
      <FontIcon type={isLoading ? "Loader spinner" : "DatalakeIcon"} />
      {!isLoading && (
        <>
          <span className="repoHeader-repoName text-ellipsis">{repo}</span>
          <span className="repoHeader-divider">/</span>
          <NessieLink to="/" className="repoHeader-branch">
            <span className="repoHeader-branchIcon">
              <dremio-icon
                name="vcs/branch"
                alt="Reference"
                class="repoHeader-branchIcon--icon"
              />
            </span>
            <span className="repoHeader-branchName text-ellipsis">
              {branch}
            </span>
          </NessieLink>
        </>
      )}
    </NessieLink>
  );
}

export default RepoHeader;
