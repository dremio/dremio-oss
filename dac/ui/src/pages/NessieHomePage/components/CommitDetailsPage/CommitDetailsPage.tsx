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
import Spinner from "@app/components/Spinner";
import { isReqLoading } from "@app/utils/smartPromise";
import { useMemo } from "react";
import { usePromise } from "react-smart-promise";
import CommitDetails from "../CommitDetails/CommitDetails";
import PageBreadcrumbHeader from "../PageBreadcrumbHeader/PageBreadcrumbHeader";
import { useNessieContext } from "../../utils/context";

import "./CommitDetailsPage.less";

function CommitDetailsPage({ params }: { params: any }) {
  const { apiV2 } = useNessieContext();
  const [branchName, commitHash] = useMemo(() => {
    const name = decodeURIComponent(params?.branchName || "");
    const hash = params?.commitHash || "";
    return [name, hash];
  }, [params]);

  const [, data, status] = usePromise(
    useMemo(
      () =>
        !branchName || !commitHash
          ? null
          : () =>
              apiV2.getCommitLogV2({
                ref: branchName,
                filter: `commit.hash == "${commitHash}"`,
              }),
      [branchName, commitHash, apiV2]
    )
  );

  const commit = useMemo(() => {
    const entries = data?.logEntries || [];
    if (entries.length > 0) return entries[0];
    else return null;
  }, [data]);

  const commitMeta = commit?.commitMeta;

  return (
    <div className="commitDetailsPage">
      <PageBreadcrumbHeader hasBranchPicker={false} />
      {isReqLoading(status) && <Spinner />}
      {commitMeta && (
        <CommitDetails
          branch={{ name: branchName, hash: commitHash, type: "BRANCH" }}
          commitMeta={commitMeta}
        />
      )}
    </div>
  );
}

export default CommitDetailsPage;
