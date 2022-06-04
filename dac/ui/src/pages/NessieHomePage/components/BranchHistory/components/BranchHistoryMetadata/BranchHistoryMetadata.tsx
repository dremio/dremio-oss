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

import { useContext } from "react";
import { FormattedMessage } from "react-intl";

import CommitDivergenceGraph from "./components/CommitDivergenceGraph/CommitDivergenceGraph";

import { BranchHistoryContext } from "../../BranchHistory";
import { countUniqueAuthors } from "./utils";

import "./BranchHistoryMetadata.less";

function BranchHistoryMetadata(): JSX.Element {
  const { commitLog, currentRef, defaultRef } =
    useContext(BranchHistoryContext);

  return (
    <div className="branch-history-metadata">
      <span className="branch-history-metadata-commits">
        <div className="branch-history-metadata-commits-title">
          <FormattedMessage id="RepoView.ViewCommits" />
        </div>
        <div className="branch-history-metadata-commits-count">
          {currentRef.metadata && currentRef.metadata.numTotalCommits}
        </div>
      </span>
      <span className="branch-history-metadata-authors">
        <div className="branch-history-metadata-authors-title">
          <FormattedMessage id="BranchHistory.Metadata.Authors" />
        </div>
        <div className="branch-history-metadata-authors-count">
          {countUniqueAuthors(commitLog.logEntries)}
        </div>
      </span>
      {defaultRef.name !== currentRef.name && currentRef.metadata && (
        <span className="branch-history-metadata-diff">
          <div className="branch-history-metadata-diff-title">
            <FormattedMessage id="BranchHistory.Metadata.Divergence" />
          </div>
          <div className="branch-history-metadata-diff-count">
            <CommitDivergenceGraph
              commitsBehind={currentRef.metadata.numCommitsBehind}
              commitsAhead={currentRef.metadata.numCommitsAhead}
            />
          </div>
        </span>
      )}
    </div>
  );
}

export default BranchHistoryMetadata;
