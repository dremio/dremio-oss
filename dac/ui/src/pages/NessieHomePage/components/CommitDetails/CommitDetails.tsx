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
import { useIntl } from "react-intl";
import CommitHash from "#oss/pages/HomePage/components/BranchPicker/components/CommitBrowser/components/CommitHash/CommitHash";
import UserIcon from "#oss/pages/HomePage/components/BranchPicker/components/CommitBrowser/components/UserIcon/UserIcon";
import { formatDate } from "#oss/utils/date";
import { CommitMetaV2 as CommitMeta } from "#oss/services/nessie/client";
import { Reference } from "#oss/types/nessie";

import "./CommitDetails.less";

function CommitDetails({
  commitMeta,
  branch,
}: {
  commitMeta: CommitMeta;
  branch: Reference;
}) {
  const intl = useIntl();

  const author = commitMeta.authors?.[0];
  return (
    <span className="commitDetails">
      <div className="commitDetails-header">
        {commitMeta.hash && (
          <span className="commitDetails-hash">
            <CommitHash branch={branch} hash={commitMeta.hash} />
          </span>
        )}
        <span
          className="commitDetails-desc text-ellipsis"
          title={commitMeta.message}
        >
          {commitMeta.message}
        </span>
      </div>
      <div className="commitDetails-content">
        <span className="commitDetails-details">
          {author && (
            <span className="commitDetails-metaSection">
              <div className="commitDetails-metaHeader">
                {intl.formatMessage({ id: "Common.Author" })}
              </div>
              <div
                className="commitDetails-userInfo commitDetails-metaDetail"
                title={author}
              >
                <UserIcon user={author} />
                <span className="commitDetails-userName text-ellipsis">
                  {author}
                </span>
              </div>
            </span>
          )}
          {commitMeta.commitTime && (
            <span className="commitDetails-metaSection">
              <div className="commitDetails-metaHeader">
                {intl.formatMessage({ id: "Common.CommitTime" })}
              </div>
              <div className="commitDetails-commitTime commitDetails-metaDetail">
                {formatDate(commitMeta.commitTime + "", "MM/DD/YYYY hh:mm A")}
              </div>
            </span>
          )}
        </span>
      </div>
    </span>
  );
}

export default CommitDetails;
