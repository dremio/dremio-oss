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
//@ts-ignore
import NessieLink from "@app/pages/NessieHomePage/components/NessieLink/NessieLink";
import { getShortHash } from "@app/utils/nessieUtils";
import { useDispatch } from "react-redux";
import { setReference } from "@app/actions/nessie/nessie";
import { Reference } from "@app/types/nessie";
import { useNessieContext } from "@app/pages/NessieHomePage/utils/context";
import { CopyButton } from "dremio-ui-lib/components";

import "./CommitHash.less";

function CommitHash({
  branch,
  hash,
  disabled,
  enableCopy = true,
}: {
  disabled?: boolean;
  branch: Reference;
  hash: string;
  enableCopy?: boolean;
}) {
  const dispatch = useDispatch();
  const nessieCtx = useNessieContext();
  const nessieBranch = nessieCtx.state.reference?.name;
  const isDifferentBranch = nessieBranch && nessieBranch !== branch.name;

  const switchBranchRef = () => {
    dispatch(
      setReference(
        {
          reference: {
            ...branch,
          } as Reference,
        },
        nessieCtx.stateKey,
      ),
    );
  };

  return (
    <div className="commitEntryHash">
      <NessieLink
        disabled={disabled}
        to={`/commit/${encodeURIComponent(branch.name)}/${hash}`}
        title={hash}
        {...(isDifferentBranch && { onClick: () => switchBranchRef() })}
        aria-label={`Details for commit ${hash}`}
      >
        {getShortHash(hash)}
      </NessieLink>
      {enableCopy && <CopyButton contents={hash} />}
    </div>
  );
}
export default CommitHash;
