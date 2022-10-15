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
import { CopyToClipboard } from "dremio-ui-lib";
import NessieLink from "@app/pages/NessieHomePage/components/NessieLink/NessieLink";
import { getShortHash } from "@app/utils/nessieUtils";

import "./CommitHash.less";

function CommitHash({
  branch,
  hash,
  disabled,
  enableCopy = true,
}: {
  disabled?: boolean;
  branch: string;
  hash: string;
  enableCopy?: boolean;
}) {
  return (
    <div className="commitEntryHash">
      <NessieLink
        disabled={disabled}
        to={`/commit/${encodeURIComponent(branch)}/${hash}`}
        title={hash}
      >
        {getShortHash(hash)}
      </NessieLink>
      {enableCopy && <CopyToClipboard tooltipText="Copied" value={hash} />}
    </div>
  );
}
export default CommitHash;
