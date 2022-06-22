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
import { Reference } from "@app/services/nessie/client";
import { getFullPathByType, getShortHash } from "@app/utils/nessieUtils";
import classNames from "classnames";
import RefIcon from "../RefIcon/RefIcon";

import "./BranchPickerTag.less";

type TagContentProps = {
  reference: Reference;
  hash?: string | null;
};

export function TagContent({ reference, hash }: TagContentProps) {
  return (
    <>
      <div className="branchPickerTag-iconDiv">
        <RefIcon reference={reference} hash={hash} />
      </div>
      <div className="branchPickerTag-labelDiv text-ellipsis">
        {hash ? getShortHash(hash) : reference.name}
      </div>
    </>
  );
}

function BranchPickerTag({
  reference,
  hash,
  isOpen,
}: TagContentProps & { isOpen?: boolean }) {
  return (
    <div
      className={classNames("branchPickerTag", { isOpen })}
      title={getFullPathByType(reference.name, hash)}
    >
      <TagContent reference={reference} hash={hash} />
    </div>
  );
}

export default BranchPickerTag;
