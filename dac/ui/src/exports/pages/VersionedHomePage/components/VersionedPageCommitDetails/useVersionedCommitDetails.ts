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

import { useEffect, useRef } from "react";
import { SmartResource } from "smart-resource";
import { useResourceSnapshot, useResourceStatus } from "smart-resource/react";
import { useNessieContext } from "#oss/pages/NessieHomePage/utils/context";

export const useVersionedCommitDetails = (
  commitId: string,
  branchName?: string,
) => {
  const { apiV2 } = useNessieContext();

  const VersionedCommitDetailsResource = useRef(
    new SmartResource(({ name, hash }) =>
      apiV2.getCommitLogV2({
        ref: name,
        filter: `commit.hash=='${hash}'`,
      }),
    ),
  );

  useEffect(() => {
    if (branchName) {
      VersionedCommitDetailsResource.current.fetch({
        name: branchName,
        hash: commitId,
      });
    }
  }, [branchName, commitId]);

  return [
    ...useResourceSnapshot(VersionedCommitDetailsResource.current),
    useResourceStatus(VersionedCommitDetailsResource.current),
  ] as const;
};
