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

import { createContext, useEffect } from "react";

import { Reference } from "#oss/types/nessie";
import { isDefaultReferenceLoading } from "#oss/selectors/nessie/nessie";
import { useNessieContext } from "../../utils/context";
import RepoViewBody from "./components/RepoViewBody/RepoViewBody";

import { RepoViewContextType, useRepoViewContext } from "./utils";

import "./RepoView.less";

export const RepoViewContext = createContext({} as RepoViewContextType);

function RepoView() {
  const { state, apiV2 } = useNessieContext();
  const defaultReferenceLoading = isDefaultReferenceLoading(state);
  const repoViewContext = useRepoViewContext(apiV2);
  const { setDefaultRef } = repoViewContext;

  useEffect(() => {
    state.defaultReference && !defaultReferenceLoading
      ? setDefaultRef({
          type: "BRANCH",
          name: state.defaultReference.name,
          hash: state.defaultReference.hash,
        } as Reference)
      : setDefaultRef({} as Reference);
  }, [state.defaultReference, defaultReferenceLoading, setDefaultRef]);

  return (
    <RepoViewContext.Provider value={repoViewContext}>
      <div className="repo-view">
        <div className="repo-view-body-div">
          <RepoViewBody />
        </div>
      </div>
    </RepoViewContext.Provider>
  );
}

export default RepoView;
