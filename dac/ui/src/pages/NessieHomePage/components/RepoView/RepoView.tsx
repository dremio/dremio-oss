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

import { Reference } from "@app/types/nessie";
import { isDefaultReferenceLoading } from "@app/selectors/nessie/nessie";
import { useNessieContext } from "../../utils/context";
import RepoViewHeader from "./components/RepoViewHeader/RepoViewHeader";
import RepoViewBody from "./components/RepoViewBody/RepoViewBody";

import { RepoViewContextType, useRepoViewContext } from "./utils";

import "./RepoView.less";

export const RepoViewContext = createContext({} as RepoViewContextType);

type RepoViewProps = {
  showHeader?: boolean;
};
function RepoView(props: RepoViewProps) {
  const { showHeader = true } = props;
  const { state, api } = useNessieContext();
  const defaultReferenceLoading = isDefaultReferenceLoading(state);
  const repoViewContext = useRepoViewContext(api);
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
        {showHeader && (
          <div className="repo-view-header-div">
            <RepoViewHeader />
          </div>
        )}
        <div className="repo-view-body-div">
          <RepoViewBody hideTitle={showHeader} />
        </div>
      </div>
    </RepoViewContext.Provider>
  );
}

export default RepoView;
