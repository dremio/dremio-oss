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
import { useEffect, useMemo } from "react";
import { connect } from "react-redux";

import { getSortedSources } from "@app/selectors/home";
import { getViewState } from "@app/selectors/resources";
import { loadSourceListData as loadSourceListDataAction } from "@app/actions/resources/sources";
import HomePage from "@app/pages/HomePage/HomePage";
import { HomePageContent } from "../../NessieHomePage";

import "./NessieSourceHomePage.less";

function NessieSourceHomePage({
  sourcesViewState,
  loadSourceListData,
  ...props
}: any) {
  const sourceInfo = useMemo(() => {
    const source = (props.sources || []).find(
      (item: any) => item.get("name") === props.params.sourceId
    );
    if (!source || !source.get("config")) return null;
    return {
      name: source.get("name"),
      id: source.get("id"),
      endpoint: source.get("config").get("nessieEndpoint"),
    };
  }, [props.params.sourceId, props.sources]);

  useEffect(() => {
    loadSourceListData();
  }, [loadSourceListData]);

  return (
    <HomePage location={props.location}>
      {sourceInfo && (
        <div className="nessieSourceHomePage">
          <HomePageContent
            key={JSON.stringify(sourceInfo)}
            source={sourceInfo}
            viewState={sourcesViewState}
          >
            {props.children}
          </HomePageContent>
        </div>
      )}
    </HomePage>
  );
}

const mapStateToProps = (state: any) => {
  return {
    sources: getSortedSources(state),
    sourcesViewState: getViewState(state, "AllSources"),
  };
};
const mapDispatchToProps = {
  loadSourceListData: loadSourceListDataAction,
};
export default connect(
  mapStateToProps,
  mapDispatchToProps
)(NessieSourceHomePage);
