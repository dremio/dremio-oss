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

import { NotFound } from "#oss/exports/components/ErrorViews/NotFound";
import HomePage from "#oss/pages/HomePage/HomePage";
import { HomePageContent } from "#oss/pages/NessieHomePage/NessieHomePage";
import { getSortedSources } from "#oss/selectors/home";
import { getEndpointFromSource } from "#oss/utils/nessieUtils";
import { useMemo } from "react";
import { connect } from "react-redux";
import { withRouter } from "react-router";
import ViewStateWrapper from "#oss/components/ViewStateWrapper";
import { fromJS } from "immutable";
import * as commonPaths from "dremio-ui-common/paths/common.js";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";
import { isVersionedSoftwareSource } from "@inject/constants/sourceTypes";
import VersionedHomePage from "../VersionedHomePage/VersionedHomePage";

import "#oss/pages/NessieHomePage/components/NessieSourceHomePage/NessieSourceHomePage.less";

function VersionedSourceHomePage(props: any) {
  const sourceInfo = useMemo(() => {
    const source = (props.sources || []).find(
      (item: any) => item.get("name") === props.params.sourceId,
    );
    if (!source) return null;

    return {
      name: source.get("name"),
      id: source.get("id"),
      endpoint: getEndpointFromSource(source.toJS()),
      type: source.get("type"),
    };
  }, [props.params.sourceId, props.sources]);

  const pathProps = {
    sourceName: sourceInfo?.name,
    projectId: getSonarContext().getSelectedProjectId?.(),
  };

  const baseUrl = isVersionedSoftwareSource(sourceInfo?.type)
    ? commonPaths.nessieSource.link(pathProps)
    : commonPaths.arcticSource.link(pathProps);

  return (
    // @ts-ignore
    <HomePage location={props.location}>
      {sourceInfo ? (
        <div className="nessieSourceHomePage">
          <HomePageContent
            key={JSON.stringify(sourceInfo)}
            source={sourceInfo}
            baseUrl={baseUrl}
            viewState={undefined}
            initialRef={{
              name: props.params?.branchName,
              hash: props.location?.query?.hash,
            }}
          >
            {props.children}
          </HomePageContent>
        </div>
      ) : (
        <ViewStateWrapper
          viewState={fromJS({ isInProgress: props.isInProgress })}
          hideChildrenWhenInProgress
          style={{ width: "100%", display: "flex" }}
        >
          <NotFound />
        </ViewStateWrapper>
      )}
    </HomePage>
  );
}

const mapStateToProps = (state: any) => {
  return {
    sources: getSortedSources(state),
    isInProgress: state?.resources?.sourceList?.get("isInProgress"),
  };
};
const ConnectedVersionedSourceHomePage = connect(
  mapStateToProps,
  null,
)(withRouter(VersionedSourceHomePage));
const VersionedSourceWithRoute = withRouter(VersionedHomePage);

const VersionedSourceWithNessie = ({ children }: any) => (
  <ConnectedVersionedSourceHomePage>
    <VersionedSourceWithRoute>{children}</VersionedSourceWithRoute>
  </ConnectedVersionedSourceHomePage>
);

export default VersionedSourceWithNessie;
