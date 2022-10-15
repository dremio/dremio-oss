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

import { NotFound } from "@app/exports/components/ErrorViews/NotFound";
import HomePage from "@app/pages/HomePage/HomePage";
import { HomePageContent } from "@app/pages/NessieHomePage/NessieHomePage";
import { getSortedSources } from "@app/selectors/home";
import { getEndpointFromSourceConfig } from "@app/utils/nessieUtils";
import { useMemo } from "react";
import { connect } from "react-redux";
import { withRouter } from "react-router";
import { ArcticCatalog } from "../ArcticCatalog/ArcticCatalog";

function ArcticSourceHomePage(props: any) {
  const sourceInfo = useMemo(() => {
    const source = (props.sources || []).find(
      (item: any) => item.get("name") === props.params.sourceId
    );
    if (!source) return null;

    const config = source.get("config");
    return {
      name: source.get("name"),
      id: source.get("id"),
      endpoint: getEndpointFromSourceConfig(config.toJS()),
    };
  }, [props.params.sourceId, props.sources]);

  return (
    // @ts-ignore
    <HomePage location={props.location}>
      {sourceInfo ? (
        <div className="nessieSourceHomePage">
          <HomePageContent
            key={JSON.stringify(sourceInfo)}
            source={sourceInfo}
            baseUrl={`/sources/arctic/${sourceInfo?.name}`}
            viewState={undefined}
            isBareMinimumNessie
            initialRef={{
              name: props.params?.branchName,
              hash: props.location?.query?.hash,
            }}
          >
            {props.children}
          </HomePageContent>
        </div>
      ) : (
        <NotFound />
      )}
    </HomePage>
  );
}

const mapStateToProps = (state: any) => {
  return {
    sources: getSortedSources(state),
  };
};
const ConnectedArcticSourceHomePage = connect(
  mapStateToProps,
  null
)(withRouter(ArcticSourceHomePage));

const ArcticSourceWithRoute = withRouter(ArcticCatalog);
const ArcticSourceWithNessie = ({ children }: any) => (
  <ConnectedArcticSourceHomePage>
    <ArcticSourceWithRoute>{children}</ArcticSourceWithRoute>
  </ConnectedArcticSourceHomePage>
);

export default ArcticSourceWithNessie;
