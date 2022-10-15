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
import { connect } from "react-redux";
import {
  fetchDefaultReference as fetchDefaultReferenceAction,
  fetchBranchReference,
} from "@app/actions/nessie/nessie";
// @ts-ignore
import { fetchFeatureFlag } from "@inject/actions/featureFlag";
import { SonarSideNav } from "@app/exports/components/SideNav/SonarSideNav";
import { NessieRootState } from "@app/types/nessie";
import { ViewStateWrapper } from "@app/components/ViewStateWrapper";
import {
  createNessieContext,
  NessieContext as NessieContext,
} from "./utils/context";
import { Branch } from "@app/services/nessie/client";

import "./NessieHomePage.less";
const DATA_OPTIMIZATION = "data_optimization";

type NessieHomePageProps = {
  children: any;
  source: { id: string; name: string; endpoint?: string };
  viewState: any;
  isBareMinimumNessie?: boolean;
  baseUrl?: string;
  initialRef?: Branch;
};

type ConnectedProps = {
  fetchDefaultReference: any;
  fetchBranchReference: any;
  fetchFeatureFlag: any;
  nessie: NessieRootState;
};

function NessieHomePageContent(props: NessieHomePageProps) {
  const Content = props.source ? <HomePageContent {...props} /> : null;

  return props.viewState ? (
    <ViewStateWrapper hideChildrenWhenInProgress viewState={props.viewState}>
      {Content}
    </ViewStateWrapper>
  ) : (
    Content
  );
}

function NessieHomePage(props: NessieHomePageProps) {
  return props.isBareMinimumNessie ? (
    NessieHomePageContent(props)
  ) : (
    <div className="nessieHomePage">
      <SonarSideNav />
      <div className="nessieHomePage-content">
        {NessieHomePageContent(props)}
      </div>
    </div>
  );
}

function HomePageContentUnconnected({
  children,
  fetchDefaultReference,
  fetchBranchReference,
  fetchFeatureFlag,
  nessie,
  source: sourceInfo,
  baseUrl,
  initialRef,
}: NessieHomePageProps & ConnectedProps) {
  const contextValue = createNessieContext(sourceInfo, nessie, "", baseUrl);
  const initReference = useRef<Branch | undefined>(initialRef);

  const { stateKey, api } = contextValue;
  useEffect(() => {
    fetchDefaultReference(stateKey, api);
  }, [fetchDefaultReference, stateKey, api]);

  useEffect(() => {
    // prevent infinite refetching by destructuring params
    fetchBranchReference(stateKey, api, {
      name: initReference.current?.name,
      hash: initReference.current?.hash,
    } as Branch);
  }, [fetchBranchReference, stateKey, api]);

  useEffect(() => {
    // DX-53967: fetchFeatureFlag does not exist in enterprise (backend team uses enterprise for local development)
    if (typeof fetchFeatureFlag === "function") {
      fetchFeatureFlag(DATA_OPTIMIZATION);
    }
  }, [fetchFeatureFlag]);

  return (
    <NessieContext.Provider value={contextValue}>
      {children}
    </NessieContext.Provider>
  );
}

const mapStateToProps = ({ nessie }: any) => ({ nessie });
const mapDispatchToProps = {
  fetchDefaultReference: fetchDefaultReferenceAction,
  fetchBranchReference,
  fetchFeatureFlag,
};
export const HomePageContent = connect(
  mapStateToProps,
  mapDispatchToProps
)(HomePageContentUnconnected);

export default NessieHomePage;
