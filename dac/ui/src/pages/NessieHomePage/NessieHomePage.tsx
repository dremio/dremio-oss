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
import { useEffect, useMemo, useRef } from "react";
import { connect } from "react-redux";
import {
  fetchDefaultReferenceIfNeeded as fetchDefaultReferenceAction,
  fetchBranchReference,
} from "@app/actions/nessie/nessie";
// @ts-ignore
import { fetchFeatureFlag } from "@inject/actions/featureFlag";
import { NessieRootState } from "@app/types/nessie";
import { ViewStateWrapper } from "@app/components/ViewStateWrapper";
import {
  createNessieContext,
  NessieContext as NessieContext,
} from "./utils/context";
import { Branch } from "@app/services/nessie/client";

type NessieHomePageProps = {
  children: any;
  source: { id: string; name: string; endpoint?: string; endpointV1?: string };
  viewState: any;
  baseUrl?: string;
  initialRef?: Branch;
  statePrefix?: string;
};

type ConnectedProps = {
  fetchDefaultReference: any;
  fetchBranchReference: any;
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

function HomePageContentUnconnected({
  children,
  fetchDefaultReference,
  fetchBranchReference,
  nessie,
  source: sourceInfo,
  baseUrl,
  initialRef,
  statePrefix = "",
}: NessieHomePageProps & ConnectedProps) {
  const contextValue = useMemo(
    () => createNessieContext(sourceInfo, nessie, statePrefix, baseUrl),
    [baseUrl, nessie, sourceInfo, statePrefix]
  );
  const initReference = useRef<Branch | undefined>(initialRef);

  const { stateKey, apiV2 } = contextValue;
  useEffect(() => {
    fetchDefaultReference(stateKey, apiV2);
  }, [fetchDefaultReference, stateKey, apiV2]);

  useEffect(() => {
    // prevent infinite refetching by destructuring params
    fetchBranchReference(stateKey, apiV2, {
      name: initReference.current?.name,
      hash: initReference.current?.hash,
    } as Branch);
  }, [fetchBranchReference, stateKey, apiV2]);

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

export default NessieHomePageContent;
