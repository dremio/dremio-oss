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
import { useEffect } from "react";
import { connect } from "react-redux";
import { fetchDefaultReference as fetchDefaultReferenceAction } from "@app/actions/nessie/nessie";
import SideNav from "@app/components/SideNav/SideNav";
import { NessieRootState } from "@app/reducers/nessie/nessie";
import { ViewStateWrapper } from "@app/components/ViewStateWrapper";
import {
  createNessieContext,
  NessieContext as NessieContext,
} from "./utils/context";

import "./NessieHomePage.less";

type NessieHomePageProps = {
  children: any;
  source: { id: string; name: string; endpoint?: string };
  viewState: any;
};

type ConnectedProps = {
  fetchDefaultReference: any;
  nessie: NessieRootState;
};

function NessieHomePage(props: NessieHomePageProps) {
  const Content = props.source ? <HomePageContent {...props} /> : null;
  return (
    <div className="nessieHomePage">
      <SideNav />
      <div className="nessieHomePage-content">
        {props.viewState ? (
          <ViewStateWrapper
            hideChildrenWhenInProgress
            viewState={props.viewState}
          >
            {Content}
          </ViewStateWrapper>
        ) : (
          Content
        )}
      </div>
    </div>
  );
}

function HomePageContentUnconnected({
  children,
  fetchDefaultReference,
  nessie,
  source: sourceInfo,
}: NessieHomePageProps & ConnectedProps) {
  const contextValue = createNessieContext(sourceInfo, nessie);

  const { stateKey, api } = contextValue;
  useEffect(() => {
    fetchDefaultReference(stateKey, api);
  }, [fetchDefaultReference, stateKey, api]);

  return (
    <NessieContext.Provider value={contextValue}>
      {children}
    </NessieContext.Provider>
  );
}

const mapStateToProps = ({ nessie }: any) => ({ nessie });
const mapDispatchToProps = {
  fetchDefaultReference: fetchDefaultReferenceAction,
};
export const HomePageContent = connect(
  mapStateToProps,
  mapDispatchToProps
)(HomePageContentUnconnected);

export default NessieHomePage;
