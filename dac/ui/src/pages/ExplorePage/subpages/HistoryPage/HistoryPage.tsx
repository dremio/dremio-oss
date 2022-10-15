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
import { connect } from "react-redux";
import { compose } from "redux";
import {
  createNessieContext,
  NessieContext,
} from "@app/pages/NessieHomePage/utils/context";
import { NessieRootState } from "@app/types/nessie";
import { getSortedSources } from "@app/selectors/home";
import { withRouter, WithRouterProps } from "react-router";
import { getTableAndNamespace } from "./utils";
import { getSourceByName } from "@app/utils/nessieUtils";
import TableHistoryContent from "@app/pages/NessieHomePage/components/TableDetailsPage/components/TableHistoryContent/TableHistoryContent";
import { fetchDefaultReference as fetchDefaultReferenceAction } from "@app/actions/nessie/nessie";
import { useEffect } from "react";

import "./HistoryPage.less";

type ConnectedProps = {
  nessie: NessieRootState;
  source?: Record<string, any>;
  namespace: string[];
  tableName: string;

  fetchDefaultReference: typeof fetchDefaultReferenceAction;
};

function HistoryPage({
  source,
  nessie,
  namespace,
  fetchDefaultReference,
}: ConnectedProps & WithRouterProps) {
  const config = source?.config;
  const endpoint = config?.nessieEndpoint;
  const context = createNessieContext(
    { id: source?.id, name: source?.name, endpoint },
    nessie
  );

  useEffect(() => {
    fetchDefaultReference(source?.name, context.api);
  }, [fetchDefaultReference, source?.name, context.api]);

  if (!source) return null;

  return (
    <NessieContext.Provider value={context}>
      <TableHistoryContent path={namespace} />
    </NessieContext.Provider>
  );
}

const mapStateToProps = (state: any, { location }: WithRouterProps) => {
  const [sourceName, namespaceString] = getTableAndNamespace(location.pathname);
  const namespace = (namespaceString || "").split(".");
  namespace.pop();
  const sources = getSortedSources(state);
  const source = getSourceByName(sourceName, sources.toJS());
  return {
    nessie: state.nessie,
    namespace,
    source,
  };
};

const mapDispatchToProps = {
  fetchDefaultReference: fetchDefaultReferenceAction,
};

export default compose(
  withRouter,
  connect(mapStateToProps, mapDispatchToProps)
)(HistoryPage);
