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
import {
  getEndpointFromSource,
  getSourceByName,
  isArcticCatalogConfig,
} from "@app/utils/nessieUtils";
import TableHistoryContent from "@app/pages/NessieHomePage/components/TableDetailsPage/components/TableHistoryContent/TableHistoryContent";
import { fetchDefaultReferenceIfNeeded as fetchDefaultReferenceAction } from "@app/actions/nessie/nessie";
import { useEffect, useMemo } from "react";
import { rmProjectBase } from "dremio-ui-common/utilities/projectBase.js";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";
import * as commonPaths from "dremio-ui-common/paths/common.js";

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
  tableName,
}: ConnectedProps & WithRouterProps) {
  const config = source?.config;

  const endpoint = getEndpointFromSource(source as any);
  const isArcticConfig = isArcticCatalogConfig(config);

  const context = useMemo(
    () =>
      createNessieContext(
        { id: source?.id, name: source?.name, endpoint },
        nessie,
        undefined,
        isArcticConfig
          ? commonPaths.arcticSource.link({
              sourceName: source?.name,
              projectId: getSonarContext().getSelectedProjectId?.(),
            })
          : undefined
      ),
    [endpoint, nessie, source?.name, source?.id, isArcticConfig]
  );

  useEffect(() => {
    fetchDefaultReference(source?.name, context.apiV2);
  }, [fetchDefaultReference, source?.name, context.apiV2]);

  if (!source) return null;

  return (
    <NessieContext.Provider value={context}>
      <TableHistoryContent path={namespace} tableName={tableName} />
    </NessieContext.Provider>
  );
}

const mapStateToProps = (state: any, { location }: WithRouterProps) => {
  const [sourceName, namespaceString] = getTableAndNamespace(
    rmProjectBase(location.pathname)
  );
  const namespace = (namespaceString || "").split(".");
  const tableName = namespace.pop();
  const sources = getSortedSources(state);
  const source = getSourceByName(sourceName, sources.toJS());
  return {
    nessie: state.nessie,
    namespace,
    source,
    tableName,
  };
};

const mapDispatchToProps = {
  fetchDefaultReference: fetchDefaultReferenceAction,
};

export default compose(
  withRouter,
  connect(mapStateToProps, mapDispatchToProps)
)(HistoryPage);
