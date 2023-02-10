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

import FontIcon from "@app/components/Icon/FontIcon";
import { useIntl } from "react-intl";
import { Entry } from "@app/services/nessie/client";
import NessieLink from "@app/pages/NessieHomePage/components/NessieLink/NessieLink";
import { withRouter, type WithRouterProps } from "react-router";
import {
  constructArcticUrl,
  getArcticTabFromPathname,
  useArcticCatalogContext,
  getIconByType,
} from "@app/exports/pages/ArcticCatalog/arctic-catalog-utils";
import { useNessieContext } from "@app/pages/NessieHomePage/utils/context";

import "@app/pages/NessieHomePage/components/NamespaceItem/NamespaceItem.less";

type ArcticCatalogDataIconProps = {
  type: string | null;
};

export function ArcticCatalogDataIcon({ type }: ArcticCatalogDataIconProps) {
  const intl = useIntl();
  const { type: iconType, id } = getIconByType(type);
  return <FontIcon type={iconType} tooltip={intl.formatMessage({ id })} />;
}

function ArcticCatalogDataItem({
  entry,
  location,
  params,
}: {
  entry: Entry;
} & WithRouterProps) {
  const arcticCtx = useArcticCatalogContext();
  const {
    state: { hash },
  } = useNessieContext();
  if (!entry.name || entry.name.elements.length === 0) return null;
  const { elements } = entry.name;
  const fullPath = elements.map((c) => encodeURIComponent(c)).join("/");
  const activeTab = arcticCtx
    ? arcticCtx?.activeTab
    : getArcticTabFromPathname(location.pathname) ?? "data";

  const url = constructArcticUrl({
    type: arcticCtx?.isCatalog ? "catalog" : "source",
    baseUrl: "",
    tab: activeTab,
    namespace: `${params?.branchName}/${fullPath}`,
    hash: hash ? `?hash=${hash}` : "",
  });

  const mainContent = (
    <>
      <ArcticCatalogDataIcon type={entry.type} />
      <span
        className="namespaceItem-name text-ellipsis"
        title={elements.join(".")}
      >
        {elements[elements.length - 1]}
      </span>
    </>
  );

  if (entry.type === "NAMESPACE") {
    return (
      <NessieLink className="namespaceItem" to={url}>
        {mainContent}
      </NessieLink>
    );
  } else {
    return <span className="namespaceItem">{mainContent}</span>;
  }
}

export default withRouter(ArcticCatalogDataItem);
