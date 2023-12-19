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
import { EntryV1 as Entry } from "@app/services/nessie/client";
import NessieLink from "@app/pages/NessieHomePage/components/NessieLink/NessieLink";
import { withRouter, type WithRouterProps } from "react-router";
import {
  constructArcticUrl,
  useArcticCatalogContext,
  getIconByType,
} from "@app/exports/pages/ArcticCatalog/arctic-catalog-utils";
import { ARCTIC_ENTITY_PRIVILEGES } from "@inject/featureFlags/flags/ARCTIC_ENTITY_PRIVILEGES";
import { useFeatureFlag } from "@app/exports/providers/useFeatureFlag";
import { useNessieContext } from "@app/pages/NessieHomePage/utils/context";
import ArcticCatalogDataItemSettings from "@inject/pages/ArcticCatalog/components/ArcticCatalogDataItemSettings/ArcticCatalogDataItemSettings";
import "@app/pages/NessieHomePage/components/NamespaceItem/NamespaceItem.less";
import { type EngineStateRead } from "@app/exports/endpoints/ArcticCatalogs/Configuration/CatalogConfiguration.types";
import {
  ICEBERG_TABLE,
  ICEBERG_VIEW,
} from "@inject/pages/ArcticCatalog/components/ArcticCatalogDataItemSettings/settings-utils";
import { Type } from "@app/types/nessie";
import "./ArcticCatalogDataItem.less";

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
  configState,
  params,
}: {
  entry: Entry;
  configState?: EngineStateRead;
} & WithRouterProps) {
  const arcticCtx = useArcticCatalogContext();
  const {
    state: { hash, reference },
  } = useNessieContext();
  const [entityPrivilegesFlag] = useFeatureFlag(ARCTIC_ENTITY_PRIVILEGES);

  if (!entry.name || entry.name.elements.length === 0 || !reference)
    return null;
  const { elements } = entry.name;
  const fullPath = elements.map((c) => encodeURIComponent(c)).join("/");
  const tableId = elements?.join(".");
  const activeTab = arcticCtx.activeTab;
  const url = constructArcticUrl({
    type: arcticCtx.isCatalog ? "catalog" : "source",
    baseUrl: "",
    tab: activeTab,
    namespace: `${encodeURIComponent(params?.branchName)}/${fullPath}`,
    hash: hash ? `?hash=${hash}` : "",
  });
  const isOnTagOrCommit = reference.type === "TAG" || !!hash;
  const renderSettingsButton = (): boolean => {
    if (
      entry.type === ICEBERG_VIEW &&
      entityPrivilegesFlag &&
      !isOnTagOrCommit
    ) {
      return true;
    }
    if (entry.type === ICEBERG_TABLE && entityPrivilegesFlag) {
      return true;
    }
    return false;
  };

  const mainContent = (
    <>
      <ArcticCatalogDataIcon type={entry.type} />
      <span
        className="namespaceItem-name text-ellipsis"
        title={elements.join(".")}
      >
        {elements[elements.length - 1]}
      </span>
      {renderSettingsButton() && (
        <ArcticCatalogDataItemSettings
          refValue={hash ?? reference.name}
          fullPath={elements}
          tableId={tableId}
          catalogId={params?.arcticCatalogId}
          title={elements[elements.length - 1]}
          configState={configState}
          entryType={entry.type}
          isOnTagOrCommit={isOnTagOrCommit}
        />
      )}
    </>
  );

  if (entry.type === Type.Namespace) {
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
