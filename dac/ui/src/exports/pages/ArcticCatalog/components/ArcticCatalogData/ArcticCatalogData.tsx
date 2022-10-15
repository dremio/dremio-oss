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
import { SearchField } from "@app/components/Fields";
import { useIntl } from "react-intl";
import { useMemo, useState } from "react";
import { type WithRouterProps } from "react-router";
import {
  constructArcticUrl,
  parseArcticCatalogUrl,
  useArcticCatalogContext,
} from "../../arctic-catalog-utils";
import { getViewStateFromReq } from "@app/utils/smartPromise";
import StatefulTableViewer from "@app/components/StatefulTableViewer";
import PageBreadcrumbHeader from "@app/pages/NessieHomePage/components/PageBreadcrumbHeader/PageBreadcrumbHeader";
import { useNessieContext } from "@app/pages/NessieHomePage/utils/context";
import useNamespaceList from "@app/pages/NessieHomePage/utils/useNamespaceList";
import { last } from "lodash";
import ArcticCatalogDataItem from "./ArcticCatalogDataItem";
import ProjectHistoryButton from "../ProjectHistoryButton";
import { Skeleton } from "dremio-ui-lib/dist-esm";

import "./ArcticCatalogData.less";
import * as headerClasses from "@app/exports/components/ArcticTableHeader/ArcticTableHeader.module.less";

const ArcticCatalogData = (props: WithRouterProps) => {
  const intl = useIntl();
  const { router, params, location } = props;
  const [searchFilter, setSearchFilter] = useState("");
  const { reservedNamespace, isCatalog } = useArcticCatalogContext() ?? {};
  const {
    state: { hash, reference },
    baseUrl,
    api,
  } = useNessieContext();

  const pushPath = constructArcticUrl({
    type: isCatalog ? "catalog" : "source",
    baseUrl,
    tab: "commits",
    namespace: reservedNamespace ?? "",
    hash: hash ? `?hash=${hash}` : "",
  });

  const columns = [
    {
      key: "name",
      label: intl.formatMessage({ id: "Common.Name" }),
      flexGrow: 1,
      placeholder: <Skeleton width="8ch" />,
    },
  ];

  const path = useMemo(() => {
    return parseArcticCatalogUrl(
      location.pathname,
      `/arctic/${params?.arcticCatalogId}/data/${params?.branchName}`,
      "data",
      params?.branchName
    );
  }, [location, params]);

  const [err, data, status] = useNamespaceList({
    reference: reference ? reference.name : "",
    hash,
    path,
    api,
  });

  const tableData = useMemo(() => {
    const items = data?.entries ?? [];
    const filteredItems = !searchFilter
      ? items
      : items.filter((item) =>
          (last(item?.name?.elements ?? []) ?? "")
            .toLowerCase()
            .includes(searchFilter.toLowerCase())
        );
    return filteredItems.flatMap((entry, i) => {
      return {
        id: i,
        rowClassName: "row" + i,
        data: {
          name: {
            node: () => <ArcticCatalogDataItem entry={entry} />,
          },
        },
      };
    });
  }, [data, searchFilter]);

  return (
    <div className="arctic-catalog-data">
      <PageBreadcrumbHeader
        path={path}
        className={headerClasses["arctic-table-header"]}
        rightContent={
          <span className={headerClasses["arctic-table-header__right"]}>
            <SearchField
              placeholder={intl.formatMessage({
                id: "ArcticCatalog.FilterName",
              })}
              onChange={(val: string) => setSearchFilter(val)}
              showCloseIcon
              showIcon
              className={headerClasses["arctic-search-box"]}
            />
            <ProjectHistoryButton onClick={() => router.push(pushPath)} />
          </span>
        }
      />
      <div className="arctic-catalog-data__content">
        <StatefulTableViewer
          virtualized
          disableZebraStripes
          columns={columns}
          rowHeight={39}
          tableData={tableData}
          viewState={getViewStateFromReq(err, status)}
          className="arctic-data-table"
        />
      </div>
    </div>
  );
};

export default ArcticCatalogData;
