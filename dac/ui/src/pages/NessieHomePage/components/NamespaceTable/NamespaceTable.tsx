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
import { useMemo } from "react";
import { useIntl } from "react-intl";

import { getViewStateFromReq } from "@app/utils/smartPromise";
import StatefulTableViewer from "@app/components/StatefulTableViewer";
import useNamespaceList from "../../utils/useNamespaceList";
import NamespaceItem from "../NamespaceItem/NamespaceItem";
import PageBreadcrumbHeader from "../PageBreadcrumbHeader/PageBreadcrumbHeader";
import { useNessieContext } from "../../utils/context";
import NessieLink from "../NessieLink/NessieLink";

import "./NamespaceTable.less";
import { parseNamespaceUrl } from "@app/utils/nessieUtils";

function NamespaceTable({ location }: { location: any }) {
  const {
    state: { hash, reference },
  } = useNessieContext();
  const intl = useIntl();
  const columns = [
    {
      key: "name",
      label: intl.formatMessage({ id: "Common.Name" }),
      flexGrow: 1,
    },
  ];
  const path = useMemo(() => {
    return parseNamespaceUrl(location.pathname, "namespace");
  }, [location]);

  const [err, data, status] = useNamespaceList({
    reference: reference ? reference.name : "",
    hash,
    path,
  });
  const tableData = useMemo(() => {
    return (data?.entries || []).flatMap((entry, i) => {
      return {
        id: i,
        rowClassName: "row" + i,
        data: {
          name: {
            node: () => <NamespaceItem entry={entry} />,
          },
        },
      };
    });
  }, [data]);

  return (
    <div className="namespaceTable">
      <PageBreadcrumbHeader
        path={path}
        rightContent={
          <NessieLink to="/branches">
            {intl.formatMessage({ id: "Nessie.ViewAllBranches" })}
          </NessieLink>
        }
      />
      <div className="namespaceTable-container">
        <StatefulTableViewer
          virtualized
          disableZebraStripes
          columns={columns}
          rowHeight={39}
          tableData={tableData}
          viewState={getViewStateFromReq(err, status)}
        />
      </div>
    </div>
  );
}

export default NamespaceTable;
