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
import { NamespaceIcon } from "../../../NamespaceItem/NamespaceItem";

import "./TableHistoryHeader.less";

function TableHistoryHeader({
  namespace,
  tableName,
  type,
}: {
  namespace: string;
  tableName: string;
  type: string;
}) {
  const intl = useIntl();
  return (
    <div className="tableDetailsPage-header">
      <span className="tableDetailsPage-tableHeader">
        <NamespaceIcon type={type} />
        <div className="tableDetailsPage-tableFullName">
          <div
            className="tableDetailsPage-tableName text-ellipsis"
            title={tableName}
          >
            {tableName}
          </div>
          <div
            className="tableDetailsPage-tableNamespace text-ellipsis"
            title={namespace}
          >
            {namespace}
          </div>
        </div>
      </span>
      <span className="tableDetailsPage-tabs">
        <span className="tableDetailsPage-historyTab">
          <FontIcon type="Clock" />
          {intl.formatMessage({ id: "Common.History" })}
        </span>
      </span>
    </div>
  );
}

export default TableHistoryHeader;
