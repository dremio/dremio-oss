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

import { withRouter } from "react-router";
import { Tabs } from "@mui/material";
import { QueryStatusType, renderTabs } from "./utils";

import "./SqlQueryTabs.less";

type SqlQueryTabsProps = {
  handleTabChange: (arg: any) => void;
  location: {
    state: {
      tabValue: any;
    };
  };
  queryStatuses: QueryStatusType[];
  router: any;
  tabNumber: number;
  statusesArray: [];
  isMultiQueryRunning: boolean;
};

function SqlQueryTabs({
  handleTabChange,
  location,
  queryStatuses,
  router,
  tabNumber,
  statusesArray,
  isMultiQueryRunning,
}: SqlQueryTabsProps): JSX.Element {
  const handleChange = (_: any, value: any) => {
    handleTabChange(value);
  };

  return (
    <div
      className={
        queryStatuses.length > 0
          ? "sql-query-results-tabs"
          : "sql-query-results-tabs--empty"
      }
    >
      {queryStatuses.length > 0 && (
        <Tabs
          indicatorColor="primary"
          textColor="inherit"
          value={tabNumber}
          onChange={handleChange}
          aria-label="sql-query-tabs"
          variant="scrollable"
          scrollButtons="auto"
        >
          {renderTabs(
            queryStatuses,
            location,
            router,
            statusesArray,
            isMultiQueryRunning
          )}
        </Tabs>
      )}
    </div>
  );
}

export default withRouter(SqlQueryTabs);
