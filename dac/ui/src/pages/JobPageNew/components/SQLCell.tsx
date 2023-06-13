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
//@ts-ignore
import { Tooltip } from "dremio-ui-lib";
import "./JobsContent.less";

const SQLCell = ({ sql }: { sql: string }) => {
  return (
    <div data-qa="SQLCell">
      <div className="jobsContent__sqlWrapper">
        <Tooltip
          title={
            <div
              onClick={(e) => {
                e.stopPropagation();
              }}
              className="sql-tooltip-content-container dremio-typography-monospace"
            >
              {sql}
            </div>
          }
          placement="bottom-start"
          type="richTooltip"
        >
          <div className="dremio-typography-monospace jobsContent-sql-content">
            {sql}
          </div>
        </Tooltip>
      </div>
    </div>
  );
};

export default SQLCell;
