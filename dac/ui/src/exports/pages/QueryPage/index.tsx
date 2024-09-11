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

import { projectBase } from "dremio-ui-common/paths/common.js";
import { SqlRunnerTabs } from "dremio-ui-common/sonar/SqlRunnerSession/components/SqlRunnerTabs.js";
import { Button } from "dremio-ui-lib/components";
import SearchItem from "@app/components/HeaderItemsTypes/SearchItem";
import { renderRoute } from "@app/utils/renderRoute";
import { SqlEditor } from "../../components/MonacoWrappers/SqlEditor";

export const queryPage = projectBase.extend(() => "query");

const SqlEditorView = () => {
  return (
    <div className="flex flex-col w-full h-full">
      <div
        className="p-1"
        style={{
          borderBottom: "1px solid rgb(229, 229, 229)",
        }}
      >
        <div className="dremio-button-group">
          <Button variant="primary" disabled={false}>
            <dremio-icon name="sql-editor/run" alt=""></dremio-icon>
            Run
          </Button>
          <Button variant="secondary" disabled={false}>
            <dremio-icon name="sql-editor/preview" alt=""></dremio-icon>
            Preview
          </Button>
        </div>
      </div>
      <div className="overflow-y-auto h-full">
        <div
          className="flex flex-col m-1 p-1"
          style={{ border: "1px solid var(--border--neutral)" }}
        >
          <SqlEditor
            onChange={(val) => {
              console.log(val);
            }}
            autoResize
          />
        </div>
      </div>
    </div>
  );
};

const QueryNavigatorPane = () => {
  return (
    <div
      className="section-tabs pt-1 px-105"
      style={{ borderBottom: "1px solid rgb(229, 229, 229)" }}
    >
      <div className="section-tab" aria-selected="true">
        Data
      </div>
      <div className="section-tab">Scripts</div>
    </div>
  );
};

export const QueryPage = renderRoute({
  path: queryPage.route(),
  element: (
    <div className="flex flex-col overflow-hidden h-full w-full bg-background">
      <div className="flex items-center p-105" style={{ height: "56px" }}>
        <SearchItem />
      </div>

      <div className="bg-primary flex flex-row h-full overflow-hidden">
        <div
          className="shrink-0 h-full overflow-hidden"
          style={{
            width: "304px",
            borderTop: "1px solid rgb(229, 229, 229)",
            borderRight: "1px solid rgb(229, 229, 229)",
          }}
        >
          <QueryNavigatorPane />
        </div>

        <div
          className="flex flex-col w-full h-full overflow-hidden"
          style={{ borderTop: "1px solid rgb(229, 229, 229)" }}
        >
          <SqlRunnerTabs
            canClose={() => undefined}
            onTabSelected={(tabId) => {}}
            onTabClosed={() => {}}
            tabActions={(tabId) => []}
            onNewTabCreated={async () => {}}
          />
          <SqlEditorView />
        </div>
      </div>
    </div>
  ),
});
