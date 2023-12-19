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

import { useMultiTabIsEnabled } from "@app/components/SQLScripts/useMultiTabIsEnabled";

export const SaveScriptAsMenuItem = {
  label: "NewQuery.SaveScriptAs",
  id: "saveScriptAs",
  class: "save-as-menu-item",
};
export const SaveScriptMenuItem = {
  label: "NewQuery.SaveScript",
  id: "saveScript",
  class: "save-menu-item",
};
export const SaveViewMenuItem = {
  label: "NewQuery.SaveView",
  id: "saveView",
  class: "save-menu-item",
};
export const SaveViewAsMenuItem = {
  label: "NewQuery.SaveViewAs",
  id: "saveViewAs",
  class: "save-as-menu-item",
};

export const SaveAsViewMenuItem = {
  label: "NewQuery.SaveAsView",
  id: "saveViewAs",
  class: "save-as-menu-item",
};

export const getTabSaveMenuItms = () => {
  return [SaveScriptAsMenuItem, SaveAsViewMenuItem];
};

export const getSaveMenuItems = ({
  scriptPermissions,
  isUntitledScript,
  isSqlEditorTab,
}: {
  scriptPermissions?: string[];
  isUntitledScript: boolean;
  isSqlEditorTab: boolean;
}) => {
  const canModify = scriptPermissions && scriptPermissions.includes("MODIFY");

  if (isSqlEditorTab) {
    if (isUntitledScript || canModify) {
      return [
        SaveScriptMenuItem,
        SaveScriptAsMenuItem,
        null,
        SaveViewAsMenuItem,
      ];
    } else {
      return [SaveScriptAsMenuItem, null, SaveViewAsMenuItem];
    }
  } else {
    return [SaveViewMenuItem, SaveViewAsMenuItem, null, SaveScriptAsMenuItem];
  }
};
