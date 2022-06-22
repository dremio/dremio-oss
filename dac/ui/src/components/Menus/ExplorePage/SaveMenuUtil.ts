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

export const getSaveMenuItems = ({
  saveMenuItems,
  permissionsFromScript,
  isUntitledScript,
  isSqlEditorTab,
}: {
  saveMenuItems: any[];
  permissionsFromScript: any[];
  isUntitledScript: boolean;
  isSqlEditorTab: boolean;
}) => {
  let list;
  if (isUntitledScript && isSqlEditorTab) {
    list = [saveMenuItems[1], saveMenuItems[0], null, saveMenuItems[3]];
    return list;
  }

  if (
    permissionsFromScript &&
    permissionsFromScript.includes("MODIFY") &&
    isSqlEditorTab
  ) {
    list = [saveMenuItems[1], saveMenuItems[0], null, saveMenuItems[3]];
  } else if (
    permissionsFromScript &&
    !permissionsFromScript.includes("MODIFY") &&
    isSqlEditorTab
  ) {
    list = [saveMenuItems[0], null, saveMenuItems[3]];
  } else {
    list = [saveMenuItems[2], saveMenuItems[3], null, saveMenuItems[0]];
  }
  return list;
};
