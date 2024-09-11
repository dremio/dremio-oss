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

import { getSelectedSql } from "dremio-ui-common/sonar/components/Monaco/components/SqlEditor/helpers/getSqlSelections.js";
import {
  previewDatasetSql,
  runDatasetSql,
} from "@app/actions/explore/dataset/run";
import { setActionState } from "@app/actions/explore/view";
import { ExploreHeaderActions } from "@app/pages/ExplorePage/components/ExploreHeaderUtils";
import { store } from "@app/store/store";

export type MonacoShortcut = {
  id: string;
  label: string;
  keybindings?: number[];
  run: (...args: any[]) => void | Promise<void>;
};

const previewAction = (editorInstance: Record<string, unknown>) => {
  store.dispatch(setActionState({ actionState: ExploreHeaderActions.PREVIEW }));
  store.dispatch(
    previewDatasetSql({ selectedSql: getSelectedSql(editorInstance) }),
  );
};

const runAction = (editorInstance: Record<string, unknown>) => {
  store.dispatch(setActionState({ actionState: ExploreHeaderActions.RUN }));
  store.dispatch(
    runDatasetSql({ selectedSql: getSelectedSql(editorInstance) }),
  );
};

/**
 * Returns an array of keyboard shortcuts used in the Explore page
 */
export const getKeyboardShortcuts = ({
  editor,
  monaco,
}: {
  editor?: Record<string, unknown> | null;
  monaco: any;
}): MonacoShortcut[] => {
  const shortcuts = [];

  if (!editor || !monaco) {
    return [];
  }

  shortcuts.push({
    id: "editor.action.preview",
    label: "Preview",
    keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter],
    run: () => previewAction(editor),
  });

  shortcuts.push({
    id: "editor.action.run",
    label: "Run",
    keybindings: [
      monaco.KeyMod.CtrlCmd | monaco.KeyMod.Shift | monaco.KeyCode.Enter,
    ],
    run: () => runAction(editor),
  });

  return shortcuts;
};
