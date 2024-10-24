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

import { forwardRef } from "react";
import { type MonacoShortcut } from "#oss/pages/ExplorePage/components/SqlEditor/utils/keyboardShortcuts";

export const withExtraSQLEditorContent = <T,>(
  WrappedComponent: React.ComponentClass,
) =>
  forwardRef((props: T, ref: any) => {
    return (
      <WrappedComponent hasExtraSQLPanelContent={false} {...props} ref={ref} />
    );
  });

export const getExtraKeyboardShortcuts = ({
  editor,
  monaco,
  toggleExtraSQLPanel,
}: {
  editor?: Record<string, unknown> | null;
  monaco: any;
  toggleExtraSQLPanel?: () => void;
}): MonacoShortcut[] => {
  return [];
};

export const renderExtraSQLToolbarIcons = ({
  renderIconButton,
  toggleExtraSQLPanel,
  isDarkMode,
}: any) => {
  return null;
};

export const EXTRA_KEYBOARD_BINDINGS_MAC = {};
export const EXTRA_KEYBOARD_BINDINGS_WINDOWS = {};

export const renderExtraSQLKeyboardShortcutMessages = ({
  keyboardShortcuts,
  hasExtraSQLPanelContent,
}: any) => {
  return null;
};

export const renderExtraSQLPanelComponent = (props: any) => {
  return null;
};

export const EXTRA_SQL_TRACKING_EVENT = "panel:opened";
