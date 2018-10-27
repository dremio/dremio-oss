/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
export const SHOW_CONFIRMATION_DIALOG = 'SHOW_CONFIRMATION_DIALOG';
export const HIDE_CONFIRMATION_DIALOG = 'HIDE_CONFIRMATION_DIALOG';

export function showConfirmationDialog({
  hideCancelButton = false, showOnlyConfirm = false, ...params
}) {
  return {
    type: SHOW_CONFIRMATION_DIALOG,
    hideCancelButton,
    showOnlyConfirm,
    ...params
  };
}

export function hideConfirmationDialog() {
  return { type: HIDE_CONFIRMATION_DIALOG };
}

export function showUnsavedChangesConfirmDialog({text, confirm }) {
  return (dispatch) => dispatch(showConfirmationDialog({
    title: la('Unsaved Changes'),
    confirmText: la('Leave'),
    cancelText: la('Stay'),
    text: text || la('You have unsaved changes. Are you sure you want to leave?'),
    confirm
  }));
}

export function showConflictConfirmationDialog({text, confirm} = {}) {
  return (dispatch) => dispatch(showConfirmationDialog({
    title: la('Configuration Modified'),
    text: text ||
      la('This configuration has been modified. To continue, Dremio must update to the latest configuration.'),
    hideCancelButton: true,
    confirmText: la('Update'),
    showOnlyConfirm: true,
    confirm
  }));
}

export function showClearReflectionDialog({confirm, reflectionName} = {}) {
  return (dispatch) => dispatch(showConfirmationDialog({
    title: la('Remove Reflection'),
    confirmText: la('Remove'),
    text: la(`Are you sure you want to remove Reflection “${reflectionName}”?`), // todo: sub loc
    confirm
  }));
}
