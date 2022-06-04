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

import Art from '../Art';
import { SQLScriptsProps } from './SQLScripts';

function compareSQLString(dir: string): any {
  return (a: any, b: any) => {
    if (a.name.toLowerCase() < b.name.toLowerCase()) {
      return dir === 'asc' ? -1 : 1;
    } else if (a.name.toLowerCase() > b.name.toLowerCase()) {
      return dir === 'asc' ? 1 : -1;
    } else {
      return 0;
    }
  };
}

export const SCRIPT_SORT_MENU = [
  { category: 'Name', dir: 'desc', compare: compareSQLString('desc')},
  { category: 'Name', dir: 'asc', compare: compareSQLString('asc')},
  null,
  { category: 'Date', dir: 'desc', compare: (a: any, b: any) => b.modifiedAt - a.modifiedAt},
  { category: 'Date', dir: 'asc', compare: (a: any, b: any) => a.modifiedAt - b.modifiedAt}
];

export const DATETIME_FORMAT = 'MM/DD/YYYY HH:mm';

export const confirmDelete = (renderedProps: SQLScriptsProps, script: any): void => {
  const { intl } = renderedProps;
  const deleteScript = () => {
    renderedProps.deleteScript(script.id).then(
      () => {
        renderedProps.fetchSQLScripts();
        if (script.id === renderedProps.activeScript.id) {
          renderedProps.setActiveScript({ script: {} });
          renderedProps.router.push({ pathname: '/new_query', state: { discard: true }});
        }
      }
    );
  };

  renderedProps.showConfirmationDialog({
    title: intl.formatMessage({ id: 'Script.DeleteConfirm'}),
    confirmText: intl.formatMessage({ id: 'Script.DeleteConfirmBtn'}),
    text: intl.formatMessage({ id: 'Script.DeleteConfirmMessage'}, { name: script.name }),
    confirm: () => deleteScript(),
    closeButtonType: 'XBig',
    className: '--newModalStyles',
    headerIcon: <Art title='Warning' src='CircleWarning.svg' alt='Warning' style={{ height: 24, width: 24, marginRight: 8 }} />
  });
};

export const handleDeleteScript = (renderedProps: SQLScriptsProps, script: any): void => {
  const { intl, activeScript } = renderedProps;
  const deleteId = activeScript.id === script.id ? 'DeleteThisMessage' : 'DeleteOtherMessage';
  renderedProps.showConfirmationDialog({
    title: intl.formatMessage({ id: 'Script.Delete'}),
    confirmText: intl.formatMessage({ id: 'Common.Delete'}),
    text: intl.formatMessage({ id: `Script.${deleteId}`}),
    confirm: () => confirmDelete(renderedProps, script),
    closeButtonType: 'XBig',
    className: '--newModalStyles',
    headerIcon: <Art title='Warning' src='CircleWarning.svg' alt='Warning' style={{ height: 24, width: 24, marginRight: 8 }} />
  });
};

export const handleOpenScript = (renderedProps: SQLScriptsProps, script: any): void => {
  const { activeScript, currentSql, setActiveScript, showConfirmationDialog, intl } = renderedProps;
  const unsavedScriptChanges = !activeScript.id && !!currentSql;
  const editedScript = activeScript.id && currentSql !== activeScript.content;
  if (script.id !== activeScript.id && (unsavedScriptChanges || editedScript)) {
    showConfirmationDialog({
      title: intl.formatMessage({ id: 'Common.UnsavedWarning' }),
      confirmText: intl.formatMessage({ id: 'Common.Leave' }),
      text: intl.formatMessage({ id: 'Common.LeaveMessage' }),
      cancelText: intl.formatMessage({ id: 'Common.Stay' }),
      confirm: () => setActiveScript({ script }),
      closeButtonType: 'XBig',
      className: '--newModalStyles',
      headerIcon: <Art title='Warning' src='CircleWarning.svg' alt='Warning' style={{ height: 24, width: 24, marginRight: 8 }} />
    });
  } else {
    setActiveScript({ script });
  }
};
