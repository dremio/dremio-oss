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

import sentryUtil from "@app/utils/sentryUtil";
import { SQLScriptsProps } from "./SQLScripts";
import { intl } from "@app/utils/intl";
import getIconColor from "@app/utils/getIconColor";
import { getUserIconInitialsForAllUsers } from "@app/utils/userIcon";
import localStorageUtils from "@app/utils/storageUtils/localStorageUtils";
import * as sqlPaths from "dremio-ui-common/paths/sqlEditor.js";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";

export const ALL_MINE_SCRIPTS_TABS = {
  all: "All",
  mine: "Mine",
};

export const MAX_MINE_SCRIPTS_ALLOWANCE = 100;
export const INITIAL_CALL_VALUE = 100;
export const SEARCH_CALL_VALUE = 500;

export const fetchAllAndMineScripts = (
  fetchCall: (arg: {
    maxResults: number;
    searchTerm: string | null;
    createdBy: string | null;
  }) => Promise<any>,
  search: string
) => {
  const userId = localStorageUtils?.getUserId();
  fetchCall({
    maxResults: search ? SEARCH_CALL_VALUE : INITIAL_CALL_VALUE,
    searchTerm: search || null,
    createdBy: null,
  }).catch((error: any) => {
    const failedErrorLog = sentryUtil.logException(error);
    // @ts-ignore
    if (failedErrorLog) {
      console.error(
        "An error has occurred while making a call in SQLscripts to All:",
        error
      );
    }
  });
  fetchCall({
    maxResults: search ? SEARCH_CALL_VALUE : INITIAL_CALL_VALUE,
    searchTerm: null, // currently a FE search
    createdBy: userId,
  }).catch((error: any) => {
    const failedErrorLog = sentryUtil.logException(error);
    // @ts-ignore
    if (failedErrorLog) {
      console.error(
        "An error has occurred while making a call in SQLscripts to Mine:",
        error
      );
    }
  });
};

export function filterAndSortScripts({
  list,
  search,
  sort,
}: {
  list: any[];
  search: string;
  sort: { category: string; dir: string; compare: any } | null;
}) {
  let tempScripts = list;

  if (search !== "") {
    tempScripts = tempScripts.filter((script) => script.name.includes(search));
  }

  if (sort) {
    tempScripts = tempScripts.sort(sort && sort.compare);
  }

  return tempScripts;
}

function compareSQLString(dir: string): any {
  return (a: any, b: any) => {
    if (a.name.toLowerCase() < b.name.toLowerCase()) {
      return dir === "asc" ? -1 : 1;
    } else if (a.name.toLowerCase() > b.name.toLowerCase()) {
      return dir === "asc" ? 1 : -1;
    } else {
      return 0;
    }
  };
}

export const SCRIPT_SORT_MENU = [
  { category: "Name", dir: "desc", compare: compareSQLString("desc") },
  { category: "Name", dir: "asc", compare: compareSQLString("asc") },
  null,
  {
    category: "Date",
    dir: "desc",
    compare: (a: any, b: any) => b.modifiedAt - a.modifiedAt,
  },
  {
    category: "Date",
    dir: "asc",
    compare: (a: any, b: any) => a.modifiedAt - b.modifiedAt,
  },
];

export const DATETIME_FORMAT = "MM/DD/YYYY HH:mm";

export const prepareScriptsFromList = ({
  list,
  updateActiveScript,
}: {
  list: any[];
  updateActiveScript?: (script: { id: string }) => void;
}) => {
  return list.map((script: any) => {
    updateActiveScript && updateActiveScript(script);

    return {
      ...script,
      colors: getIconColor(script.createdBy.id),
      userNameFirst2: getUserIconInitialsForAllUsers(script.createdBy),
    };
  });
};

export const openPrivilegesModalForScript = ({
  router,
  location,
  script,
  VIEW_ID,
  noDataText,
}: {
  router: any;
  location: any;
  script: any;
  VIEW_ID: string;
  noDataText: string;
}) => {
  return router.push({
    ...location,
    state: {
      modal: "PrivilegesModal",
      accessControlId: script.id,
      entityId: script.id,
      viewId: VIEW_ID,
      entity: script,
      fullPathList: ["scripts", script.id, "grants"],
      showUser: true,
      modalContext: "scripts",
      reFetchOnSave: true,
      disabledTooltipText: intl.formatMessage({
        id: "Scripts.Privileges.Owner.Cannot.Change",
      }),
      noDataText,
    },
  });
};

export const confirmDelete = (
  renderedProps: SQLScriptsProps,
  script: any,
  searchTerm: string
): void => {
  const { intl } = renderedProps;
  const projectId = getSonarContext()?.getSelectedProjectId?.();
  const deleteScript = () => {
    renderedProps
      .deleteScript(script.id)
      .then(() => {
        fetchAllAndMineScripts(renderedProps.fetchSQLScripts, searchTerm);
        if (script.id === renderedProps.activeScript.id) {
          renderedProps.resetQueryState();
          renderedProps.router.push({
            pathname: sqlPaths.sqlEditor.link({ projectId }),
            state: { discard: true },
          });
        }
        return;
      })
      .catch((error: any) => {
        const failedErrorLog = sentryUtil.logException(error);
        // @ts-ignore
        if (failedErrorLog) {
          console.error(
            "An error has occurred while making a call in SQLscripts to delete:",
            error
          ); //specifically for instances of logErrorsToSentry & outsideCommunicationDisabled not allowing a sentry error log to be created
        }
      });
  };

  renderedProps.showConfirmationDialog({
    title: intl.formatMessage({ id: "Script.DeleteConfirm" }),
    confirmText: intl.formatMessage({ id: "Script.DeleteConfirmBtn" }),
    text: intl.formatMessage(
      { id: "Script.DeleteConfirmMessage" },
      { name: script.name }
    ),
    confirm: () => deleteScript(),
    closeButtonType: "XBig",
    className: "--newModalStyles",
    headerIcon: (
      <dremio-icon
        name="interface/warning"
        alt="Warning"
        class="warning-icon"
      />
    ),
  });
};

export const handleDeleteScript = (
  renderedProps: SQLScriptsProps,
  script: any,
  userId: string,
  searchTerm: string
): void => {
  const { intl, activeScript } = renderedProps;
  const deleteId =
    activeScript.id === script.id ? "DeleteThisMessage" : "DeleteOtherMessage";

  renderedProps.showConfirmationDialog({
    title: intl.formatMessage({ id: "Script.Delete" }),
    confirmText: intl.formatMessage({ id: "Common.Delete" }),
    text: intl.formatMessage({ id: `Script.${deleteId}` }),
    confirm: () => confirmDelete(renderedProps, script, searchTerm),
    closeButtonType: "XBig",
    className: "--newModalStyles",
    headerIcon: (
      <dremio-icon
        name="interface/warning"
        alt="Warning"
        class="warning-icon"
      />
    ),
  });
};

export const handleOpenScript = (
  renderedProps: SQLScriptsProps,
  script: any
): void => {
  const {
    activeScript,
    currentSql,
    setActiveScript,
    showConfirmationDialog,
    resetQueryState,
    router,
    intl,
  } = renderedProps;
  const unsavedScriptChanges = !activeScript.id && !!currentSql;
  const editedScript = activeScript.id && currentSql !== activeScript.content;
  const projectId = getSonarContext()?.getSelectedProjectId?.();

  const openScript = () => {
    router.push({
      pathname: sqlPaths.sqlEditor.link({ projectId }),
      state: { discard: true },
    });
    resetQueryState({ exclude: ["currentSql"] });
    setActiveScript({ script });
  };

  if (script.id !== activeScript.id && (unsavedScriptChanges || editedScript)) {
    showConfirmationDialog({
      title: intl.formatMessage({ id: "Common.UnsavedWarning" }),
      confirmText: intl.formatMessage({ id: "Common.Leave" }),
      text: intl.formatMessage({ id: "Common.LeaveMessage" }),
      cancelText: intl.formatMessage({ id: "Common.Stay" }),
      confirm: () => openScript(),
      closeButtonType: "XBig",
      className: "--newModalStyles",
      headerIcon: (
        <dremio-icon
          name="interface/warning"
          alt="Warning"
          class="warning-icon"
        />
      ),
    });
  } else if (script.id !== activeScript.id) {
    openScript();
  }
};
