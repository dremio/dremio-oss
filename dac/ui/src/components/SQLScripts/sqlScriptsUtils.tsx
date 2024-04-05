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
import localStorageUtils from "@app/utils/storageUtils/localStorageUtils";
import * as sqlPaths from "dremio-ui-common/paths/sqlEditor.js";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";
import { nameToInitials } from "@app/exports/utilities/nameToInitials";
import { useSqlRunnerSession } from "dremio-ui-common/sonar/SqlRunnerSession/providers/useSqlRunnerSession.js";
import {
  loadScriptJobs,
  pollScriptJobs,
  setTabView,
} from "@app/actions/resources/scripts";
import { store } from "@app/store/store";
import { getExploreState, selectTabDataset } from "@app/selectors/explore";
import { ScriptsResource } from "dremio-ui-common/sonar/scripts/resources/ScriptsResource.js";
import {
  closeTab,
  selectTab,
} from "dremio-ui-common/sonar/SqlRunnerSession/resources/SqlRunnerSessionResource.js";
import { getLocation } from "@app/selectors/routing";
import { isTabbableUrl } from "@app/utils/explorePageTypeUtils";
import { deleteQuerySelectionsFromStorage } from "@app/sagas/utils/querySelections";

export const ALL_MINE_SCRIPTS_TABS = {
  all: "All",
  mine: "Mine",
};

export const MAX_MINE_SCRIPTS_ALLOWANCE = 1000;
export const INITIAL_CALL_VALUE = 1000;
export const SEARCH_CALL_VALUE = 500;

export const fetchAllAndMineScripts = (
  fetchCall: (arg: {
    maxResults: number;
    searchTerm: string | null;
    createdBy: string | null;
  }) => Promise<any>,
  search: string,
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
        error,
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
        error,
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
      initials: nameToInitials(script.createdBy.name || script.createdBy.email),
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

export const handleDeleteScript = (
  renderedProps: SQLScriptsProps,
  script: any,
  userId: string,
  searchTerm: string,
  openNextScript: any,
  isMultiTabsEnabled: boolean,
): void => {
  const projectId = getSonarContext()?.getSelectedProjectId?.();
  const { intl, activeScript } = renderedProps;
  const deleteScript = () => {
    renderedProps
      .deleteScript(script.id)
      .then(() => {
        deleteQuerySelectionsFromStorage(script.id);
        fetchAllAndMineScripts(renderedProps.fetchSQLScripts, searchTerm);
        ScriptsResource.fetch();
        closeTab(script.id);
        if (isMultiTabsEnabled) {
          if (
            !renderedProps.activeScript.id ||
            renderedProps.activeScript.id === script.id
          ) {
            renderedProps.setActiveScript({ script: {} });
            openNextScript?.();
          }
          return;
        } else {
          if (script.id === renderedProps.activeScript.id) {
            renderedProps.resetQueryState();
            renderedProps.router.push({
              pathname: sqlPaths.sqlEditor.link({ projectId }),
              state: { discard: true },
            });
          }
          return;
        }
      })
      .catch((error: any) => {
        const failedErrorLog = sentryUtil.logException(error);
        // @ts-ignore
        if (failedErrorLog) {
          console.error(
            "An error has occurred while making a call in SQLscripts to delete:",
            error,
          ); //specifically for instances of logErrorsToSentry & outsideCommunicationDisabled not allowing a sentry error log to be created
        }
      });
  };

  const deleteId =
    activeScript.id === script.id ? "DeleteThisMessage" : "DeleteOtherMessage";
  renderedProps.showConfirmationDialog({
    title: intl.formatMessage({ id: "Script.Delete" }),
    confirmText: intl.formatMessage({ id: "Common.Delete" }),
    text: intl.formatMessage({ id: `Script.${deleteId}` }),
    confirm: () => deleteScript(),
    closeButtonType: "CloseBig",
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

function getScriptQueryParams(dataset: any, queryStatuses: any) {
  if (dataset) {
    return {
      tipVersion: dataset.get("tipVersion"),
      version: dataset.get("datasetVersion"),
      jobId: dataset.get("jobId"),
    };
  } else if (queryStatuses?.length) {
    const qStatus = queryStatuses[0];
    return {
      tipVersion: qStatus.version,
      version: qStatus.version,
      jobId: qStatus.jobId,
    };
  } else {
    return {};
  }
}

function doScriptSelect(
  router: any,
  script: any,
  selectTabFunction: any,
  newQueryStatuses?: any,
) {
  const dataset = selectTabDataset(store.getState(), script.id);
  const prevScript = selectActiveScript(store.getState());
  const location = getLocation(store.getState());

  // If a script is already open and a different one is selected, immediately update the script
  // in the store and sync the changes
  if (prevScript.id) {
    store.dispatch({
      type: "REPLACE_SCRIPT_CONTENTS",
      scriptId: prevScript.id,
      script: {
        ...prevScript,
        content: getExploreState(store.getState()).view.currentSql,
      },
    });
  }

  // Switch tabs and run side-effects prior to changing URL
  store.dispatch(setTabView(script, prevScript));
  selectTabFunction(script.id);
  //Begin polling after a delay since EXPLORE_PAGE_LOCATION_CHANGED cancels job polling, polling will wait for the page change below
  store.dispatch(pollScriptJobs(script, newQueryStatuses));

  const action = isTabbableUrl(location) ? router.replace : router.push;
  action({
    query: {
      scriptId: script.id,
      ...getScriptQueryParams(dataset, newQueryStatuses),
    },
    pathname: sqlPaths.sqlEditor.link({
      projectId: getSonarContext()?.getSelectedProjectId?.(),
    }),
    state: { discard: true },
  });

  // load a script's saved jobs when changing tabs if they aren't loaded already
  if (!dataset) {
    store.dispatch(loadScriptJobs(script));
  }
}

export const handleOpenTabScript =
  (router: any) => (script: any, newQueryStatuses: any) => {
    doScriptSelect(router, script, selectTab, newQueryStatuses);
  };

export const handleOpenScript =
  (context: {
    sqlRunnerSession: ReturnType<typeof useSqlRunnerSession>;
    multiTabEnabled: boolean;
  }) =>
  (renderedProps: SQLScriptsProps, script: any): void => {
    const { activeScript, currentSql, showConfirmationDialog, router, intl } =
      renderedProps;
    const unsavedScriptChanges = !activeScript.id && !!currentSql;
    const editedScript = activeScript.id && currentSql !== activeScript.content;

    const openScript = () => {
      doScriptSelect(router, script, context.sqlRunnerSession.selectTab);
    };

    if (activeScript.id === script.id) return;

    if (context.multiTabEnabled) {
      openScript();
      return;
    }

    if (
      script.id !== activeScript.id &&
      (unsavedScriptChanges || editedScript)
    ) {
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

export const selectActiveScript = (state: any) =>
  state.modulesState["explorePage"]?.data?.view?.activeScript;

export const selectCurrentSql = (state: any) =>
  state.modulesState["explorePage"]?.data?.view?.currentSql;

export const selectCurrentContext = (state: any) =>
  state.modulesState["explorePage"]?.data?.view?.queryContext;
