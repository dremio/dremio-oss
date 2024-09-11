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

import "./ExplorePageContentWrapper.less";

import { createRef, PureComponent } from "react";
import moize from "moize";
import Immutable, { Map } from "immutable";
import { connect } from "react-redux";
import PropTypes from "prop-types";
import classNames from "clsx";
import Mousetrap from "mousetrap";
import { cloneDeep, debounce } from "lodash";
import { withRouter } from "react-router";
import { injectIntl } from "react-intl";
import { compose } from "redux";

import { getQueryStatuses } from "@app/selectors/jobs";

import DataGraph from "@inject/pages/ExplorePage/subpages/datagraph/DataGraph";
import DetailsWizard from "components/Wizards/DetailsWizard";
import { MARGIN_PANEL } from "uiTheme/radium/sizes";

import { RECOMMENDED_JOIN } from "@app/constants/explorePage/joinTabs";
import { Wiki } from "@app/pages/ExplorePage/components/Wiki/Wiki";
import { PageTypes, pageTypesProp } from "@app/pages/ExplorePage/pageTypes";
import {
  getApproximate,
  getDatasetEntityId,
  getExploreState,
  getJobProgress,
} from "@app/selectors/explore";
import { getJobSummaries } from "@app/selectors/exploreJobs";
import {
  loadJobResults,
  loadJobTabs,
  removeTabView,
} from "@app/actions/resources/scripts";
import { runDatasetSql, previewDatasetSql } from "actions/explore/dataset/run";
import { loadSourceListData } from "actions/resources/sources";
import {
  navigateToExploreDefaultIfNecessary,
  constructFullPath,
} from "utils/pathUtils";
import { getExploreViewState } from "@app/selectors/resources";
import Reflections from "@app/pages/ExplorePage/subpages/reflections/Reflections";
import SQLScriptLeaveTabDialog from "@app/components/SQLScripts/components/SQLScriptLeaveTabDialog/SQLScriptLeaveTabDialog";
import {
  updateColumnFilter,
  setCurrentSql,
  setCustomDefaultSql,
  setPreviousAndCurrentSql,
  setSelectedSql,
  setQuerySelections,
} from "@app/actions/explore/view";
import {
  toggleExploreSql,
  setResizeProgressState,
} from "@app/actions/explore/ui";

import NavCrumbs, {
  showNavCrumbs,
} from "@inject/components/NavCrumbs/NavCrumbs";
import { handleOnTabRouting } from "@app/pages/ExplorePage/components/SqlEditor/SqlQueryTabs/utils.tsx";
import {
  setQueryStatuses as setQueryStatusesFunc,
  setQueryTabNumber,
} from "actions/explore/view";
import { cancelJobAndShowNotification } from "@app/actions/jobs/jobs";
import {
  clearExploreJobs,
  fetchJobDetails,
  fetchJobSummary,
} from "@app/actions/explore/exploreJobs";
import {
  extractQueries,
  extractSelections,
} from "@app/utils/statements/statementParser";
import HistoryLineController from "../components/Timeline/HistoryLineController";
import DatasetsPanel from "../components/SqlEditor/Sidebar/DatasetsPanel";
import ExploreInfoHeader from "../components/ExploreInfoHeader";
import ExploreHeader from "../components/ExploreHeader";
import { withDatasetChanges } from "../DatasetChanges";
import SqlQueryTabs from "../components/SqlEditor/SqlQueryTabs/SqlQueryTabs";
import SqlErrorSection from "./../components/SqlEditor/SqlErrorSection";

import ExploreTableController from "./../components/ExploreTable/ExploreTableController";
import JoinTables from "./../components/ExploreTable/JoinTables";
import TableControls from "./../components/TableControls";
import ExplorePageMainContent from "./ExplorePageMainContent";
import {
  assemblePendingOrRunningTabContent,
  EXPLORE_HEADER_HEIGHT,
  getExploreContentHeight,
} from "./utils";
import ResizeObserver from "resize-observer-polyfill";
import { fetchSupportFlags } from "@app/actions/supportFlags";
import config from "@inject/utils/config";
import { isEnterprise, isCommunity } from "dyn-load/utils/versionUtils";
import exploreUtils from "@app/utils/explore/exploreUtils";
import HistoryPage from "./HistoryPage/HistoryPage";
import { HomePageTop } from "@inject/pages/HomePage/HomePageTop";
import { newQuery } from "@app/exports/paths";
import { ErrorBoundary } from "@app/components/ErrorBoundary/ErrorBoundary";
import { intl } from "@app/utils/intl";
import { getSourceMap } from "@app/selectors/home";
import { isVersionedSource } from "@app/utils/sourceUtils";
import { rmProjectBase } from "dremio-ui-common/utilities/projectBase.js";
import { DATASET_PATH_FROM_ONBOARDING } from "@inject/components/SonarWelcomeModal/constants";
import { selectState } from "@app/selectors/nessie/nessie";
import { REFLECTION_ARCTIC_ENABLED } from "@app/exports/endpoints/SupportFlags/supportFlagConstants";
import { withAvailablePageTypes } from "dyn-load/utils/explorePageTypes";
import {
  CodeView,
  CopyButton,
  Spinner,
  SyntaxHighlighter,
  TabPanel,
  getControlledTabPanelProps,
} from "dremio-ui-lib/components";
import QueryDataset from "@app/components/QueryDataset/QueryDataset";
import { SqlRunnerTabs } from "dremio-ui-common/sonar/SqlRunnerSession/components/SqlRunnerTabs.js";
import { EXPLORE_DRAG_TYPE } from "../constants";
import { addDatasetATSyntax } from "@app/utils/nessieUtils";
import { withCatalogARSFlag } from "@inject/utils/arsUtils";
import { PHYSICAL_DATASET } from "@app/constants/datasetTypes";
import { getVersionContextFromId } from "dremio-ui-common/utilities/datasetReference.js";
import { formatQuery } from "dremio-ui-common/sql/formatter/sqlFormatter.js";
import socket from "@inject/utils/socket";
import {
  MultiTabIsEnabledProvider,
  withIsMultiTabEnabled,
} from "@app/components/SQLScripts/useMultiTabIsEnabled";
import clsx from "clsx";
import { DatasetDetailsPanel } from "../components/DatasetDetailsPanel/DatasetDetailsPanel";
import { getEntitySummary } from "@app/endpoints/EntitySummary/getEntitySummary";
import sentryUtil from "@app/utils/sentryUtil";
import { getEntityTypeFromObject } from "@app/utils/entity-utils";
import { ENTITY_TYPES_LIST } from "@app/constants/Constants";
import { EntityDetailsPanel } from "../components/EntityDetailsPanel/EntityDetailsPanel";
import { EmptyDetailsPanel } from "../components/EmptyDetailsPanel/EmptyDetailsPanel";
import {
  handleOpenTabScript,
  fetchAllAndMineScripts,
} from "@app/components/SQLScripts/sqlScriptsUtils";
import { $SqlRunnerSession } from "dremio-ui-common/sonar/SqlRunnerSession/resources/SqlRunnerSessionResource.js";
import { ScriptsResource } from "dremio-ui-common/sonar/scripts/resources/ScriptsResource.js";
import { isTabbableUrl } from "@app/utils/explorePageTypeUtils";
import {
  closeTab,
  closeTabs,
  newPopulatedTab,
} from "dremio-ui-common/sonar/SqlRunnerSession/resources/SqlRunnerSessionResource.js";
import { getSupportFlag } from "@app/exports/endpoints/SupportFlags/getSupportFlag";
import { SQLRUNNER_TABS_UI } from "@app/exports/endpoints/SupportFlags/supportFlagConstants";
import { fetchScripts } from "@app/actions/resources/scripts";
import { addNotification } from "@app/actions/notification";
import { BadRequestError } from "dremio-ui-common/errors/BadRequestError";
import SQLScriptRenameDialog from "@app/components/SQLScripts/components/SQLScriptRenameDialog/SQLScriptRenameDialog";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";
import {
  isTemporaryScript,
  addTemporaryPrefix,
} from "dremio-ui-common/sonar/SqlRunnerSession/utilities/temporaryTabs.js";
import { ModalContainer } from "dremio-ui-lib/components";
import { TemporaryTabConfirmDeleteDialog } from "dremio-ui-common/sonar/SqlRunnerSession/components/TemporaryTabConfirmDialog.js";
import { deleteScript } from "dremio-ui-common/sonar/scripts/endpoints/deleteScript.js";
import DocumentTitle from "react-document-title";
import { useScript } from "dremio-ui-common/sonar/scripts/providers/useScript.js";
import { generateNewTabName } from "dremio-ui-common/sonar/SqlRunnerSession/utilities/generateNewTabName.js";
import { getSelectedSql } from "dremio-ui-common/sonar/components/Monaco/components/SqlEditor/helpers/getSqlSelections.js";

const newQueryLink = newQuery();
const HISTORY_BAR_WIDTH = 34;
const SIDEBAR_MIN_WIDTH = 300;
const COLLAPSED_SIDEBAR_WIDTH = 36;
const SQL_EDITOR_PADDING = 20;

const { t } = getIntlContext();

const ScriptDocumentTitle = (props) => {
  const script = useScript(props.currentScriptId);
  if (!script?.name) {
    return <DocumentTitle title={null} />;
  }
  return <DocumentTitle title={script.name} />;
};

const resizeColumn = new ResizeObserver(
  debounce((item) => {
    const column = item[0];
    if (!column) return;

    if (column.contentRect.width <= 1000) {
      column.target.classList.add("--minimal");
    } else {
      column.target.classList.remove("--minimal");
    }
  }, 1),
);

const getStatusesArray = moize((queryStatuses, jobSummaries) => {
  return queryStatuses?.map((queryStatus) => {
    const jobSummary = jobSummaries[queryStatus.jobId];

    if (jobSummary?.state) {
      return jobSummary.state;
    }

    if (queryStatus.error) {
      return "FAILED";
    }
    if (queryStatus.cancelled) {
      return "REMOVED";
    }

    return;
  });
});

export class ExplorePageContentWrapper extends PureComponent {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    datasetSummary: PropTypes.object,
    exploreViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    sqlSize: PropTypes.number,
    sqlState: PropTypes.bool,
    type: PropTypes.string,
    dragType: PropTypes.string,
    approximate: PropTypes.bool,
    jobProgress: PropTypes.object,
    jobId: PropTypes.string,
    outputRecords: PropTypes.number,
    runStatus: PropTypes.bool,

    pageType: pageTypesProp.isRequired,
    rightTreeVisible: PropTypes.bool.isRequired,

    toggleRightTree: PropTypes.func.isRequired,
    startDrag: PropTypes.func.isRequired,
    errorData: PropTypes.object.isRequired,
    isError: PropTypes.bool.isRequired,
    location: PropTypes.object.isRequired,
    setRecommendationInfo: PropTypes.func,
    intl: PropTypes.object.isRequired,

    haveRows: PropTypes.string,
    updateColumnFilter: PropTypes.func,

    // connected
    addNotification: PropTypes.func,
    entityId: PropTypes.string,
    runDatasetSql: PropTypes.func,
    previewDatasetSql: PropTypes.func,
    canSelect: PropTypes.any,
    version: PropTypes.string,
    datasetVersion: PropTypes.string,
    lastDataset: PropTypes.any,
    currentSql: PropTypes.string,
    queryContext: PropTypes.instanceOf(Immutable.List),
    datasetSql: PropTypes.string,
    isSqlQuery: PropTypes.bool,
    loadSourceListData: PropTypes.func,
    queryStatuses: PropTypes.array,
    cancelJob: PropTypes.func,
    setQueryStatuses: PropTypes.func,
    router: PropTypes.any,
    querySelections: PropTypes.array,
    setQuerySelections: PropTypes.func,
    previousMultiSql: PropTypes.string,
    setPreviousAndCurrentSql: PropTypes.func,
    selectedSql: PropTypes.string,
    setSelectedSql: PropTypes.func,
    setCurrentSql: PropTypes.func,
    setCustomDefaultSql: PropTypes.func,
    isMultiQueryRunning: PropTypes.bool,
    queryTabNumber: PropTypes.number,
    setQueryTabNumber: PropTypes.func,
    supportFlagsObj: PropTypes.object,
    fetchSupportFlags: PropTypes.func,
    scripts: PropTypes.any,
    fetchJobSummary: PropTypes.func,
    fetchJobDetails: PropTypes.func,
    clearExploreJobs: PropTypes.func,
    setResizeProgressState: PropTypes.func,
    toggleExploreSql: PropTypes.func,
    nessieState: PropTypes.object,
    loadJobTabs: PropTypes.func,
    loadJobResults: PropTypes.func,

    //HOC
    availablePageTypes: PropTypes.arrayOf(PropTypes.string).isRequired,
    isArsLoading: PropTypes.bool,
    isArsEnabled: PropTypes.bool,
    isMultiTabEnabled: PropTypes.bool,
  };

  static contextTypes = {
    router: PropTypes.object.isRequired,
  };

  constructor(props) {
    super(props);
    this.renderUpperContent = this.getUpperContent.bind(this);
    this.getBottomContent = this.getBottomContent.bind(this);
    this.getControlsBlock = this.getControlsBlock.bind(this);
    this.insertFullPathAtCursor = this.insertFullPathAtCursor.bind(this);
    this.onTabChange = this.onTabChange.bind(this);
    this.cancelPendingSql = this.cancelPendingSql.bind(this);
    this.resetTabToJobList = this.resetTabToJobList.bind(this);
    this.sidebarMouseDown = this.sidebarMouseDown.bind(this);
    this.sidebarMouseMove = this.sidebarMouseMove.bind(this);
    this.sidebarMouseUp = this.sidebarMouseUp.bind(this);

    this.state = {
      funcHelpPanel: false,
      datasetsPanel: !!(props.dataset && props.dataset.get("isNewQuery")),
      sidebarCollapsed: !props.isSqlQuery,
      sidebarResize: false,
      sidebarWidth: null,
      isSqlQuery: props.isSqlQuery,
      tooltipHover: false,
      jobId: "",
      keyboardShortcuts: {
        // Default to Windows commands
        run: "CTRL + Shift + Enter",
        preview: "CTRL + Enter",
        comment: "CTRL + /",
        find: "CTRL + F",
      },
      currentSqlIsEmpty: true,
      currentSqlEdited: false,
      datasetSqlIsEmpty: true,
      showJobsTable: false,
      numberOfTabs: 0,
      editorWidth: null,
      datasetDetailsCollapsed: true,
      datasetDetails: null,
      SQLScriptsLeaveTabDialog: {
        isOpen: false,
        onConfirm: null,
        onCancel: null,
      },
      SQLScriptsRenameDialog: {
        isOpen: false,
        onCancel: () => null,
        script: null,
      },
      requestedTemporaryTabClose: null,
    };

    this.editorRef = createRef();
    this.headerRef = createRef();
    this.dremioSideBarRef = createRef();
    this.dremioSideBarDrag = createRef();
    this.explorePageRef = createRef();
    this.observeRef = createRef();
  }
  timeoutRef = undefined;

  redirectToLastUsedTab = async (router) => {
    const script = await exploreUtils.getCurrentScript();

    handleOpenTabScript(router)(script);
  };

  createNewScriptFromQueryPath = async (content, context = []) => {
    if (!(await getSupportFlag(SQLRUNNER_TABS_UI)).value) {
      return;
    }

    if (!content) return;

    // Throws the error, don't catch here
    const result = await newPopulatedTab({
      content,
      context,
      description: "",
      name: addTemporaryPrefix(generateNewTabName()),
    });

    await ScriptsResource.fetch();
    await fetchAllAndMineScripts(this.props.fetchScripts, null);

    return result;
  };

  handleQueryPathWithoutTabs = (sql) => {
    const { router, setPreviousAndCurrentSql, setCustomDefaultSql } =
      this.props;

    setPreviousAndCurrentSql({ sql });
    setCustomDefaultSql({ sql });

    router.replace({
      ...location,
      query: {
        ...location.query,
        queryPath: undefined,
        versionContext: undefined,
      },
      // setting a state prevents the editor from resetting when querying a dataset
      // see: getExplorePageLocationChangePredicate
      state: { ignoreReset: true },
    });

    this.scriptCreating = false;
  };

  handleQueryPath = async (queryPathParam) => {
    let queryPath;
    try {
      queryPath = constructFullPath(JSON.parse(queryPathParam));
    } catch (e) {
      //
    }
    const sql = exploreUtils.createNewQueryFromDatasetOverlay(queryPath);
    this.loadDatasetSummaryFromUrl();

    if (!(await getSupportFlag(SQLRUNNER_TABS_UI)).value) {
      return this.handleQueryPathWithoutTabs(sql);
    }

    return this.createNewTabWithSql(sql);
  };

  handlePureSql = async (sql, queryStatuses, context) => {
    if ((await getSupportFlag(SQLRUNNER_TABS_UI)).value) {
      return this.createNewTabWithSql(sql, queryStatuses, context);
    }
  };

  handleOpenJobResultsWithoutTabs(statuses) {
    const { setQueryStatuses } = this.props;
    setQueryStatuses({ statuses });
  }

  handleOpenJobResults = async (sql, newQueryStatuses) => {
    if (
      !isTabbableUrl(this.props.location) ||
      !(await getSupportFlag(SQLRUNNER_TABS_UI)).value
    ) {
      return this.handleOpenJobResultsWithoutTabs(newQueryStatuses);
    }

    // Not sure if we need to pass queryContext for queryPath flow too
    return this.createNewTabWithSql(
      sql,
      newQueryStatuses,
      this.props.queryContext,
    );
  };

  createNewTabWithSql = async (sql, newQueryStatuses, context) => {
    if (this.scriptCreating) return;

    this.scriptCreating = true;

    this.createNewScriptFromQueryPath(sql, context)
      .then((createdScript) => {
        //Important: queryPath is removed here so that the lifecycle won't run this side-effect again
        const success = handleOpenTabScript(this.props.router)(
          createdScript,
          newQueryStatuses,
        );
        this.scriptCreating = false;
        return success;
      })
      .catch((e) => {
        if (e instanceof BadRequestError) {
          const code = e.responseBody.errors?.[0]?.code;
          this.props.addNotification(
            code ? t(code + ":from_dataset") : e.responseBody.errorMessage,
            "error",
          );
        }
        const failure = this.redirectToLastUsedTab(this.props.router);
        //Remove queryPath so that this side-effect does not run again
        this.props.router.replace({
          ...location,
          query: {
            ...location.query,
            queryPath: undefined,
            versionContext: undefined,
          },
        });
        this.scriptCreating = false;
        return failure;
      });
  };

  checkForScriptJobs = async () => {
    const { loadJobTabs, previousMultiSql, setPreviousAndCurrentSql } =
      this.props;

    const script = await exploreUtils.getCurrentScript();

    // previousMultiSql is null when refreshing the page after running a successful job
    if (!previousMultiSql) {
      setPreviousAndCurrentSql({ sql: script.content });
    }

    loadJobTabs(script);
  };

  componentDidMount() {
    const {
      location,
      setPreviousAndCurrentSql,
      dataset,
      queryStatuses,
      isMultiTabEnabled,
    } = this.props;
    // fetch support flags here for powerbi and tableau only if its not enterprise
    if (!(isEnterprise?.() || isCommunity?.())) {
      this.props.fetchSupportFlags("client.tools.tableau");
      this.props.fetchSupportFlags("client.tools.powerbi");
    }
    if (!isCommunity?.()) {
      this.props.fetchSupportFlags(REFLECTION_ARCTIC_ENABLED);
    }

    // If the new_query page is loaded without a scriptId, redirect the user to the most
    // recently used tab in the sql runner session
    if (
      isTabbableUrl(location) &&
      !location.query?.scriptId &&
      // Handled below
      !location.query?.queryPath &&
      !dataset.get("sql")
    ) {
      this.redirectToLastUsedTab(this.props.router);
    }

    // load a script's saved jobs when going to the SQL Runner

    const isOnHistoryNode =
      location.query?.tipVersion !== location.query?.version;

    if (
      isTabbableUrl(location) &&
      !location.query?.queryPath &&
      !isOnHistoryNode &&
      !dataset.get("sql")
    ) {
      this.checkForScriptJobs();
    }

    Mousetrap.bind(
      ["mod+enter", "mod+shift+enter", "mod+option+t", "shift+enter"],
      this.kbdShorthand,
    );
    this.getUserOperatingSystem();
    this.onJobIdChange();
    this.props.loadSourceListData();
    this.onTabRender();
    this.handleResize();
    this.props.clearExploreJobs();

    // Jobs list is reset whenever moving between the SQL Runner and dataset editor.
    // This fetches the jobs when multi tabs are disabled, otherwise they're fetched
    // by the "doJobFetch" saga.
    if (
      !isMultiTabEnabled &&
      queryStatuses &&
      (!isTabbableUrl(location) ||
        (isTabbableUrl(location) && dataset.get("sql")))
    ) {
      queryStatuses.forEach((status) => {
        this.props.fetchJobDetails(status.jobId);
        this.props.fetchJobSummary(status.jobId, 0);
      });
    }

    this.headerRef = document.querySelector(".c-homePageTop");

    document.addEventListener("mouseup", this.sidebarMouseUp);
    document.addEventListener("mousemove", this.sidebarMouseMove);
    window.addEventListener("resize", this.handleResize);

    this.doObserve();

    if (isTabbableUrl(location) && location.query?.queryPath) {
      this.handleQueryPath(location.query?.queryPath);
    }

    // executes when opening a table and editing the query
    if (isTabbableUrl(location) && dataset.get("sql")) {
      this.handlePureSql(
        dataset.get("sql"),
        queryStatuses,
        dataset.get("context"),
      );
    }

    if (sessionStorage.getItem(DATASET_PATH_FROM_ONBOARDING)) {
      // updates the sql when redirected at the end of onboarding flow
      sessionStorage.getItem(DATASET_PATH_FROM_ONBOARDING);
      setPreviousAndCurrentSql({
        sql: exploreUtils.createNewQueryFromDatasetOverlay(
          sessionStorage.getItem(DATASET_PATH_FROM_ONBOARDING),
        ),
      });
      sessionStorage.removeItem(DATASET_PATH_FROM_ONBOARDING);
    }

    socket.setServerHeartbeat(true);
  }

  componentWillUnmount() {
    Mousetrap.unbind([
      "mod+enter",
      "mod+shift+enter",
      "mod+option+t",
      "shift+enter",
    ]);
    document.removeEventListener("mouseup", this.sidebarMouseUp);
    document.removeEventListener("mousemove", this.sidebarMouseMove);
    window.removeEventListener("resize", this.handleResize);
    if (this.observeRef.current) {
      resizeColumn.unobserve(this.observeRef.current);
    }
    clearTimeout(this.timeoutRef);

    // Sidebar resizing
    if (this.dremioSideBarDrag.current) {
      this.dremioSideBarDrag.current.removeEventListener(
        "mousedown",
        this.sidebarMouseDown,
      );
    }

    socket.setServerHeartbeat(false);
  }

  doObserve = () => {
    const { pageType } = this.props;
    if (pageType === "default") {
      if (this.observeRef.current) {
        resizeColumn.observe(this.observeRef.current);
      } else {
        this.observeRef = createRef();
      }
    } else {
      //Chrome auto unobserves when removing from dom but do this just in case
      if (this.observeRef.current) {
        resizeColumn.unobserve(this.observeRef.current);
        this.observeRef.current = null;
      }
    }
  };

  scriptCreating = false;

  componentDidUpdate(prevProps) {
    const {
      jobProgress,
      setPreviousAndCurrentSql: newPreviousAndCurrentSql,
      datasetSql,
      currentSql,
      datasetVersion,
      queryStatuses,
      jobSummaries,
      queryTabNumber,
      selectedSql,
      setCurrentSql,
      setSelectedSql: newSelectedSql,
      setQuerySelections: newQuerySelections,
      isMultiQueryRunning,
      location,
      pageType,
    } = this.props;

    const prevStatusesArray = getStatusesArray(
      prevProps.queryStatuses,
      prevProps.jobSummaries,
    );
    const statusesArray = getStatusesArray(queryStatuses, jobSummaries);

    this.doObserve();

    // Clicking the query button from the ExplorePage search
    const loc = rmProjectBase(location.pathname);
    if (
      loc === newQueryLink &&
      location.query?.queryPath &&
      location.query?.queryPath !== prevProps.location.query?.queryPath
    ) {
      this.handleQueryPath(location.query?.queryPath);
    }

    // if a single job, force the explore table to be in view
    if (
      statusesArray.length === 1 &&
      prevStatusesArray.length === statusesArray.length &&
      !prevStatusesArray[0] &&
      statusesArray[0]
    ) {
      this.onTabRender(true);
    }

    const isDifferentTab =
      queryTabNumber !== 0 &&
      prevProps.queryTabNumber !== queryTabNumber &&
      !isMultiQueryRunning;
    const isDifferentJob =
      prevProps.jobProgress && prevProps.jobProgress.jobId !== this.state.jobId;
    if (isDifferentJob || isDifferentTab) {
      if (jobProgress && jobProgress.jobId) {
        // Reset the currentSql to the statement that was executed
        if (datasetSql !== currentSql && !isDifferentTab) {
          if (queryStatuses && queryStatuses.length < 2) {
            const currentSqlQueries = extractQueries(currentSql ?? datasetSql);

            if (
              currentSqlQueries &&
              currentSqlQueries.length < 2 &&
              !selectedSql
            ) {
              // upon initialization (from job listing page), add a query status
              if (!queryStatuses.length && !!location?.query?.openResults) {
                this.handleOpenJobResults(datasetSql, [
                  {
                    sqlStatement: datasetSql,
                    cancelled: false,
                    jobId: jobProgress.jobId,
                    version: datasetVersion,
                  },
                ]);
              }
            }

            if (selectedSql) {
              newSelectedSql({ sql: "" });
            }
          } else if (currentSql == null && queryStatuses.length) {
            // this executes when the module state is reset as a query is being run
            // for some reason, this only happens when a new user account runs a query
            // for the first time on DCS
            let newCurrentSql = "";
            for (const status of queryStatuses) {
              newCurrentSql += status.sqlStatement + ";\n";
            }
            newPreviousAndCurrentSql({ sql: newCurrentSql });
            const newSelections = extractSelections(newCurrentSql);
            newQuerySelections({ selections: newSelections });
          }
        }
      }
      this.onJobIdChange();
    }

    // Show / hide jobs table when switching between scripts
    if (
      (queryTabNumber === 0 && !this.state.showJobsTable) ||
      (queryTabNumber && this.state.showJobsTable)
    ) {
      this.setState({ showJobsTable: !this.state.showJobsTable });
    }

    // Update when switching between Dataset and SQL Runner
    if (prevProps.isSqlQuery !== this.props.isSqlQuery) {
      this.setPanelCollapse();
    }

    // resets currentSql when opening datasets from the graph and clicking the back button
    if (pageType === PageTypes.graph && datasetSql !== prevProps.datasetSql) {
      setCurrentSql({ sql: datasetSql });
    }

    this.handleDisableButton(prevProps);

    // Sidebar resizing
    if (this.dremioSideBarDrag.current) {
      this.dremioSideBarDrag.current.addEventListener(
        "mousedown",
        this.sidebarMouseDown,
      );
    }
  }

  loadDatasetSummaryFromUrl = async () => {
    const { location } = this.props;
    let versionContext;
    try {
      versionContext = JSON.parse(location.query?.versionContext);
    } catch (e) {
      //
    }
    try {
      const res = await getEntitySummary({
        fullPath: JSON.parse(location.query?.queryPath),
        versionContext: versionContext,
      });

      this.setState(
        {
          datasetDetails: Immutable.fromJS(res),
          datasetDetailsCollapsed: false,
        },
        () => this.handleResize(),
      );
    } catch (e) {
      sentryUtil.logException(e);
    }
  };

  onJobIdChange() {
    if (this.props.jobProgress) {
      this.setState({ jobId: this.props.jobProgress.jobId });
    }
  }

  resetTabToJobList() {
    this.setState({
      showJobsTable: true,
    });
    this.props.setQueryTabNumber({ tabNumber: 0 });
  }

  onTabRender(forceTab = false) {
    const { queryStatuses } = this.props;
    if ((queryStatuses && queryStatuses.length < 2) || forceTab) {
      this.setState({
        showJobsTable: false,
      });
      this.props.setQueryTabNumber({ tabNumber: 1 });
    } else {
      this.resetTabToJobList();
    }
  }

  async onTabChange(tabIndex) {
    const {
      queryStatuses,
      location,
      router,
      isMultiQueryRunning,
      loadJobResults,
    } = this.props;
    const showJobs = tabIndex === 0;
    const currentJob = queryStatuses[tabIndex - 1];
    this.setState({
      showJobsTable: showJobs,
    });
    this.props.setQueryTabNumber({ tabNumber: tabIndex });
    this.handleSqlSelection(tabIndex);
    if (tabIndex !== 0 && !isMultiQueryRunning) {
      const script = await exploreUtils.getCurrentScript();
      loadJobResults(script, currentJob);

      handleOnTabRouting(
        tabIndex,
        currentJob.jobId,
        currentJob.version,
        location,
        router,
        isMultiQueryRunning,
      );
    } else {
      handleOnTabRouting(
        tabIndex,
        undefined,
        undefined,
        location,
        router,
        isMultiQueryRunning,
      );
    }
  }

  onLeaveTabWhileQueriesRunning(onConfirm) {
    this.setState({
      SQLScriptsLeaveTabDialog: {
        isOpen: true,
        onCancel: () =>
          this.setState({
            SQLScriptsLeaveTabDialog: {
              isOpen: false,
              onCancel: null,
              onConfirm: null,
            },
          }),
        onConfirm,
      },
    });
  }

  onRenameTab(script) {
    this.setState({
      SQLScriptsRenameDialog: {
        isOpen: true,
        onCancel: () => {
          this.setState({
            SQLScriptsRenameDialog: {
              isOpen: false,
              onCancel: () => null,
              script: null,
            },
          });
        },
        script,
      },
    });
  }

  handleSqlSelection(tabIndex) {
    const { querySelections, currentSql, previousMultiSql } = this.props;
    const isNotEdited = !!previousMultiSql && currentSql === previousMultiSql;
    if (
      isNotEdited &&
      querySelections.length > 0 &&
      tabIndex <= querySelections.length
    ) {
      if (tabIndex > 0) {
        let currentSelection = this.getMonacoEditorInstance().getSelections();
        const newSelection = querySelections[tabIndex - 1];
        currentSelection = { ...newSelection };
        this.getMonacoEditorInstance().setSelection(currentSelection);
        this.getMonacoEditorInstance().revealLine(
          currentSelection.startLineNumber,
        );
      } else {
        this.getMonacoEditorInstance().setSelection({
          endColumn: 1,
          endLineNumber: 1,
          positionColumn: 1,
          positionLineNumber: 1,
          selectionStartColumn: 1,
          selectionStartLineNumber: 1,
          startColumn: 1,
          startLineNumber: 1,
        });
        this.getMonacoEditorInstance().revealLine(1);
      }
    }
  }

  kbdShorthand = (e) => {
    if (!e) return;

    const { pageType, location } = this.props;
    navigateToExploreDefaultIfNecessary(
      pageType,
      location,
      this.context.router,
    );

    if (e.metaKey && e.key === "Enter") {
      if (e.shiftKey) {
        this.props.runDatasetSql({
          selectedSql: getSelectedSql(this.getMonacoEditorInstance()),
        });
      } else {
        this.props.previewDatasetSql({
          selectedSql: getSelectedSql(this.getMonacoEditorInstance()),
        });
      }
    } else if (e.key === "Enter" && e.shiftKey) {
      this.getMonacoEditorInstance()?.handleGenerate?.();
    } else if (e.code === "KeyT") {
      this.editorRef.current?.explorePageMainContentRef?.topSplitterContentRef?.toggleExtraSQLPanel?.();
    }
  };

  getMonacoEditorInstance() {
    const sqlAutoCompleteRef =
      this.editorRef.current?.explorePageMainContentRef?.topSplitterContentRef
        ?.sqlEditorControllerRef?.sqlAutoCompleteRef;

    return sqlAutoCompleteRef?.getEditorInstance?.();
  }

  getUserOperatingSystem() {
    // Change to Mac commands
    if (navigator.userAgent.indexOf("Mac OS X") !== -1) {
      this.setState({
        keyboardShortcuts: {
          run: "⌘⇧↵",
          preview: "⌘↵",
          comment: "⌘/",
          find: "⌘F",
        },
      });
    }
  }

  cancelPendingSql(index) {
    const { queryStatuses, setQueryStatuses } = this.props;
    const updatedQueries = cloneDeep(queryStatuses);
    updatedQueries[index].cancelled = true;
    setQueryStatuses({ statuses: updatedQueries });
  }

  insertFullPathAtCursor(id) {
    this.insertFullPath(id);
  }

  insertFieldName(name, ranges) {
    const text = exploreUtils.escapeFieldNameForSQL(name);
    this.insertAtRanges(text, ranges);
  }

  insertFullPath(pathList, ranges) {
    // String is sent for column nodes, pathlist for others
    if (typeof pathList === "string") {
      this.insertFieldName(pathList, ranges);
    } else {
      const text = constructFullPath(pathList);
      this.insertAtRanges(text + addDatasetATSyntax(pathList.toJS()), ranges);
    }
  }

  insertAtRanges(
    text,
    ranges = this.getMonacoEditorInstance().getSelections(),
  ) {
    // getSelections() falls back to cursor location automatically
    const edits = ranges.map((range) => ({
      identifier: "dremio-inject",
      range,
      text,
    }));

    // Remove highlighted string and place cursor to the end of the new `text`
    ranges[0].selectionStartColumn =
      ranges[0].selectionStartColumn + text.length;
    ranges[0].positionColumn = ranges[0].positionColumn + text.length;

    this.getMonacoEditorInstance().executeEdits("dremio", edits, ranges);
    this.getMonacoEditorInstance().pushUndoStop();
    this.getMonacoEditorInstance().focus();
  }

  handleSidebarCollapse = () => {
    this.dremioSideBarRef.current.style.width = null;
    const width = this.state.sidebarCollapsed
      ? SIDEBAR_MIN_WIDTH
      : COLLAPSED_SIDEBAR_WIDTH;
    this.setState({
      sidebarCollapsed: !this.state.sidebarCollapsed,
      editorWidth: this.getNewEditorWidth(width),
    });
  };

  handleResize = () => {
    const width = this.state.sidebarCollapsed
      ? COLLAPSED_SIDEBAR_WIDTH
      : this.state.sidebarWidth ?? SIDEBAR_MIN_WIDTH;
    this.setState({ editorWidth: this.getNewEditorWidth(width) });
  };

  getNewEditorWidth = (sideBarWidth) => {
    const { isArsEnabled, isArsLoading } = this.props;
    const { datasetDetailsCollapsed } = this.state;
    const datasetDetailsWidth = datasetDetailsCollapsed ? 36 : 320;
    if (this.explorePageRef?.current) {
      const padding =
        isArsEnabled || isArsLoading
          ? SQL_EDITOR_PADDING
          : HISTORY_BAR_WIDTH + SQL_EDITOR_PADDING;
      return (
        this.explorePageRef.current.offsetWidth -
        sideBarWidth -
        padding -
        datasetDetailsWidth
      );
    }
  };

  toggleSqlPaneDisplay = () => {
    clearTimeout(this.timeoutRef);
    this.props.setResizeProgressState(true);
    this.props.toggleExploreSql?.();
    this.timeoutRef = setTimeout(
      () => this.props.setResizeProgressState(false),
      500,
    ); // ref for calculating height isn't auto updated
  };

  handleDatasetDetailsCollapse = () => {
    this.setState(
      {
        datasetDetailsCollapsed: !this.state.datasetDetailsCollapsed,
      },
      () => this.handleResize(),
    );
  };

  handleDatasetDetails = (datasetDetails) => {
    const stateDetails = this.state.datasetDetails;
    const argDetails = datasetDetails;
    const dataset = argDetails?.get("error")
      ? stateDetails?.merge(argDetails)
      : argDetails;
    if (
      stateDetails?.get("entityId") !== argDetails?.get("entityId") ||
      stateDetails?.size !== argDetails?.size ||
      (datasetDetails?.get("fromTreeNode") &&
        stateDetails?.get("entityId") !== argDetails?.get("entityId"))
    ) {
      this.setState(
        {
          datasetDetails: dataset,
          datasetDetailsCollapsed: false,
        },
        () => this.handleResize(),
      );
    }
  };

  getBottomContent() {
    const {
      dataset,
      pageType,
      location,
      entityId,
      canSelect,
      queryStatuses,
      jobSummaries,
      queryTabNumber,
      cancelJob,
      setQueryStatuses: newQueryStatuses,
    } = this.props;
    const statusesArray = getStatusesArray(queryStatuses, jobSummaries);

    const { showJobsTable } = this.state;

    const locationQuery = location.query;
    const tabStatusArr = assemblePendingOrRunningTabContent(
      queryStatuses,
      statusesArray,
      newQueryStatuses,
      cancelJob,
      this.cancelPendingSql,
    );

    if (
      locationQuery &&
      locationQuery.type === "JOIN" &&
      locationQuery.joinTab !== RECOMMENDED_JOIN
    ) {
      return (
        <JoinTables
          pageType={pageType}
          dataset={dataset}
          location={location}
          rightTreeVisible={this.props.rightTreeVisible}
          exploreViewState={this.props.exploreViewState}
        />
      );
    }

    switch (pageType) {
      case PageTypes.graph:
        return (
          DataGraph && (
            <DataGraph
              dragType={EXPLORE_DRAG_TYPE}
              dataset={dataset}
              rightTreeVisible={this.props.rightTreeVisible}
            />
          )
        );
      case PageTypes.wiki: {
        // should allow edit a wiki only if we receive a entity id and permissions allow this.
        // If we do not receive permissions object, that means the current user is admin (CE)
        const isWikiEditAllowed =
          entityId &&
          dataset.getIn(["permissions", "canManageWiki"], true) &&
          dataset.getIn(["permissions", "canAlter"], true);

        return (
          <Wiki
            entityId={entityId}
            isEditAllowed={isWikiEditAllowed}
            className="bottomContent"
            dataset={dataset}
          />
        );
      }
      case PageTypes.reflections: {
        return <Reflections datasetId={entityId} />;
      }
      case PageTypes.default:
      case PageTypes.details:
        return (
          <ExploreTableController
            pageType={pageType}
            dataset={dataset}
            dragType={EXPLORE_DRAG_TYPE}
            location={location}
            rightTreeVisible={this.props.rightTreeVisible}
            exploreViewState={this.props.exploreViewState}
            canSelect={canSelect}
            showJobsTable={showJobsTable}
            handleTabChange={this.onTabChange}
            currentJobsMap={queryStatuses}
            cancelPendingSql={this.cancelPendingSql}
            tabStatusArr={tabStatusArr}
            queryTabNumber={queryTabNumber}
            shouldRenderInvisibles
          />
        );
      case PageTypes.history:
        return <HistoryPage />;
      default:
        throw new Error(`Not supported page type: '${pageType}'`);
    }
  }

  getControlsBlock() {
    const {
      dataset,
      location,
      rightTreeVisible,
      exploreViewState,
      pageType,
      approximate,
      version,
      queryStatuses,
      jobSummaries,
      datasetSql,
      isMultiQueryRunning,
      currentSql,
      previousMultiSql,
      queryTabNumber,
    } = this.props;
    const statusesArray = getStatusesArray(queryStatuses, jobSummaries);

    const { showJobsTable } = this.state;

    switch (pageType) {
      case PageTypes.graph:
      case PageTypes.details:
      case PageTypes.reflections:
      case PageTypes.wiki:
      case PageTypes.history:
        return;
      case PageTypes.default:
        return (
          <TableControls
            dataset={dataset}
            location={location}
            pageType={pageType}
            rightTreeVisible={rightTreeVisible}
            exploreViewState={exploreViewState}
            disableButtons={
              this.isButtonDisabled() ||
              (!queryStatuses.length && !datasetSql) ||
              isMultiQueryRunning ||
              currentSql !== previousMultiSql ||
              statusesArray[queryTabNumber - 1] === "FAILED" ||
              statusesArray[queryTabNumber - 1] === "CANCELED" ||
              statusesArray[queryTabNumber - 1] === "REMOVED" ||
              queryStatuses.length === 0 ||
              queryStatuses[queryTabNumber - 1]?.error
            }
            approximate={approximate}
            version={version}
            showJobsTable={showJobsTable}
            jobsCount={queryStatuses.length}
            queryTabNumber={queryTabNumber}
          />
        );
      default:
        throw new Error(`not supported page type; '${pageType}'`);
    }
  }

  getTabsBlock() {
    const {
      queryStatuses,
      jobSummaries,
      location,
      pageType,
      isMultiQueryRunning,
      queryTabNumber,
      sqlState,
    } = this.props;
    const statusesArray = getStatusesArray(queryStatuses, jobSummaries);
    return (
      pageType === PageTypes.default && (
        <SqlQueryTabs
          handleTabChange={this.onTabChange}
          location={location}
          tabNumber={queryTabNumber}
          queryStatuses={queryStatuses}
          statusesArray={statusesArray}
          isMultiQueryRunning={isMultiQueryRunning}
          sqlState={sqlState}
        />
      )
    );
  }

  getContentHeader() {
    const {
      approximate,
      pageType,
      exploreViewState,
      dataset,
      location,
      rightTreeVisible,
      toggleRightTree,
      sqlState,
      queryStatuses,
      jobSummaries,
    } = this.props;
    const statusesArray = getStatusesArray(queryStatuses, jobSummaries);

    switch (pageType) {
      case PageTypes.graph:
      case PageTypes.details:
      case PageTypes.reflections:
      case PageTypes.wiki:
      case PageTypes.history:
        return;
      case PageTypes.default:
        return (
          <ExploreHeader
            dataset={dataset}
            pageType={pageType}
            toggleRightTree={toggleRightTree}
            rightTreeVisible={rightTreeVisible}
            exploreViewState={exploreViewState}
            approximate={approximate}
            location={location}
            sqlState={sqlState}
            keyboardShortcuts={this.state.keyboardShortcuts}
            disableButtons={this.isButtonDisabled()}
            getSelectedSql={() =>
              getSelectedSql(this.getMonacoEditorInstance())
            }
            statusesArray={statusesArray}
            resetSqlTabs={this.resetTabToJobList}
            supportFlagsObj={this.props.supportFlagsObj}
            toggleSqlPaneDisplay={this.toggleSqlPaneDisplay}
          />
        );
      default:
        throw new Error(`not supported page type; '${pageType}'`);
    }
  }

  sidebarMouseDown() {
    this.setState({ sidebarResize: true, datasetDetailsCollapsed: true });
  }

  sidebarMouseMove(e) {
    if (this.state.sidebarResize) {
      e.preventDefault();
      e.stopPropagation();

      let newSidebarWidth;
      const LEFT_NAV_WIDTH = 64;
      const SIDEBAR_MIN_WIDTH = 300 + LEFT_NAV_WIDTH;
      const SIDEBAR_MAX_WIDTH = 480 + LEFT_NAV_WIDTH;

      if (e.pageX <= SIDEBAR_MIN_WIDTH) {
        newSidebarWidth = SIDEBAR_MIN_WIDTH - LEFT_NAV_WIDTH;
      } else if (e.pageX > SIDEBAR_MAX_WIDTH) {
        newSidebarWidth = SIDEBAR_MAX_WIDTH - LEFT_NAV_WIDTH;
      } else {
        newSidebarWidth = e.pageX - LEFT_NAV_WIDTH;
      }

      this.dremioSideBarRef.current.style.transition = "inherit";
      this.setState({
        sidebarWidth: newSidebarWidth,
        editorWidth: this.getNewEditorWidth(newSidebarWidth),
      });
    }
  }

  sidebarMouseUp() {
    if (this.state.sidebarResize) {
      this.dremioSideBarRef.current.style.transition = null;
      this.setState({ sidebarResize: false });
    }
  }

  getSidebar() {
    const { pageType, exploreViewState, dataset, sqlSize, location } =
      this.props;

    switch (pageType) {
      case PageTypes.graph:
      case PageTypes.details:
      case PageTypes.reflections:
      case PageTypes.wiki:
      case PageTypes.history:
        return;
      case PageTypes.default:
        return (
          <div
            className={classNames(
              "dremioSidebar",
              this.state.sidebarResize && "--active",
            )}
            ref={this.dremioSideBarRef}
            style={{
              width: this.state.sidebarWidth && this.state.sidebarWidth,
            }}
          >
            <div
              className="dremioSidebar__drag"
              ref={this.dremioSideBarDrag}
            ></div>
            <div className="dremioSidebarWrap">
              <div className="dremioSidebarWrap__inner">
                <DatasetsPanel
                  dataset={dataset}
                  height={sqlSize - MARGIN_PANEL}
                  isVisible={this.state.datasetsPanel}
                  dragType={EXPLORE_DRAG_TYPE}
                  viewState={exploreViewState}
                  insertFullPathAtCursor={this.insertFullPathAtCursor}
                  location={location}
                  sidebarCollapsed={this.state.sidebarCollapsed}
                  handleSidebarCollapse={this.handleSidebarCollapse}
                  handleDatasetDetails={this.handleDatasetDetails}
                />
              </div>
            </div>
          </div>
        );
      default:
        throw new Error(`not supported page type; '${pageType}'`);
    }
  }

  getUpperContent() {
    const {
      pageType,
      canSelect,
      exploreViewState,
      dataset,
      location,
      startDrag,
      rightTreeVisible,
      sqlSize,
      sqlState,
      toggleRightTree,
    } = this.props;

    switch (pageType) {
      case PageTypes.details:
        return (
          <DetailsWizard
            dataset={dataset}
            location={location}
            startDrag={startDrag}
            dragType={EXPLORE_DRAG_TYPE}
            exploreViewState={exploreViewState}
            canSelect={canSelect}
          />
        );
      case PageTypes.default:
      case PageTypes.reflections:
      case PageTypes.graph:
      case PageTypes.wiki:
      case PageTypes.history:
        return (
          <ExplorePageMainContent
            dataset={dataset}
            pageType={pageType}
            rightTreeVisible={rightTreeVisible}
            sqlSize={sqlSize}
            sqlState={sqlState}
            dragType={EXPLORE_DRAG_TYPE}
            toggleRightTree={toggleRightTree}
            startDrag={startDrag}
            exploreViewState={exploreViewState}
            sidebarCollapsed={this.state.sidebarCollapsed}
            handleSidebarCollapse={this.handleSidebarCollapse}
            ref={this.editorRef}
            editorWidth={this.state.editorWidth}
          />
        );
      default:
        throw new Error(`not supported page type; '${pageType}'`);
    }
  }

  getDatasetDetailsPanel() {
    const { pageType, isSqlQuery, dataset } = this.props;
    const { datasetDetails } = this.state;
    const curDataset = datasetDetails || (!isSqlQuery ? dataset : null);
    const isEmpty = !curDataset;
    const type = getEntityTypeFromObject(curDataset);
    const isEntity =
      ENTITY_TYPES_LIST.includes(type?.toLowerCase()) &&
      !curDataset?.get("queryable");
    switch (pageType) {
      case PageTypes.graph:
      case PageTypes.details:
      case PageTypes.reflections:
      case PageTypes.wiki:
      case PageTypes.history:
        return;
      case PageTypes.default:
        return isEmpty ? (
          <EmptyDetailsPanel
            datasetDetailsCollapsed={this.state.datasetDetailsCollapsed}
            handleDatasetDetailsCollapse={this.handleDatasetDetailsCollapse}
          />
        ) : isEntity ? (
          <EntityDetailsPanel
            entityDetailsCollapsed={this.state.datasetDetailsCollapsed}
            handleEntityDetailsCollapse={this.handleDatasetDetailsCollapse}
            entityDetails={curDataset}
            handleEntityDetails={this.handleDatasetDetails}
          />
        ) : (
          <DatasetDetailsPanel
            datasetDetailsCollapsed={this.state.datasetDetailsCollapsed}
            handleDatasetDetailsCollapse={this.handleDatasetDetailsCollapse}
            datasetDetails={curDataset}
            handleDatasetDetails={this.handleDatasetDetails}
            hideGoToButton={
              !isSqlQuery &&
              (!curDataset ||
                curDataset?.get("entityId") === curDataset?.get("entityId"))
            }
          />
        );
      default:
        throw new Error(`not supported page type; '${pageType}'`);
    }
  }

  handleDisableButton(prevProps) {
    const { currentSql, datasetSql } = this.props;

    // Only update state if SQL statement has been edited
    if (prevProps.currentSql !== currentSql) {
      this.setState({ currentSqlEdited: true });

      if (currentSql === null || currentSql.length === 0) {
        this.setState({ currentSqlIsEmpty: true });
      } else {
        this.setState({ currentSqlIsEmpty: false });
      }
    }

    // Update state because datasetSql prop is not sent with the first component update
    if (prevProps.datasetSql !== datasetSql) {
      if (datasetSql.length > 0) {
        this.setState({ datasetSqlIsEmpty: false });
      } else {
        this.setState({ datasetSqlIsEmpty: true });
      }
    } else if (datasetSql.length > 0) {
      this.setState({ datasetSqlIsEmpty: false });
    }
  }

  // Disabled Run and Preview buttons if SQL statement is empty
  // TODO: revisit, might be able to simplify this logic
  isButtonDisabled = () => {
    const { currentSqlEdited, currentSqlIsEmpty, datasetSqlIsEmpty } =
      this.state;

    // SQL statement has been edited
    if (currentSqlEdited) {
      if (currentSqlIsEmpty) {
        return true;
      }

      // On intial load of a dataset, show buttons
    } else if (!currentSqlEdited && currentSqlIsEmpty && !datasetSqlIsEmpty) {
      return false;
    }

    // Show buttons if SQL statement has any value
    if (!currentSqlIsEmpty) {
      return false;
    }

    return true;
  };

  setPanelCollapse = () => {
    this.setState({ sidebarCollapsed: !this.props.isSqlQuery }, () =>
      this.handleResize(),
    );
  };

  updateColumnFilter = (columnFilter) => {
    this.props.updateColumnFilter(columnFilter);
  };

  getFormattedTableDefinition = (shouldShowCodeView, returnError) => {
    try {
      return shouldShowCodeView
        ? formatQuery(this.props.dataset.get("sql"))
        : "";
    } catch (e) {
      sentryUtil.logException(e);
      return returnError ? "error" : this.props.dataset.get("sql");
    }
  };

  render() {
    const {
      dataset,
      exploreViewState,
      isArsEnabled,
      isArsLoading,
      isMultiTabEnabled,
      isMultiQueryRunning,
      pageType,
      removeTabView,
    } = this.props;

    const isDatasetPage = exploreUtils.isExploreDatasetPage(location);
    const versionContext = getVersionContextFromId(dataset.get("entityId"));

    const isDatasetLoading =
      isDatasetPage && !dataset.get("sql") && !exploreViewState.get("isFailed");

    const shouldShowCodeView =
      isDatasetPage &&
      pageType === PageTypes.default &&
      dataset.get("datasetType") === PHYSICAL_DATASET &&
      !!versionContext;
    const formattedSQL = this.getFormattedTableDefinition(shouldShowCodeView);

    const isTabbable = isTabbableUrl(this.props.location);

    const getCloseableScripts = () => {
      const { currentScriptId, scriptIds } = $SqlRunnerSession.$source.value;
      return scriptIds.filter((id) => id !== currentScriptId);
    };

    const handleCloseOthers = () => {
      const { currentScriptId } = $SqlRunnerSession.$source.value;

      const closedScriptIds = getCloseableScripts();

      // Delete temporary scripts without asking for confirmation
      ScriptsResource.getResource()
        .value.filter(
          (script) =>
            isTemporaryScript(script) && closedScriptIds.includes(script.id),
        )
        .forEach((script) => {
          deleteScript(script.id);
        });

      closeTabs(closedScriptIds);

      closedScriptIds.forEach((id) => {
        removeTabView(id);
      });

      const nextScriptId = $SqlRunnerSession.$merged.value.currentScriptId;

      if (nextScriptId && currentScriptId !== nextScriptId) {
        const script = ScriptsResource.getResource().value?.find(
          (script) => script.id === nextScriptId,
        );
        if (!script) {
          return;
        }
        handleOpenTabScript(this.props.router)(script);
      }
    };

    const handleTabClosed = (tabId, options = {}) => {
      closeTab(tabId);
      if (options.shouldDelete) {
        deleteScript(tabId);
      }
      removeTabView(tabId); // Removes tab state from redux (jobs and view state)
      const nextScriptId = $SqlRunnerSession.$merged.value.currentScriptId;
      const currentScriptId = $SqlRunnerSession.$source.value.currentScriptId;
      if (nextScriptId && currentScriptId !== nextScriptId) {
        const script = ScriptsResource.getResource().value?.find(
          (script) => script.id === nextScriptId,
        );
        if (!script) {
          return;
        }
        handleOpenTabScript(this.props.router)(script);
      }
    };

    const errorMessageObject = intl.formatMessage(
      { id: "Support.error.section" },
      {
        section: intl.formatMessage({
          id: "SectionLabel.sql.editor",
        }),
      },
    );

    return (
      <>
        {this.props.location.query.scriptId && (
          <ScriptDocumentTitle
            currentScriptId={this.props.location.query.scriptId}
          />
        )}
        {/* <ExplorePage /> always assumes there are two HTML elements (grid-template-rows: auto 1fr)
      so if the NavCrumbs don't render we need a dummy <div> in the DOM */}
        {showNavCrumbs ? <NavCrumbs /> : <HomePageTop />}
        <div
          className={classNames(
            "explorePage",
            "dremio-layout-container",
            this.state.sidebarCollapsed && "--collpase",
          )}
          ref={this.explorePageRef}
        >
          <div
            className={clsx("dremioContent", {
              "--withTabs": isTabbable && isMultiTabEnabled,
            })}
          >
            {!(isTabbable && isMultiTabEnabled) && (
              <div className="dremioContent__header">
                <ExploreInfoHeader
                  dataset={dataset}
                  pageType={pageType}
                  nessieState={this.props.nessieState}
                />
              </div>
            )}
            {isDatasetLoading ? (
              <Spinner className="dremioContent__spinner" />
            ) : shouldShowCodeView ? (
              <ErrorBoundary title={errorMessageObject}>
                <CodeView
                  contentClass={this.getFormattedTableDefinition(
                    shouldShowCodeView,
                    true,
                  )}
                >
                  <SyntaxHighlighter language="sql">
                    {formattedSQL}
                  </SyntaxHighlighter>
                  <span className="inner-actions">
                    <QueryDataset
                      fullPath={dataset.get("displayFullPath")}
                      resourceId={dataset.getIn(["displayFullPath", 0])}
                      tooltipPlacement="top"
                      tooltipPortal
                    />
                    <CopyButton contents={formattedSQL} />
                  </span>
                </CodeView>
              </ErrorBoundary>
            ) : (
              <div className="dremioContent__main">
                {this.props.availablePageTypes.map((page) => (
                  <TabPanel
                    key={`${page}Panel`}
                    {...getControlledTabPanelProps({
                      id: `${page}Panel`,
                      labelledBy: page,
                      currentTab:
                        // Details does not currently have a corresponding tab, this should be fixed in the UX for accessibility
                        this.props.pageType === "details"
                          ? PageTypes.default
                          : this.props.pageType,
                    })}
                    className="dremioContent__content-tabpanel"
                  >
                    {this.getSidebar()}

                    {!isArsLoading && !isArsEnabled ? (
                      <HistoryLineController
                        dataset={dataset}
                        location={this.props.location}
                        pageType={pageType}
                      />
                    ) : (
                      <div />
                    )}

                    <div
                      ref={this.observeRef}
                      className={clsx("dremioContent__content", {
                        "dremioContent__content--withTabs":
                          isMultiTabEnabled && isTabbable,
                      })}
                      style={{
                        maxHeight: getExploreContentHeight(
                          this.headerRef?.offsetHeight,
                          window.innerHeight,
                          isMultiTabEnabled && isTabbable
                            ? 0
                            : EXPLORE_HEADER_HEIGHT,
                        ),
                      }}
                    >
                      <MultiTabIsEnabledProvider>
                        {isTabbable && (
                          <SqlRunnerTabs
                            canClose={() =>
                              $SqlRunnerSession.$merged.value?.scriptIds
                                .length > 1
                            }
                            onTabSelected={(tabId) => {
                              const script =
                                ScriptsResource.getResource().value?.find(
                                  (script) => script.id === tabId,
                                );
                              if (!script) {
                                return;
                              }
                              if (
                                isMultiQueryRunning &&
                                exploreUtils.hasUnsubmittedQueries(
                                  this.props.queryStatuses,
                                )
                              ) {
                                this.onLeaveTabWhileQueriesRunning(() =>
                                  handleOpenTabScript(this.props.router)(
                                    script,
                                  ),
                                );
                              } else {
                                handleOpenTabScript(this.props.router)(script);
                              }
                            }}
                            onTabClosed={(scriptId) => {
                              const script =
                                ScriptsResource.getResource().value?.find(
                                  (script) => script.id === scriptId,
                                );
                              if (
                                isTemporaryScript(script) &&
                                script.content.trim().length > 0
                              ) {
                                this.setState({
                                  requestedTemporaryTabClose: scriptId,
                                });
                                return;
                              }
                              handleTabClosed(scriptId, {
                                shouldDelete: isTemporaryScript(script),
                              });
                            }}
                            onScriptRename={(scriptId) => {
                              const script =
                                ScriptsResource.getResource().value?.find(
                                  (script) => script.id === scriptId,
                                );
                              this.onRenameTab(script);
                            }}
                            tabActions={(tabId) => {
                              const isTemporary = (() => {
                                try {
                                  return isTemporaryScript(
                                    ScriptsResource.getResource().value.find(
                                      (script) =>
                                        script.id ===
                                        $SqlRunnerSession.$source.value
                                          .currentScriptId,
                                    ),
                                  );
                                } catch (e) {
                                  return false;
                                }
                              })();
                              return [
                                {
                                  id: "rename",
                                  label: isTemporary
                                    ? "Save"
                                    : intl.formatMessage({
                                        id: "Common.Rename",
                                      }),
                                  handler: () => {
                                    const script =
                                      ScriptsResource.getResource().value?.find(
                                        (script) => script.id === tabId,
                                      );
                                    this.onRenameTab(script);
                                  },
                                },
                                {
                                  id: "saveScript",
                                  label: intl.formatMessage({
                                    id: "NewQuery.SaveScriptAs",
                                  }),
                                  handler: () => {
                                    window.sqlUtils.handleSaveScriptAs();
                                  },
                                },
                                {
                                  id: "saveView",
                                  label: intl.formatMessage({
                                    id: "NewQuery.SaveAsViewBtn",
                                  }),
                                  handler: () => {
                                    window.sqlUtils.handleSaveViewAs();
                                  },
                                },
                                {
                                  id: "close",
                                  label: intl.formatMessage({
                                    id: "Common.Close",
                                  }),
                                  handler: () => {
                                    const script =
                                      ScriptsResource.getResource().value?.find(
                                        (script) => script.id === tabId,
                                      );
                                    if (isTemporaryScript(script)) {
                                      this.setState({
                                        requestedTemporaryTabClose: tabId,
                                      });
                                      return;
                                    }
                                    handleTabClosed(tabId);
                                  },
                                  disabled:
                                    $SqlRunnerSession.$merged.value?.scriptIds
                                      .length <= 1,
                                },
                                {
                                  id: "closeOthers",
                                  label: t(
                                    "Sonar.SqlRunner.Tab.Actions.CloseOthers",
                                  ),
                                  handler: () => {
                                    handleCloseOthers();
                                  },
                                  disabled: !getCloseableScripts().length,
                                },
                              ];
                            }}
                            onNewTabCreated={async () => {
                              await ScriptsResource.fetch();
                              await fetchAllAndMineScripts(
                                this.props.fetchScripts,
                                null,
                              );
                              handleOpenTabScript(this.props.router)(
                                ScriptsResource.getResource().value.find(
                                  (script) =>
                                    script.id ===
                                    $SqlRunnerSession.$source.value
                                      .currentScriptId,
                                ),
                              );
                            }}
                          />
                        )}
                      </MultiTabIsEnabledProvider>
                      {this.getContentHeader()}

                      <ErrorBoundary title={errorMessageObject}>
                        {this.getUpperContent()}
                      </ErrorBoundary>

                      {this.getTabsBlock()}

                      <div
                        className={`dremioContent__table ${
                          pageType === PageTypes.wiki ? "fullHeight" : ""
                        }`}
                      >
                        {this.getControlsBlock()}

                        <SqlErrorSection
                          visible={this.props.isError}
                          errorData={this.props.errorData}
                        />

                        {this.getBottomContent()}
                      </div>
                    </div>
                    {this.getDatasetDetailsPanel()}
                  </TabPanel>
                ))}
              </div>
            )}
          </div>
        </div>
        {this.state.SQLScriptsRenameDialog.isOpen && (
          <SQLScriptRenameDialog {...this.state.SQLScriptsRenameDialog} />
        )}
        <SQLScriptLeaveTabDialog {...this.state.SQLScriptsLeaveTabDialog} />
        <ModalContainer
          close={() =>
            this.setState({
              requestedTemporaryTabClose: null,
            })
          }
          isOpen={!!this.state.requestedTemporaryTabClose}
        >
          <TemporaryTabConfirmDeleteDialog
            onCancel={() =>
              this.setState({
                requestedTemporaryTabClose: null,
              })
            }
            onDiscard={() => {
              this.setState({
                requestedTemporaryTabClose: null,
              });
              handleTabClosed(this.state.requestedTemporaryTabClose);
            }}
            onSave={() => {
              this.setState({
                requestedTemporaryTabClose: null,
              });
              this.onRenameTab(
                ScriptsResource.getResource().value?.find(
                  (script) =>
                    script.id === this.state.requestedTemporaryTabClose,
                ),
              );
            }}
          />
        </ModalContainer>
      </>
    );
  }
}

function mapStateToProps(state, ownProps) {
  const { location, dataset } = ownProps;
  const version = location.query && location.query.version;
  const loc = rmProjectBase(location.pathname);
  const isSource = loc.startsWith("/source/");
  const sources = getSourceMap(state);
  let nessieState;
  if (isSource && sources?.size > 0) {
    const sourceId = loc.split("/")[2];
    const source = sources.get(sourceId);
    if (source && isVersionedSource(source.get("type"))) {
      nessieState = selectState(state.nessie, `ref/${source.get("name")}`);
    }
  }
  const entityId = getDatasetEntityId(state, location);
  const exploreViewState = getExploreViewState(state);
  const explorePageState = getExploreState(state);
  const datasetVersion = dataset.get("datasetVersion");
  const jobProgress = getJobProgress(state, version);
  const isSqlQuery = exploreUtils.isSqlEditorTab(location);
  const scripts = state?.resources?.scripts;

  const queryStatuses = getQueryStatuses(state);

  // RBAC needs the permissions sent to the Acceleration components and passed down, in case any component along the way needs to be able to alter reflections
  const datasetUI = state.resources.entities.get("datasetUI");
  const fullDataset = state.resources.entities.get("fullDataset");
  const firstDataset = datasetUI && datasetUI.first();
  const lastDataset = fullDataset && fullDataset.last();
  const permissions =
    firstDataset && Map.isMap(firstDataset) && firstDataset.get("permissions");

  const jobSummaries = getJobSummaries(state);

  const canSelect = permissions && permissions.get("canSelect");
  const isEnterpriseFlag = isEnterprise && isEnterprise();
  const isCommunityFlag = isCommunity && isCommunity();
  let supportFlagsObj = state.supportFlags;
  //  if its enterprise read supportFlags from dremioConfig instead of API
  if (isEnterpriseFlag || isCommunityFlag) {
    supportFlagsObj = {
      "client.tools.powerbi": config.analyzeTools.powerbi.enabled,
      "client.tools.tableau": config.analyzeTools.tableau.enabled,
      "ui.upload.allow": config.allowAutoComplete,
    };
  } else {
    supportFlagsObj = state.supportFlags;
  }

  return {
    entityId,
    lastDataset,
    datasetVersion,
    exploreViewState,
    approximate: getApproximate(state, datasetVersion),
    currentSql: explorePageState.view.currentSql,
    canSelect,
    version,
    jobProgress,
    queryContext: explorePageState.view.queryContext,
    isSqlQuery,
    queryStatuses,
    jobSummaries,
    previousMultiSql: explorePageState.view.previousMultiSql,
    selectedSql: explorePageState.view.selectedSql,
    querySelections: explorePageState.view.querySelections,
    isMultiQueryRunning: explorePageState.view.isMultiQueryRunning,
    queryTabNumber: explorePageState.view.queryTabNumber,
    supportFlagsObj,
    nessieState,
    scripts,
  };
}

export default compose(
  withRouter,
  withIsMultiTabEnabled,
  connect(
    mapStateToProps,
    {
      removeTabView,
      addNotification,
      runDatasetSql,
      previewDatasetSql,
      updateColumnFilter,
      loadSourceListData,
      setQueryStatuses: setQueryStatusesFunc,
      cancelJob: cancelJobAndShowNotification,
      setQuerySelections,
      setPreviousAndCurrentSql,
      setSelectedSql,
      setCurrentSql,
      setCustomDefaultSql,
      setQueryTabNumber,
      fetchScripts,
      fetchSupportFlags,
      fetchJobSummary,
      fetchJobDetails,
      clearExploreJobs,
      toggleExploreSql,
      setResizeProgressState,
      loadJobTabs,
      loadJobResults,
    },
    null,
    { forwardRef: true },
  ),
  injectIntl,
  withDatasetChanges,
  withAvailablePageTypes,
  withCatalogARSFlag,
)(ExplorePageContentWrapper);
