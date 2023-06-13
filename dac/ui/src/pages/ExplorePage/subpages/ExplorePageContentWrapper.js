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
import Immutable, { Map } from "immutable";
import { connect } from "react-redux";
import PropTypes from "prop-types";
import classNames from "clsx";
import Mousetrap from "mousetrap";
import { cloneDeep, debounce } from "lodash";

import { withRouter } from "react-router";
import { injectIntl } from "react-intl";

import { getQueryStatuses, getJobList } from "selectors/jobs";

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
import { runDatasetSql, previewDatasetSql } from "actions/explore/dataset/run";
import { loadSourceListData } from "actions/resources/sources";
import {
  navigateToExploreDefaultIfNecessary,
  constructFullPath,
} from "utils/pathUtils";
import { getExploreViewState } from "@app/selectors/resources";
import Reflections from "@app/pages/ExplorePage/subpages/reflections/Reflections";

import {
  updateColumnFilter,
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
import { addNotification } from "@app/actions/notification";
import { handleOnTabRouting } from "@app/pages/ExplorePage/components/SqlEditor/SqlQueryTabs/utils.tsx";
import {
  setQueryStatuses as setQueryStatusesFunc,
  setQueryTabNumber,
} from "actions/explore/view";
import { cancelJobAndShowNotification } from "@app/actions/jobs/jobs";
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
  getExploreContentHeight,
} from "./utils";
import ResizeObserver from "resize-observer-polyfill";
import { fetchSupportFlags } from "@app/actions/supportFlags";
import config from "@inject/utils/config";
import { isEnterprise, isCommunity } from "dyn-load/utils/versionUtils";
import exploreUtils from "@app/utils/explore/exploreUtils";
import HistoryPage from "./HistoryPage/HistoryPage";
import { HomePageTop } from "@inject/pages/HomePage/HomePageTop";
import {
  fetchFilteredJobsList,
  resetFilteredJobsList,
  JOB_PAGE_NEW_VIEW_ID,
} from "@app/actions/joblist/jobList";
import { newQuery } from "@app/exports/paths";
import { ErrorBoundary } from "@app/components/ErrorBoundary/ErrorBoundary";
import { intl } from "@app/utils/intl";
import { getSortedSources } from "@app/selectors/home";
import { isVersionedSource } from "@app/utils/sourceUtils";
import { rmProjectBase } from "dremio-ui-common/utilities/projectBase.js";
import { DATASET_PATH_FROM_ONBOARDING } from "@inject/components/SonarOnboardingModal/constants";
import { selectState } from "@app/selectors/nessie/nessie";
import { REFLECTION_ARCTIC_ENABLED } from "@app/exports/endpoints/SupportFlags/supportFlagConstants";

const newQueryLink = newQuery();
const EXPLORE_DRAG_TYPE = "explorePage";
const HISTORY_BAR_WIDTH = 34;
const SIDEBAR_MIN_WIDTH = 300;
const COLLAPSED_SIDEBAR_WIDTH = 36;
const SQL_EDITOR_PADDING = 20;
const EXTRA_PAGE_WIDTH = HISTORY_BAR_WIDTH + SQL_EDITOR_PADDING;

const CREATE_NEW_QUERY = "SELECT * FROM /* Insert dataset here */";

const resizeColumn = new ResizeObserver(
  debounce((item) => {
    const column = item[0];
    if (!column) return;

    if (column.contentRect.width <= 1000) {
      column.target.classList.add("--minimal");
    } else {
      column.target.classList.remove("--minimal");
    }
  }, 1)
);

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
    entityId: PropTypes.string,
    runDatasetSql: PropTypes.func,
    previewDatasetSql: PropTypes.func,
    canSelect: PropTypes.any,
    version: PropTypes.string,
    datasetVersion: PropTypes.string,
    lastDataset: PropTypes.any,
    addNotification: PropTypes.func,
    currentSql: PropTypes.string,
    queryContext: PropTypes.instanceOf(Immutable.List),
    datasetSql: PropTypes.string,
    isSqlQuery: PropTypes.bool,
    loadSourceListData: PropTypes.func,
    queryStatuses: PropTypes.array,
    statusesArray: PropTypes.array,
    cancelJob: PropTypes.func,
    setQueryStatuses: PropTypes.func,
    router: PropTypes.any,
    querySelections: PropTypes.array,
    setQuerySelections: PropTypes.func,
    previousMultiSql: PropTypes.string,
    setPreviousAndCurrentSql: PropTypes.func,
    selectedSql: PropTypes.string,
    setSelectedSql: PropTypes.func,
    setCustomDefaultSql: PropTypes.func,
    isMultiQueryRunning: PropTypes.bool,
    queryTabNumber: PropTypes.number,
    setQueryTabNumber: PropTypes.func,
    supportFlagsObj: PropTypes.object,
    fetchSupportFlags: PropTypes.func,
    jobDetails: PropTypes.any,
    isNessieOrArcticSource: PropTypes.bool,
    fetchFilteredJobsList: PropTypes.func,
    resetFilteredJobsList: PropTypes.func,
    setResizeProgressState: PropTypes.func,
    toggleExploreSql: PropTypes.func,
    nessieState: PropTypes.object,
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
    };

    this.editorRef = createRef();
    this.headerRef = createRef();
    this.dremioSideBarRef = createRef();
    this.dremioSideBarDrag = createRef();
    this.explorePageRef = createRef();
  }
  timeoutRef = undefined;

  componentDidMount() {
    // fetch support flags here for powerbi and tableau only if its not enterprise
    if (!(isEnterprise?.() || isCommunity?.())) {
      this.props.fetchSupportFlags("client.tools.tableau");
      this.props.fetchSupportFlags("client.tools.powerbi");
    }
    if (!isCommunity?.()) {
      this.props.fetchSupportFlags(REFLECTION_ARCTIC_ENABLED);
    }

    Mousetrap.bind(["mod+enter", "mod+shift+enter"], this.kbdShorthand);
    this.getUserOperatingSystem();
    this.onJobIdChange();
    this.props.loadSourceListData();
    this.onTabRender();
    this.handleResize();
    this.props.resetFilteredJobsList();

    this.headerRef = document.querySelector(".c-homePageTop");

    document.addEventListener("mouseup", this.sidebarMouseUp);
    document.addEventListener("mousemove", this.sidebarMouseMove);
    window.addEventListener("resize", this.handleResize);

    // Observing content column
    resizeColumn.observe(document.querySelector(".dremioContent__content"));

    const { location, router, setCustomDefaultSql, setPreviousAndCurrentSql } =
      this.props;
    if (location.query?.create) {
      setPreviousAndCurrentSql({ sql: CREATE_NEW_QUERY });
    } else if (location.query?.queryPath) {
      // executes when navigating to the editor to query a dataset
      const sql = exploreUtils.createNewQueryFromDatasetOverlay(
        location.query.queryPath
      );

      setPreviousAndCurrentSql({ sql });
      setCustomDefaultSql({ sql });

      router.replace({
        ...location,
        query: {
          ...location.query,
          queryPath: undefined,
        },
      });
    }

    if (sessionStorage.getItem(DATASET_PATH_FROM_ONBOARDING)) {
      // updates the sql when redirected at the end of onboarding flow
      sessionStorage.getItem(DATASET_PATH_FROM_ONBOARDING);
      setPreviousAndCurrentSql({
        sql: exploreUtils.createNewQueryFromDatasetOverlay(
          sessionStorage.getItem(DATASET_PATH_FROM_ONBOARDING)
        ),
      });
      sessionStorage.removeItem(DATASET_PATH_FROM_ONBOARDING);
    }
  }

  componentWillUnmount() {
    Mousetrap.unbind(["mod+enter", "mod+shift+enter"]);
    document.removeEventListener("mouseup", this.sidebarMouseUp);
    document.removeEventListener("mousemove", this.sidebarMouseMove);
    window.removeEventListener("resize", this.handleResize);
    resizeColumn.unobserve(document.querySelector(".dremioContent__content"));
    clearTimeout(this.timeoutRef);

    // Sidebar resizing
    if (this.dremioSideBarDrag.current) {
      this.dremioSideBarDrag.current.removeEventListener(
        "mousedown",
        this.sidebarMouseDown
      );
    }
  }

  componentDidUpdate(prevProps) {
    const {
      addNotification: addSuccessNotification,
      intl: { formatMessage },
      jobProgress,
      setQueryStatuses: newQueryStatuses,
      setPreviousAndCurrentSql: newPreviousAndCurrentSql,
      datasetSql,
      currentSql,
      datasetVersion,
      queryStatuses,
      queryTabNumber,
      statusesArray,
      selectedSql,
      setSelectedSql: newSelectedSql,
      setQuerySelections: newQuerySelections,
      isMultiQueryRunning,
      jobDetails,
      location,
      router,
    } = this.props;
    const loc = rmProjectBase(location.pathname);
    // This is specific to using the overlay query button from the dataset page.
    if (
      loc === newQueryLink &&
      location.query?.queryPath &&
      this.getMonacoEditorInstance()
    ) {
      this.insertFullPathAtCursor(
        exploreUtils.createNewQueryFromDatasetOverlay(location.query.queryPath)
      );

      router.replace({
        ...location,
        query: {
          ...location.query,
          queryPath: undefined,
        },
      });
    }

    // if a single job, force the explore table to be in view
    if (
      statusesArray.length === 1 &&
      prevProps.statusesArray.length === statusesArray.length &&
      !prevProps.statusesArray[0] &&
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
        // Retrieve the Job's details to see if the results were truncated from the backend
        if (isDifferentTab && jobDetails) {
          if (jobDetails.getIn(["stats", "isOutputLimited"])) {
            addSuccessNotification(
              formatMessage(
                { id: "Explore.Run.Warning" },
                {
                  rows: jobDetails
                    .getIn(["stats", "outputRecords"])
                    .toLocaleString(),
                }
              ),
              "success"
            );
          }
        }

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
                newQueryStatuses({
                  statuses: [
                    {
                      sqlStatement: datasetSql,
                      cancelled: false,
                      jobId: jobProgress.jobId,
                      version: datasetVersion,
                    },
                  ],
                });
                this.props.fetchFilteredJobsList(
                  jobProgress.jobId,
                  JOB_PAGE_NEW_VIEW_ID
                );
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

    // Update when switching between Dataset and SQL Runner
    if (prevProps.isSqlQuery !== this.props.isSqlQuery) {
      this.setPanelCollapse();
    }

    this.handleDisableButton(prevProps);

    // Sidebar resizing
    if (this.dremioSideBarDrag.current) {
      this.dremioSideBarDrag.current.addEventListener(
        "mousedown",
        this.sidebarMouseDown
      );
    }
  }

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

  onTabChange(tabIndex) {
    const { queryStatuses, location, router, isMultiQueryRunning } = this.props;
    const showJobs = tabIndex === 0;
    const currentJob = queryStatuses[tabIndex - 1];
    this.setState({
      showJobsTable: showJobs,
    });
    this.props.setQueryTabNumber({ tabNumber: tabIndex });
    this.handleSqlSelection(tabIndex);
    if (tabIndex !== 0) {
      handleOnTabRouting(
        tabIndex,
        currentJob.jobId,
        currentJob.version,
        location,
        router,
        isMultiQueryRunning
      );
    } else {
      handleOnTabRouting(
        tabIndex,
        undefined,
        undefined,
        location,
        router,
        isMultiQueryRunning
      );
    }
  }

  handleSqlSelection(tabIndex) {
    const { querySelections, currentSql, previousMultiSql } = this.props;
    const editorQueries = extractQueries(currentSql);
    const isNotEdited = !!previousMultiSql && currentSql === previousMultiSql;
    const isConsistent = editorQueries.length === querySelections.length;
    if (
      isNotEdited &&
      isConsistent &&
      querySelections.length > 0 &&
      tabIndex <= querySelections.length
    ) {
      if (tabIndex > 0) {
        let currentSelection = this.getMonacoEditorInstance().getSelections();
        const newSelection = querySelections[tabIndex - 1];
        currentSelection = { ...newSelection };
        this.getMonacoEditorInstance().setSelection(currentSelection);
        this.getMonacoEditorInstance().revealLine(
          currentSelection.startLineNumber
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

  // TODO: look into this, see if it needs to support query highlighting
  kbdShorthand = (e) => {
    if (!e) return;

    const { pageType, location } = this.props;
    navigateToExploreDefaultIfNecessary(
      pageType,
      location,
      this.context.router
    );

    if (e.shiftKey) {
      this.props.runDatasetSql();
    } else {
      this.props.previewDatasetSql();
    }
  };

  getMonacoEditorInstance() {
    // TODO: Refactor this `ref` to use Context
    return this.editorRef.current?.explorePageMainContentRef
      ?.topSplitterContentRef?.sqlEditorControllerRef?.sqlAutoCompleteRef
      ?.monacoEditorComponent?.editor;
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

  insertFullPath(pathList, ranges) {
    if (typeof pathList === "string") {
      this.insertAtRanges(pathList, ranges);
    } else {
      const text = constructFullPath(pathList);
      this.insertAtRanges(text, ranges);
    }
  }

  insertAtRanges(
    text,
    ranges = this.getMonacoEditorInstance().getSelections()
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

  getSelectedSql = () => {
    if (this.getMonacoEditorInstance() === undefined) {
      return "";
    }

    const selection = this.getMonacoEditorInstance().getSelection();
    const range = {
      endColumn: selection.endColumn,
      endLineNumber: selection.endLineNumber,
      startColumn: selection.startColumn,
      startLineNumber: selection.startLineNumber,
    };
    return this.getMonacoEditorInstance().getModel().getValueInRange(range);
  };

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
    if (this.explorePageRef?.current) {
      return (
        this.explorePageRef.current.offsetWidth -
        sideBarWidth -
        EXTRA_PAGE_WIDTH
      );
    }
  };

  toggleSqlPaneDisplay = () => {
    clearTimeout(this.timeoutRef);
    this.props.setResizeProgressState(true);
    this.props.toggleExploreSql?.();
    this.timeoutRef = setTimeout(
      () => this.props.setResizeProgressState(false),
      500
    ); // ref for calculating height isn't auto updated
  };

  getBottomContent() {
    const {
      dataset,
      pageType,
      location,
      entityId,
      canSelect,
      queryStatuses,
      queryTabNumber,
      statusesArray,
      cancelJob,
      setQueryStatuses: newQueryStatuses,
      isNessieOrArcticSource,
    } = this.props;

    const { showJobsTable } = this.state;

    const locationQuery = location.query;
    const tabStatusArr = assemblePendingOrRunningTabContent(
      queryStatuses,
      statusesArray,
      newQueryStatuses,
      cancelJob,
      this.cancelPendingSql
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
            showTags={!isNessieOrArcticSource}
            showWikiContent={!isNessieOrArcticSource}
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
      datasetSql,
      isMultiQueryRunning,
      currentSql,
      previousMultiSql,
      statusesArray,
      queryTabNumber,
    } = this.props;

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
              queryStatuses.length === 0
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
      location,
      pageType,
      statusesArray,
      isMultiQueryRunning,
      queryTabNumber,
      sqlState,
    } = this.props;
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
    } = this.props;

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
            getSelectedSql={this.getSelectedSql}
            statusesArray={this.props.statusesArray}
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
    this.setState({ sidebarResize: true });
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
              this.state.sidebarResize && "--active"
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
      this.handleResize()
    );
  };

  updateColumnFilter = (columnFilter) => {
    this.props.updateColumnFilter(columnFilter);
  };

  render() {
    return (
      <>
        {/* <ExplorePage /> always assumes there are two HTML elements (grid-template-rows: auto 1fr)
      so if the NavCrumbs don't render we need a dummy <div> in the DOM */}
        {showNavCrumbs ? <NavCrumbs /> : <HomePageTop />}
        <div
          className={classNames(
            "explorePage",
            "dremio-layout-container",
            this.state.sidebarCollapsed && "--collpase"
          )}
          ref={this.explorePageRef}
        >
          <div className="dremioContent">
            <div className="dremioContent__header">
              <ExploreInfoHeader
                dataset={this.props.dataset}
                pageType={this.props.pageType}
                toggleRightTree={this.props.toggleRightTree}
                rightTreeVisible={this.props.rightTreeVisible}
                exploreViewState={this.props.exploreViewState}
                nessieState={this.props.nessieState}
              />
            </div>

            <div className="dremioContent__main">
              {this.getSidebar()}

              <HistoryLineController
                dataset={this.props.dataset}
                location={this.props.location}
                pageType={this.props.pageType}
              />

              <div
                className="dremioContent__content"
                style={{
                  maxHeight: getExploreContentHeight(
                    this.headerRef?.offsetHeight,
                    window.innerHeight
                  ),
                }}
              >
                {this.getContentHeader()}

                <ErrorBoundary
                  title={intl.formatMessage(
                    { id: "Support.error.section" },
                    {
                      section: intl.formatMessage({
                        id: "SectionLabel.sql.editor",
                      }),
                    }
                  )}
                >
                  {this.getUpperContent()}
                </ErrorBoundary>

                {this.getTabsBlock()}

                <div
                  className={`dremioContent__table ${
                    this.props.pageType === PageTypes.wiki ? "fullHeight" : ""
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
            </div>
          </div>
        </div>
      </>
    );
  }
}

function mapStateToProps(state, ownProps) {
  const { location, dataset } = ownProps;
  const version = location.query && location.query.version;
  const loc = rmProjectBase(location.pathname);
  const isSource = loc.startsWith("/source/");
  const sources = getSortedSources(state);
  let isNessieOrArcticSource = false;
  let nessieState;
  if (isSource && sources?.size > 0) {
    const sourceId = loc.split("/")[2];
    const source = (sources || []).find(
      (item) => item.get("name") === sourceId
    );
    if (source && isVersionedSource(source.get("type"))) {
      isNessieOrArcticSource = true;
      nessieState = selectState(state.nessie, `ref/${source.get("name")}`);
    }
  }
  const entityId = getDatasetEntityId(state, location);
  const exploreViewState = getExploreViewState(state);
  const explorePageState = getExploreState(state);
  const datasetVersion = dataset.get("datasetVersion");
  const jobProgress = getJobProgress(state, version);
  const isSqlQuery = exploreUtils.isSqlEditorTab(location);
  const jobDetails = state?.resources?.entities?.getIn([
    "jobDetails",
    jobProgress?.jobId,
  ]);

  const queryStatuses = getQueryStatuses(state);

  // RBAC needs the permissions sent to the Acceleration components and passed down, in case any component along the way needs to be able to alter reflections
  const datasetUI = state.resources.entities.get("datasetUI");
  const fullDataset = state.resources.entities.get("fullDataset");
  const firstDataset = datasetUI && datasetUI.first();
  const lastDataset = fullDataset && fullDataset.last();
  const permissions =
    firstDataset && Map.isMap(firstDataset) && firstDataset.get("permissions");
  const jobListForStatusArray = getJobList(state, ownProps);
  const statusesArray = queryStatuses.map((job) => {
    const filteredResult = jobListForStatusArray.filter((listedJob) => {
      return job.jobId === listedJob.get("id");
    });
    const jobResult =
      Immutable.List.isList(filteredResult) && filteredResult.first();
    if (jobResult && jobResult.get("state")) {
      return jobResult.get("state");
    }

    if (job.error) {
      return "FAILED";
    }
    if (job.cancelled) {
      return "REMOVED";
    }

    return;
  });

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
    statusesArray,
    previousMultiSql: explorePageState.view.previousMultiSql,
    selectedSql: explorePageState.view.selectedSql,
    querySelections: explorePageState.view.querySelections,
    isMultiQueryRunning: explorePageState.view.isMultiQueryRunning,
    queryTabNumber: explorePageState.view.queryTabNumber,
    supportFlagsObj,
    jobDetails,
    isNessieOrArcticSource,
    nessieState,
  };
}

export default withRouter(
  connect(
    mapStateToProps,
    {
      runDatasetSql,
      previewDatasetSql,
      updateColumnFilter,
      addNotification,
      loadSourceListData,
      setQueryStatuses: setQueryStatusesFunc,
      cancelJob: cancelJobAndShowNotification,
      setQuerySelections,
      setPreviousAndCurrentSql,
      setSelectedSql,
      setCustomDefaultSql,
      setQueryTabNumber,
      fetchSupportFlags,
      fetchFilteredJobsList,
      resetFilteredJobsList,
      toggleExploreSql,
      setResizeProgressState,
    },
    null,
    { forwardRef: true }
  )(withDatasetChanges(injectIntl(ExplorePageContentWrapper)))
);
