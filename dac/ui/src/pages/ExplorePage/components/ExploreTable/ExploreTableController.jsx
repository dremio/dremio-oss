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
import { PureComponent } from "react";
import Immutable from "immutable";
import { connect } from "react-redux";
import { FormattedMessage } from "react-intl";
import { intl } from "@app/utils/intl";
import PropTypes from "prop-types";
import { Spinner } from "dremio-ui-lib/components";

import exploreUtils from "utils/explore/exploreUtils";
import exploreTransforms from "utils/exploreTransforms";
import jobsUtils from "@app/utils/jobsUtils";

import { LIST, MAP, STRUCT } from "@app/constants/DataTypes";
import * as jobPaths from "dremio-ui-common/paths/jobs.js";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";

import {
  getPeekData,
  getImmutableTable,
  getPaginationUrl,
  getExploreState,
  getColumnFilter,
} from "@app/selectors/explore";
import { getViewState } from "selectors/resources";
import { resetViewState } from "actions/resources";
import { accessEntity } from "actions/resources/lru";

import {
  transformHistoryCheck,
  performTransform,
} from "actions/explore/dataset/transform";

import { FULL_CELL_VIEW_ID } from "actions/explore/dataset/data";
import { isSqlChanged } from "@app/sagas/utils";
import { ErrorBoundary } from "@app/components/OldErrorBoundary";

import { LOAD_TRANSFORM_CARDS_VIEW_ID } from "actions/explore/recommended";

import { constructFullPath } from "utils/pathUtils";
import { PageTypes } from "@app/pages/ExplorePage/pageTypes";
import {
  sqlEditorTableColumns,
  pendingSQLJobs,
} from "@app/constants/Constants";
import {
  renderJobStatus,
  getJobIdsList,
  getSqlList,
  getFilteredSqlList,
} from "@app/utils/jobsUtils";
import StatefulTableViewer from "@app/components/StatefulTableViewer";
import { Button, IconButton } from "dremio-ui-lib/components";

import JobListingPage from "@app/pages/JobPageNew/JobListingPage";
import { parseQueryState } from "utils/jobsQueryState";
import { updateQueryState } from "actions/jobs/jobs";
import { Tooltip } from "dremio-ui-lib";
import { cancelJobAndShowNotification } from "@app/actions/jobs/jobs";

import Message from "@app/components/Message";
import apiUtils from "@app/utils/apiUtils/apiUtils";
import DropdownForSelectedText from "./DropdownForSelectedText";
import ExploreCellLargeOverlay from "./ExploreCellLargeOverlay";
import ExploreTable from "./ExploreTable";
import "./ExploreTableController.less";

export class ExploreTableController extends PureComponent {
  static propTypes = {
    pageType: PropTypes.string,
    dataset: PropTypes.instanceOf(Immutable.Map),
    tableData: PropTypes.instanceOf(Immutable.Map).isRequired,
    previewVersion: PropTypes.string,
    paginationUrl: PropTypes.string,
    isDumbTable: PropTypes.bool,
    exploreViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    cardsViewState: PropTypes.instanceOf(Immutable.Map),
    fullCellViewState: PropTypes.instanceOf(Immutable.Map),
    currentSql: PropTypes.string,
    queryContext: PropTypes.instanceOf(Immutable.List),
    location: PropTypes.object,
    dragType: PropTypes.string,
    width: PropTypes.number,
    height: PropTypes.number,
    widthScale: PropTypes.number,
    rightTreeVisible: PropTypes.bool,
    isResizeInProgress: PropTypes.bool,
    children: PropTypes.node,
    getTableHeight: PropTypes.func,
    shouldRenderInvisibles: PropTypes.bool,
    columnFilter: PropTypes.string,
    isMultiQueryRunning: PropTypes.bool,
    // Actions
    resetViewState: PropTypes.func,
    transformHistoryCheck: PropTypes.func,
    performTransform: PropTypes.func,
    confirmTransform: PropTypes.func,
    accessEntity: PropTypes.func.isRequired,
    canSelect: PropTypes.any,
    showJobsTable: PropTypes.bool,
    handleTabChange: PropTypes.func,
    currentJobsMap: PropTypes.array,
    cancelPendingSql: PropTypes.func,
    tabStatusArr: PropTypes.array,
    sqlList: PropTypes.array,
    queryTabNumber: PropTypes.number,
    jobIdList: PropTypes.array,
    queryState: PropTypes.object,
    cancelJob: PropTypes.func,
    previousMultiSql: PropTypes.string,
    queryStatuses: PropTypes.array,
    queryFilter: PropTypes.string,
  };

  static contextTypes = {
    router: PropTypes.object,
  };

  static defaultProps = {
    dataset: Immutable.Map(),
  };

  transformPreconfirmed = false; // eslint-disable-line react/sort-comp

  constructor(props) {
    super(props);
    this.openDetailsWizard = this.openDetailsWizard.bind(this);

    this.handleCellTextSelect = this.handleCellTextSelect.bind(this);
    this.handleCellShowMore = this.handleCellShowMore.bind(this);
    this.selectAll = this.selectAll.bind(this);
    this.selectItemsOfList = this.selectItemsOfList.bind(this);
    this.processPendingJobs = this.processPendingJobs.bind(this);
    this.renderButtonsForJobsList = this.renderButtonsForJobsList.bind(this);
    this.renderButtonsForSqlList = this.renderButtonsForSqlList.bind(this);

    this.state = {
      activeTextSelect: null,
      openPopover: false,
      activeCell: null,
      pendingJobs: Immutable.List(),
      jobsTableHeight: 0,
      pendingSQLJobsTableHeight: 0,
    };
  }

  UNSAFE_componentWillReceiveProps(nextProps) {
    const { isGrayed } = this.state;
    const isContextChanged = this.isContextChanged(nextProps);

    const newIsGreyed =
      nextProps.pageType === PageTypes.default &&
      (this.isSqlChanged(nextProps) || isContextChanged);
    if (isGrayed !== newIsGreyed) {
      this.setState({ isGrayed: newIsGreyed });
    }

    const nextVersion =
      nextProps.tableData && nextProps.tableData.get("version");

    if (nextVersion && nextVersion !== this.props.tableData.get("version")) {
      this.transformPreconfirmed = false;
      this.props.accessEntity("tableData", nextVersion);
    }
  }

  componentDidMount() {
    const { currentJobsMap } = this.props;
    currentJobsMap && this.getUpdatedTableHeights(true, true);
  }

  componentDidUpdate(prevProps) {
    const { jobIdList, sqlList, currentJobsMap, isMultiQueryRunning } =
      this.props;
    if (
      currentJobsMap &&
      (prevProps.jobIdList.length !== jobIdList.length ||
        prevProps.sqlList.length !== sqlList.length)
    ) {
      this.getUpdatedTableHeights(
        prevProps.jobIdList.length !== jobIdList.length,
        prevProps.sqlList.length !== sqlList.length,
      );
    }

    if (isMultiQueryRunning && !prevProps.isMultiQueryRunning) {
      this.hideDrop();
    }
  }

  getUpdatedTableHeights(recalcJobs, recalcSqls) {
    const { jobIdList, sqlList } = this.props;

    if (recalcJobs) {
      const newSize =
        sqlList.length === 0 ? "100%" : `${(jobIdList.length + 1) * 40}px`;
      this.setState({
        jobsTableHeight: newSize,
      });
    }
    if (recalcSqls) {
      const newSize =
        jobIdList.length === 0 ? "100%" : `${(sqlList.length + 1) * 40}px`;
      this.setState({
        pendingSQLJobsTableHeight: newSize,
      });
    }
  }

  getNextTableAfterTransform({ data, tableData }) {
    const hash = {
      DROP: () =>
        exploreTransforms.dropColumn({
          name: data.columnName,
          table: tableData,
        }),
      RENAME: () =>
        exploreTransforms.renameColumn({
          name: data.columnName,
          nextName: data.newColumnName,
          table: tableData,
        }),
      DESC: () => tableData,
      ASC: () => tableData,
      MULTIPLY: () => tableData,
    };
    return exploreTransforms.isTransformOptimistic(data.type)
      ? (hash[data.type] && hash[data.type]()) || null
      : null;
  }

  selectAll(elem, columnType, columnName, cellText, cellValue) {
    const { pathname, query, state } = this.props.location;
    const isNull = cellValue === null;
    const text = isNull ? null : cellText;
    const length = isNull ? 0 : text.length;
    const model = {
      cellText: text,
      offset: 0,
      columnName,
      length,
    };
    const position = exploreUtils.selectAll(elem);
    this.context.router.push({
      pathname,
      query,
      state: {
        ...state,
        columnName,
        columnType,
        hasSelection: true,
        selection: Immutable.fromJS(model),
      },
    });
    this.handleCellTextSelect({ ...position, columnType });
  }

  selectItemsOfList(columnText, columnName, columnType, selection) {
    const { router } = this.context;
    const { location } = this.props;
    const content = exploreUtils.getSelectionForList(
      columnText,
      columnName,
      selection,
    );
    if (!content) {
      return;
    }

    router.push({
      ...location,
      state: {
        ...location.state,
        columnName,
        columnType,
        listOfItems: content.listOfItems,
        hasSelection: true,
        selection: Immutable.fromJS(content.model),
      },
    });
    this.handleCellTextSelect({ ...content.position, columnType });
  }

  handleCellTextSelect(activeTextSelect) {
    this.setState({
      activeTextSelect,
      openPopover: !this.props.isDumbTable,
    });
  }

  handleCellShowMore(cellValue, anchor, columnType, columnName, valueUrl) {
    this.setState({
      activeCell: {
        cellValue,
        anchor,
        columnType,
        columnName,
        // for dumb table do not try to load full cell value, as server does not support this functionality
        // for that case. Lets show truncated values that was loaded.
        isTruncatedValue: Boolean(this.props.isDumbTable && valueUrl),
        valueUrl: this.props.isDumbTable ? null : valueUrl,
      },
    });
  }

  preconfirmTransform = () => {
    return new Promise((resolve) => {
      if (!this.transformPreconfirmed) {
        this.props.transformHistoryCheck(this.props.dataset, () => {
          this.transformPreconfirmed = true;
          resolve();
        });
      } else {
        resolve();
      }
    });
  };

  isSqlChanged(nextProps) {
    if (!nextProps.dataset || !this.props.dataset) {
      return false;
    }
    const datasetSql = nextProps.dataset.get("sql");
    const { currentSql } = nextProps;
    const { previousMultiSql } = this.props;

    if (!previousMultiSql) {
      return isSqlChanged(datasetSql, currentSql);
    } else {
      return currentSql !== previousMultiSql;
    }
  }

  isContextChanged(nextProps) {
    if (!nextProps.dataset || !this.props.dataset) {
      return false;
    }
    const nextContext = nextProps.dataset.get("context");
    const { queryContext } = nextProps;
    return (
      nextContext &&
      queryContext &&
      constructFullPath(nextContext) !== constructFullPath(queryContext)
    );
  }

  openDetailsWizard({ detailType, columnName, toType = null }) {
    const {
      dataset,
      queryContext,
      exploreViewState,
      queryTabNumber,
      queryStatuses,
    } = this.props;

    const callback = () => {
      const { router } = this.context;
      const { location } = this.props;
      const column = this.props.tableData
        .get("columns")
        .find((col) => col.get("name") === columnName)
        .toJS();
      const nextLocation = exploreUtils.getLocationToGoToTransformWizard({
        detailType,
        column,
        toType,
        location,
      });

      router.push(nextLocation);
    };
    this.props.performTransform({
      dataset,
      currentSql: queryStatuses[queryTabNumber - 1].sqlStatement,
      queryContext,
      viewId: exploreViewState.get("viewId"),
      callback,
      indexToModify: queryTabNumber - 1,
    });
  }

  makeTransform = (data, checkHistory = true) => {
    const {
      dataset,
      queryContext,
      exploreViewState,
      queryTabNumber,
      queryStatuses,
    } = this.props;
    const doTransform = () => {
      this.props.performTransform({
        dataset,
        currentSql: queryStatuses[queryTabNumber - 1].sqlStatement,
        queryContext,
        transformData: exploreUtils.getMappedDataForTransform(data),
        viewId: exploreViewState.get("viewId"),
        nextTable: this.getNextTableAfterTransform({
          data,
          tableData: this.props.tableData,
        }),
        indexToModify: queryTabNumber - 1,
      });
    };
    if (checkHistory) {
      this.props.transformHistoryCheck(dataset, doTransform);
    } else {
      doTransform();
    }
  };

  updateColumnName = (oldColumnName, e) => {
    const newColumnName = e.target.value;
    if (newColumnName !== oldColumnName) {
      this.makeTransform(
        { type: "RENAME", columnName: oldColumnName, newColumnName },
        false,
      );
    }
  };

  hideCellMore = () => this.setState({ activeCell: null });
  hideDrop = () =>
    this.setState({
      activeTextSelect: null,
      openPopover: false,
    });

  decorateTable = (tableData) => {
    const { location } = this.props;
    if (location.query.type === "transform") {
      const transform = exploreUtils.getTransformState(location);
      const columnType = transform.get("columnType");
      const initializeColumnTypeForExtract =
        columnType === LIST || columnType === MAP || columnType === STRUCT
          ? columnType
          : "default";
      const isDefault = initializeColumnTypeForExtract === "default";

      if (!exploreUtils.transformHasSelection(transform) && isDefault) {
        const columnName = transform.get("columnName");
        const columns = tableData.get("columns").map((column) => {
          if (column.get("name") === columnName && !column.get("status")) {
            return column
              .set("status", "TRANSFORM_ON")
              .set("color", "var(--color--brand--100)");
          }
          return column;
        });

        return tableData.set("columns", columns);
      }
    }

    return tableData;
  };

  renderExploreCellLargeOverlay() {
    return this.state.activeCell && !this.props.location.query.transformType ? (
      <ErrorBoundary>
        <ExploreCellLargeOverlay
          {...this.state.activeCell}
          isDumbTable={this.props.isDumbTable}
          fullCellViewState={this.props.fullCellViewState}
          onSelect={this.handleCellTextSelect}
          hide={this.hideCellMore}
          openPopover={this.state.openPopover}
          selectAll={this.selectAll}
          style={{ width: "400px" }}
        />
      </ErrorBoundary>
    ) : null;
  }

  renderButtonsForJobsList(status, jobId, jobAttempts) {
    const { cancelJob } = this.props;
    const projectId = getSonarContext().getSelectedProjectId?.();

    return (
      <div className="sqlEditor__jobsTable__actionButtonContainer">
        {exploreUtils.getCancellable(status) ? (
          <IconButton
            tooltip={intl.formatMessage({ id: "Query.Table.Cancel" })}
            tooltipPortal
            tooltipPlacement="top"
            onClick={() => cancelJob(jobId)}
            className="sqlEditor__jobsTable__buttons"
          >
            <dremio-icon name="sql-editor/stop" />
          </IconButton>
        ) : (
          <div className={"sqlEditor__jobsTable__buttons"}></div>
        )}
        <IconButton
          tooltip={intl.formatMessage({ id: "Job.Open.External" })}
          tooltipPortal
          tooltipPlacement="top"
          onClick={() => {
            const jobTabPath = jobsUtils.isNewJobsPage()
              ? `${jobPaths.job.link({ jobId, projectId })}${
                  jobAttempts ? `?attempts=${jobAttempts || 1}` : ""
                }`
              : `/jobs#${jobId}`;
            window.open(jobTabPath, "_blank");
          }}
          className="sqlEditor__jobsTable__buttons"
        >
          <dremio-icon name="interface/external-link" />
        </IconButton>
      </div>
    );
  }

  renderButtonsForSqlList(index) {
    const { cancelPendingSql } = this.props;
    return (
      <div className="sqlEditor__jobsTable__actionButtonContainer">
        <IconButton
          tooltip={intl.formatMessage({ id: "NewQuery.Remove" })}
          tooltipPortal
          tooltipPlacement="top"
          onClick={() => cancelPendingSql(index)}
          className={"sqlEditor__jobsTable__buttons"}
        >
          <dremio-icon name="interface/delete" />
        </IconButton>
      </div>
    );
  }

  processPendingJobs(jobs) {
    const { queryFilter } = this.props;
    const curJobs = queryFilter ? getFilteredSqlList(jobs, queryFilter) : jobs;
    return curJobs.map((sql) => {
      return {
        data: {
          jobStatus: { node: () => {} },
          sql: {
            tabIndex: sql[1],
            node: () => (
              <div className="dremio-typography-monospace">{sql[0]}</div>
            ),
          },
          buttons: {
            rowIndex: sql[1] - 1,
            node: () => this.renderButtonsForSqlList(sql[1] - 1),
          },
        },
      };
    });
  }

  renderJobsListingTable(show) {
    const {
      location,
      queryState,
      handleTabChange,
      jobIdList,
      pageType,
      queryStatuses,
    } = this.props;
    const { jobsTableHeight } = this.state;
    const isShown = show && pageType === PageTypes.default;
    const jobsTableStyles = isShown
      ? { height: jobsTableHeight }
      : { height: "0px", width: "0px", visibility: "hidden" };

    // quick fix is show/hide = rendering the jobs table since it forces the jobList to refresh and update statusArray
    // return and fix- figure out how to refresh with jobs table not rendered (using listeners, etc..)
    if (!queryStatuses || !queryStatuses.length) {
      return null;
    }

    return (
      <div style={jobsTableStyles} className="jobListPage-wrapper">
        <JobListingPage
          location={location}
          queryState={queryState}
          updateQueryState={updateQueryState}
          jobsColumns={sqlEditorTableColumns}
          renderButtons={this.renderButtonsForJobsList}
          handleTabChange={handleTabChange}
          isFromExplorePage
          jobIdList={jobIdList}
        />
      </div>
    );
  }

  render() {
    const tableData = this.decorateTable(this.props.tableData);
    const rows = tableData.get("rows");
    const columns = exploreUtils.getFilteredColumns(
      tableData.get("columns"),
      this.props.columnFilter,
    );
    const {
      canSelect,
      showJobsTable,
      handleTabChange,
      sqlList,
      jobIdList,
      tabStatusArr,
      queryTabNumber,
      currentJobsMap,
      currentSql,
      previousMultiSql,
      shouldRenderInvisibles,
      isExploreTableLoading,
    } = this.props;
    const { pendingSQLJobsTableHeight } = this.state;

    const currentTab = tabStatusArr && tabStatusArr[queryTabNumber - 1];

    const isCurrentQueryFinished =
      currentJobsMap &&
      queryTabNumber > 0 &&
      currentJobsMap[queryTabNumber - 1] &&
      !!currentJobsMap[queryTabNumber - 1].jobId;

    let jobTableContent;

    if (isExploreTableLoading) {
      // generic spinner to override the results table
      jobTableContent = (
        <div className="flex self-center justify-center">
          <Spinner className="sqlEditor__spinner" />
        </div>
      );
    } else if (currentTab) {
      jobTableContent = currentTab.error ? (
        <>
          <Message
            message={apiUtils.getThrownErrorException(currentTab.error)}
            isDismissable={false}
            messageType="error"
            style={{ padding: "0 8px" }}
          />
        </>
      ) : (
        <div className="sqlEditor__pendingTable">
          <div className="sqlEditor__pendingTable__statusMessage">
            {currentTab.renderIcon && renderJobStatus(currentTab.renderIcon)}
            {currentTab.text}
          </div>
          {currentTab.buttonFunc && (
            <Button
              variant="secondary"
              className="sqlEditor__pendingTable__button"
              onClick={currentTab.buttonFunc}
              disabled={
                currentTab.ranJob && // only disabled if cancelJob button is visible
                currentJobsMap[queryTabNumber - 1].isCancelDisabled
              }
            >
              <dremio-icon
                name={currentTab.buttonIcon}
                class={"sqlEditor__jobsTable__tab-button"}
              />
              {currentTab.buttonText}
            </Button>
          )}
        </div>
      );
    } else {
      jobTableContent = (
        <div className="sqlEditor__exploreTable">
          <ExploreTable
            pageType={this.props.pageType}
            dataset={this.props.dataset}
            rows={rows}
            columns={columns}
            paginationUrl={this.props.paginationUrl}
            exploreViewState={this.props.exploreViewState}
            cardsViewState={this.props.cardsViewState}
            isResizeInProgress={this.props.isResizeInProgress}
            widthScale={this.props.widthScale}
            openDetailsWizard={this.openDetailsWizard}
            makeTransform={this.makeTransform}
            preconfirmTransform={this.preconfirmTransform}
            width={this.props.width}
            updateColumnName={this.updateColumnName}
            height={this.props.height}
            dragType={this.props.dragType}
            rightTreeVisible={this.props.rightTreeVisible}
            onCellTextSelect={this.handleCellTextSelect}
            onCellShowMore={this.handleCellShowMore}
            selectAll={this.selectAll}
            selectItemsOfList={this.selectItemsOfList}
            isDumbTable={this.props.isDumbTable}
            getTableHeight={this.props.getTableHeight}
            isGrayed={this.state.isGrayed}
            shouldRenderInvisibles={shouldRenderInvisibles}
            canSelect={canSelect}
            isMultiSql={currentJobsMap && currentJobsMap.length > 1}
            isEdited={!!previousMultiSql && currentSql !== previousMultiSql}
            isCurrentQueryFinished={isCurrentQueryFinished}
          />
          {this.renderExploreCellLargeOverlay()}
          {this.state.activeTextSelect && (
            <DropdownForSelectedText
              dropPositions={Immutable.fromJS(this.state.activeTextSelect)}
              openPopover={this.state.openPopover}
              hideDrop={this.hideDrop}
            />
          )}
          {this.props.children}
        </div>
      );
    }

    return (
      <>
        <div
          className={
            !showJobsTable
              ? "sqlEditor__jobsTable--hidden"
              : sqlList.length === 0
                ? "sqlEditor__jobsTable--solo"
                : "sqlEditor__jobsTable--withPendingTable"
          }
        >
          {jobIdList.length > 0 && this.renderJobsListingTable(showJobsTable)}
          {sqlList.length > 0 && showJobsTable && (
            <>
              <div className="sqlEditor__strikeThroughContainer">
                <div className="sqlEditor__strikeThroughHeader">
                  <Tooltip title="These queries have not yet been submitted to the engine. They will be submitted once the current job has completed.">
                    <span className="sqlEditor__strikeThroughHeaderContent">
                      <FormattedMessage id="NewQuery.Waiting" />
                    </span>
                  </Tooltip>
                  <div className="sqlEditor__lineForStrikethrough"></div>
                </div>
              </div>
              <div style={{ height: pendingSQLJobsTableHeight }}>
                <StatefulTableViewer
                  virtualized
                  rowHeight={40}
                  columns={pendingSQLJobs}
                  tableData={this.processPendingJobs(sqlList)}
                  enableHorizontalScroll
                  disableZebraStripes
                  onClick={handleTabChange}
                />
              </div>
            </>
          )}
        </div>
        {showJobsTable ? null : jobTableContent}
      </>
    );
  }
}

function mapStateToProps(state, ownProps) {
  const location = state.routing.locationBeforeTransitions || {};
  const { dataset } = ownProps;
  const datasetVersion = dataset && dataset.get("datasetVersion");
  const paginationUrl = getPaginationUrl(state, datasetVersion);

  const exploreState = getExploreState(state);
  let explorePageProps = null;
  let queryStatuses;
  let waitingForJobResults;
  if (exploreState) {
    explorePageProps = {
      currentSql: exploreState.view.currentSql,
      queryContext: exploreState.view.queryContext,
      queryState: parseQueryState(location.query),
      isResizeInProgress: exploreState.ui.get("isResizeInProgress"),
      previousMultiSql: exploreState.view.previousMultiSql,
      queryStatuses: exploreState.view.queryStatuses,
      queryFilter: exploreState.view.queryFilter,
      isMultiQueryRunning: exploreState.view.isMultiQueryRunning,
      isExploreTableLoading: exploreState.view.isExploreTableLoading,
    };
    queryStatuses = exploreState.view.queryStatuses;
    waitingForJobResults = exploreState.view.waitingForJobResults;
  }

  const SqlList = queryStatuses && getSqlList(queryStatuses);
  const JobIdList = queryStatuses && getJobIdsList(queryStatuses);

  let tableData = ownProps.tableData;
  const previewVersion = location.state && location.state.previewVersion;
  const currentDataset =
    queryStatuses && queryStatuses[ownProps.queryTabNumber - 1];

  const curDatasetVersion =
    currentDataset && queryStatuses.length === 1
      ? location.query.version
      : currentDataset && queryStatuses.length > 1
        ? currentDataset.version
        : datasetVersion;

  if (!ownProps.isDumbTable) {
    if (
      ownProps.pageType === PageTypes.default ||
      !previewVersion ||
      ownProps.exploreViewState.get("isAutoPeekFailed")
    ) {
      tableData = getImmutableTable(state, curDatasetVersion);
    } else {
      tableData = getPeekData(state, previewVersion);
    }
  }

  const jobToWaitFor = queryStatuses?.find(
    (queryStatus) => queryStatus.jobId === waitingForJobResults,
  );

  let hideResults = false;

  // should only hide the results table when on the query tab of the currently running job
  if (jobToWaitFor && currentDataset?.jobId === jobToWaitFor.jobId) {
    hideResults = true;
  }

  return {
    tableData:
      !hideResults && tableData
        ? tableData
        : Immutable.fromJS({ rows: null, columns: [] }),
    columnFilter: getColumnFilter(state, curDatasetVersion),
    previewVersion,
    paginationUrl,
    location,
    jobIdList: JobIdList || [],
    sqlList: SqlList || [],
    exploreViewState: ownProps.exploreViewState,
    fullCellViewState: ownProps.isDumbTable
      ? ownProps.exploreViewState
      : getViewState(state, FULL_CELL_VIEW_ID),
    cardsViewState: ownProps.isDumbTable
      ? ownProps.exploreViewState
      : getViewState(state, LOAD_TRANSFORM_CARDS_VIEW_ID),
    ...explorePageProps,
  };
}

export default connect(mapStateToProps, {
  resetViewState,
  transformHistoryCheck,
  performTransform,
  cancelJob: cancelJobAndShowNotification,
  accessEntity,
})(ExploreTableController);
