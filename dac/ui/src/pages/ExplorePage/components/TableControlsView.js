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
import { compose } from "redux";
import { connect } from "react-redux";
import PropTypes from "prop-types";
import Immutable from "immutable";
import { injectIntl } from "react-intl";

import ExploreTableColumnFilter from "pages/ExplorePage/components/ExploreTable/ExploreTableColumnFilter";
import { Button, Tooltip } from "dremio-ui-lib/components";
import ExploreCopyTableButton from "@app/pages/ExplorePage/components/ExploreTable/ExploreCopyTableButton";
import modelUtils from "@app/utils/modelUtils";
import { isSqlChanged } from "@app/sagas/utils";
import { CombinedActionMenu } from "@app/components/Menus/ExplorePage/CombinedActionMenu";
import { navigateToExploreDefaultIfNecessary } from "@app/utils/pathUtils";
import DropdownMenu from "@app/components/Menus/DropdownMenu";
import { PHYSICAL_DATASET_TYPES } from "@app/constants/datasetTypes";
import { getSupportFlag } from "@app/exports/endpoints/SupportFlags/getSupportFlag";

import { memoOne } from "@app/utils/memoUtils";
import {
  columnFilterWrapper,
  searchField,
} from "@app/pages/ExplorePage/components/ExploreTable/ExploreTableColumnFilter.less";
import { SearchField } from "components/Fields";
import { formatMessage } from "@app/utils/locale";
import ExploreTableJobStatus from "../components/ExploreTable/ExploreTableJobStatus";
import { ALLOW_DOWNLOAD } from "@app/exports/endpoints/SupportFlags/supportFlagConstants";
import { getExploreState } from "@app/selectors/explore";
import { getJobSummaries } from "@app/selectors/exploreJobs";
import Message from "@app/components/Message";
import "./TableControls.less";

const HIDE_MESSAGE_KEY = "output-limited";

const datasetColumnsMemoize = memoOne((tableColumns) => {
  return (
    (tableColumns && tableColumns.map((column) => column.get("type")).toJS()) ||
    []
  );
});

export class TableControlsView extends PureComponent {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    exploreViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    tableColumns: PropTypes.instanceOf(Immutable.List).isRequired,

    groupBy: PropTypes.func.isRequired,
    addField: PropTypes.func,
    join: PropTypes.func.isRequired,
    rightTreeVisible: PropTypes.bool,
    approximate: PropTypes.bool,
    intl: PropTypes.object.isRequired,
    columnCount: PropTypes.number,
    disableButtons: PropTypes.bool,

    saveDataset: PropTypes.func,
    saveAsDataset: PropTypes.func,
    performNextAction: PropTypes.func,
    needsTransform: PropTypes.func,
    queryContext: PropTypes.func,
    performTransform: PropTypes.func,
    transformHistoryCheck: PropTypes.func,
    pageType: PropTypes.string,
    showConfirmationDialog: PropTypes.func,
    startDownloadDataset: PropTypes.func,
    currentSql: PropTypes.string,
    history: PropTypes.object,
    location: PropTypes.object,
    filteredColumnCount: PropTypes.number,
    version: PropTypes.any,
    jobsList: PropTypes.array,
    showJobsTable: PropTypes.bool,
    jobsCount: PropTypes.number,
    filterQueries: PropTypes.func,
    columnFilter: PropTypes.any,
    queryFilter: PropTypes.string,
    isQuerySuccess: PropTypes.bool,
    jobSummary: PropTypes.object,
  };

  constructor(props) {
    super(props);

    this.state = {
      tooltipState: false,
      anchorOrigin: {
        horizontal: "right",
        vertical: "bottom",
      },
      targetOrigin: {
        horizontal: "right",
        vertical: "top",
      },
      isDownloading: false,
    };
  }

  async componentDidMount() {
    try {
      const res = await getSupportFlag(ALLOW_DOWNLOAD);
      this.setState({
        allowDownload: res?.value,
      });
    } catch (e) {
      //
    }
  }

  renderCopyToClipboard = () => {
    const { dataset } = this.props;

    const version = dataset && dataset.get("datasetVersion");

    const isActionDisabled =
      dataset.get("isNewQuery") || !dataset.get("datasetType") || !version; // not new query nor loaded

    return (
      <ExploreCopyTableButton
        version={version}
        isActionDisabled={isActionDisabled}
      />
    );
  };

  isTransformNeeded() {
    const { dataset, queryContext, currentSql, needsTransform } = this.props;
    return needsTransform(dataset, queryContext, currentSql);
  }

  transformIfNecessary(callback, forceDataLoad) {
    const { dataset, currentSql, queryContext, exploreViewState } = this.props;

    const doPerformTransform = () => {
      return this.props.performTransform({
        dataset,
        currentSql,
        queryContext,
        viewId: exploreViewState.get("viewId"),
        callback,
        // forces preview to reload a data if nothing is changed. Primary use case is
        // when a user clicks a preview button
        forceDataLoad,
      });
    };

    if (this.isTransformNeeded()) {
      // need to navigate before history check
      this.navigateToExploreTableIfNecessary();
      this.props.transformHistoryCheck(dataset, doPerformTransform);
    } else {
      doPerformTransform();
    }
  }

  navigateToExploreTableIfNecessary() {
    const { pageType, location } = this.props;
    navigateToExploreDefaultIfNecessary(
      pageType,
      location,
      this.context.router,
    );
  }

  downloadDataset(format) {
    this.transformIfNecessary((didTransform, dataset) => {
      this.props.showConfirmationDialog({
        title: this.props.intl.formatMessage({ id: "Download.DownloadLimit" }),
        confirmText: this.props.intl.formatMessage({ id: "Download.Download" }),
        text: this.props.intl.formatMessage({
          id: "Download.DownloadLimitValue",
        }),
        doNotAskAgainKey: "isDownloadWarningDisabled",
        doNotAskAgainText: this.props.intl.formatMessage({
          id: "Download.DownloadLimitWarn",
        }),
        confirm: () => this.props.startDownloadDataset(dataset, format),
      });
    });
  }

  // Note: similar to but different from ExplorePageControllerComponent#shouldShowUnsavedChangesPopup
  isEditedDataset() {
    const { dataset, history, currentSql } = this.props;
    if (!dataset.get("datasetType")) {
      // not loaded yet
      return false;
    }

    if (PHYSICAL_DATASET_TYPES.has(dataset.get("datasetType"))) {
      return false;
    }

    // New Query can not show (edited)
    if (!modelUtils.isNamedDataset(dataset)) {
      return false;
    }

    if (isSqlChanged(dataset.get("sql"), currentSql)) {
      return true;
    }

    return history ? history.get("isEdited") : false;
  }

  isCreatedAndNamedDataset() {
    const { dataset } = this.props;
    return (
      dataset.get("datasetVersion") !== undefined &&
      modelUtils.isNamedDataset(dataset)
    );
  }

  isNewDataset() {
    const { mode } = this.props.location.query;
    return modelUtils.isNewDataset(this.props.dataset, mode);
  }

  updateDownloading = () => {
    this.setState({
      isDownloading: !this.state.isDownloading,
    });
  };

  // ellipsis button with settings, download, and analyze options
  renderSavedButton = () => {
    const { dataset, tableColumns } = this.props;
    const isActionDisabled =
      dataset.get("isNewQuery") || !dataset.get("datasetType"); // not new query nor loaded
    const datasetColumns = datasetColumnsMemoize(tableColumns);

    return (
      <DropdownMenu
        className="explore-ellipsis-button"
        iconType="Ellipsis"
        disabled={isActionDisabled || this.state.isDownloading}
        isDownloading={this.state.isDownloading}
        arrowStyle={{
          fontSize: "12px",
          marginLeft: "0",
          color: "#505862",
          paddingLeft: "2px",
        }}
        selectClass="select-view-wrapper"
        menu={
          <CombinedActionMenu
            dataset={dataset}
            datasetColumns={datasetColumns}
            downloadAction={this.downloadDataset}
            updateDownloading={this.updateDownloading}
          />
        }
      />
    );
  };

  getWording = () => {
    const { showJobsTable, jobsCount, columnCount, isQuerySuccess } =
      this.props;
    if (showJobsTable) {
      return jobsCount === 1
        ? "Explore.Middle.Counter.Job"
        : "Explore.Middle.Counter.Jobs";
    } else {
      return columnCount !== 1 || !isQuerySuccess
        ? "Explore.Middle.Counter.Columns"
        : "Explore.Middle.Counter.Column";
    }
  };

  renderOutputLimitedMessage() {
    const { intl, jobSummary } = this.props;

    if (!jobSummary?.outputLimited) {
      return;
    }

    return (
      <Message
        message={intl.formatMessage(
          {
            id: "Explore.Run.NewWarning",
          },
          {
            rows: jobSummary.outputRecords.toLocaleString(),
          },
        )}
        messageType="warning"
        isDismissable
        allowHideForever
        localStorageKey={HIDE_MESSAGE_KEY}
      />
    );
  }

  render() {
    const {
      dataset,
      addField,
      groupBy,
      join,
      columnCount,
      intl,
      disableButtons,
      columnFilter,
      queryFilter,
      filteredColumnCount,
      approximate,
      version,
      showJobsTable,
      jobsCount,
      filterQueries,
      isQuerySuccess,
    } = this.props;

    return (
      <div className="table-controls-wrapper">
        <div className="table-controls">
          <div className="left-controls">
            <div className="controls" style={styles.controlsInner}>
              {showJobsTable && (
                <div className={columnFilterWrapper} data-qa="columnFilter">
                  <SearchField
                    disabled={disableButtons}
                    value={queryFilter}
                    onChange={(value) => filterQueries(value)}
                    className={searchField}
                    placeholder={intl.formatMessage({
                      id: "Explore.SearchFilterJobId",
                    })}
                    dataQa="explore-column-filter"
                  />
                </div>
              )}
              {!showJobsTable && (
                <>
                  <Button
                    className="controls-addField"
                    variant="secondary"
                    onClick={addField}
                    disabled={disableButtons}
                  >
                    <Tooltip content="Add Column">
                      <dremio-icon
                        name="sql-editor/add-field"
                        alt="Add Column"
                        class={
                          disableButtons
                            ? "controls-addField-icon--disabled"
                            : "controls-addField-icon"
                        }
                      />
                    </Tooltip>
                    <span className="noText">
                      {intl.formatMessage({ id: "Dataset.AddColumn" })}
                    </span>
                  </Button>
                  <Button
                    className="controls-groupBy"
                    variant="secondary"
                    onClick={groupBy}
                    disabled={disableButtons}
                  >
                    <Tooltip content="Group By">
                      <dremio-icon
                        name="sql-editor/group-by"
                        alt="Group By"
                        class={
                          disableButtons
                            ? "controls-groupBy-icon--disabled"
                            : "controls-groupBy-icon"
                        }
                      />
                    </Tooltip>
                    <span className="noText">
                      {intl.formatMessage({ id: "Dataset.GroupBy" })}
                    </span>
                  </Button>
                  <Button
                    className="controls-join"
                    variant="secondary"
                    onClick={join}
                    disabled={disableButtons}
                  >
                    <Tooltip content="Join">
                      <dremio-icon
                        name="sql-editor/join"
                        alt="Join"
                        class={
                          disableButtons
                            ? "controls-join-icon--disabled"
                            : "controls-join-icon"
                        }
                      />
                    </Tooltip>
                    <span className="noText">
                      {intl.formatMessage({ id: "Dataset.Join" })}
                    </span>
                  </Button>
                  <ExploreTableColumnFilter
                    dataset={dataset}
                    disabled={!isQuerySuccess}
                  />
                </>
              )}
              <div className="table-controls__actions">
                <div data-qa="columnFilterStats">
                  {columnFilter && (
                    <span data-qa="columnFilterCount">
                      {isQuerySuccess ? filteredColumnCount : 0} of{" "}
                    </span>
                  )}
                  {showJobsTable ? jobsCount : isQuerySuccess ? columnCount : 0}{" "}
                  {formatMessage(this.getWording())}
                </div>
              </div>
            </div>
            {!showJobsTable && isQuerySuccess && (
              <div className="table-controls__right" style={styles.right}>
                <ExploreTableJobStatus
                  approximate={approximate}
                  version={version}
                />
                {this.state.allowDownload ? this.renderSavedButton() : ""}
                {this.state.allowDownload ? this.renderCopyToClipboard() : ""}
              </div>
            )}
          </div>
        </div>
        {this.renderOutputLimitedMessage()}
      </div>
    );
  }
}

const styles = {
  controlsInner: {
    height: 24,
  },
  right: {
    display: "flex",
    flex: 1,
    justifyContent: "flex-end",
    alignItems: "center",
    gap: "8px",
  },
};

function mapStateToProps(state) {
  const explorePageState = getExploreState(state);
  const queryStatuses = explorePageState.view.queryStatuses;
  const queryTabNumber = explorePageState.view.queryTabNumber;
  const jobId = queryStatuses[queryTabNumber - 1]?.jobId;
  const jobSummaries = getJobSummaries(state);

  return {
    jobSummary: jobSummaries[jobId],
  };
}

export default compose(connect(mapStateToProps), injectIntl)(TableControlsView);
