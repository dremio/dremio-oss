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
import { PureComponent, Fragment } from "react";
import { compose } from "redux";
import { connect } from "react-redux";
import PropTypes from "prop-types";
import Immutable from "immutable";
import DocumentTitle from "react-document-title";
import { injectIntl } from "react-intl";
import { withRouter } from "react-router";
import { Tooltip } from "dremio-ui-lib";

import CopyButton from "#oss/components/Buttons/CopyButton";

import DropdownMenu from "#oss/components/Menus/DropdownMenu";
import EllipsedText from "components/EllipsedText";
import modelUtils from "utils/modelUtils";
import {
  constructFullPath,
  navigateToExploreDefaultIfNecessary,
} from "utils/pathUtils";
import { formatMessage } from "utils/locale";
import { needsTransform, isSqlChanged } from "sagas/utils";

import { PHYSICAL_DATASET_TYPES } from "#oss/constants/datasetTypes";
import explorePageInfoHeaderConfig from "@inject/pages/ExplorePage/components/explorePageInfoHeaderConfig";
import SQLScriptDeletedDialog from "#oss/components/SQLScripts/components/SQLScriptDeletedDialog/SQLScriptDeletedDialog";
//actions
import { saveDataset, saveAsDataset } from "actions/explore/dataset/save";
import {
  performTransform,
  transformHistoryCheck,
} from "actions/explore/dataset/transform";
import {
  performTransformAndRun,
  runDatasetSql,
  previewDatasetSql,
} from "actions/explore/dataset/run";
import { showConfirmationDialog } from "actions/confirmation";
import { PageTypes, pageTypesProp } from "#oss/pages/ExplorePage/pageTypes";
import { withDatasetChanges } from "#oss/pages/ExplorePage/DatasetChanges";

import { startDownloadDataset } from "actions/explore/download";
import { performNextAction, NEXT_ACTIONS } from "actions/explore/nextAction";

import ExploreHeaderMixin from "#oss/pages/ExplorePage/components/ExploreHeaderMixin";
import config from "dyn-load/utils/config";
import { getAnalyzeToolsConfig } from "#oss/utils/config";
import exploreUtils from "#oss/utils/explore/exploreUtils";
import { VIEW_ID as SCRIPTS_VIEW_ID } from "#oss/components/SQLScripts/SQLScripts";
import {
  closeTab,
  newTab,
} from "dremio-ui-common/sonar/SqlRunnerSession/resources/SqlRunnerSessionResource.js";
import SaveMenu, {
  DOWNLOAD_TYPES,
} from "components/Menus/ExplorePage/SaveMenu";
import BreadCrumbs, { formatFullPath } from "components/BreadCrumbs";
import DatasetItemLabel from "components/Dataset/DatasetItemLabel";
import { getIconPath } from "#oss/utils/getIconPath";
import { Button } from "dremio-ui-lib/components";
import { showQuerySpinner } from "@inject/pages/ExplorePage/utils";
import { getIconDataTypeFromDatasetType } from "utils/iconUtils";
import { NoticeTag } from "dremio-ui-common/components/NoticeTag.js";
import {
  getHistory,
  getTableColumns,
  getJobProgress,
  getRunStatus,
  getExploreState,
  isWikAvailable,
} from "selectors/explore";
import { getScriptsSyncPending } from "#oss/selectors/scriptsSyncPending";
import { getExploreJobId } from "#oss/selectors/exploreJobs";
import {
  getActiveScript,
  getActiveScriptPermissions,
  getNumberOfMineScripts,
} from "selectors/scripts";
import { HANDLE_THROUGH_API } from "@inject/pages/HomePage/components/HeaderButtonConstants";
import { cancelJobAndShowNotification } from "#oss/actions/jobs/jobs";
import SQLScriptDialog from "#oss/components/SQLScripts/components/SQLScriptDialog/SQLScriptDialog";
import {
  setQueryStatuses,
  setActionState,
  resetQueryState,
  resetTableState,
} from "actions/explore/view";
import {
  createScript,
  fetchScripts,
  updateScript,
  setActiveScript,
} from "actions/resources/scripts";
import {
  fetchAllAndMineScripts,
  handleOpenTabScript,
  MAX_MINE_SCRIPTS_ALLOWANCE,
  openPrivilegesModalForScript,
} from "#oss/components/SQLScripts/sqlScriptsUtils";
import {
  DisabledEngineActions,
  ExploreHeaderActions,
} from "#oss/pages/ExplorePage/components/ExploreHeaderUtils";
import { addNotification } from "#oss/actions/notification";
import { ExploreActions } from "./ExploreActions";
import ExploreTableJobStatusSpinner from "./ExploreTable/ExploreTableJobStatusSpinner";
import * as sqlPaths from "dremio-ui-common/paths/sqlEditor.js";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";
import { getVersionContextFromId } from "dremio-ui-common/utilities/datasetReference.js";
import { hideForNonDefaultBranch } from "dremio-ui-common/utilities/versionContext.js";
import { withIsMultiTabEnabled } from "#oss/components/SQLScripts/useMultiTabIsEnabled";
import { isTabbableUrl } from "#oss/utils/explorePageTypeUtils";
import sentryUtil from "#oss/utils/sentryUtil";
import { newPopulatedTab } from "dremio-ui-common/sonar/SqlRunnerSession/resources/SqlRunnerSessionResource.js";
import { ScriptsResource } from "dremio-ui-common/sonar/scripts/resources/ScriptsResource.js";

import * as classes from "./ExploreHeader.module.less";
import "./ExploreHeader.less";

export const TABLEAU_TOOL_NAME = "Tableau";
export const QLIK_TOOL_NAME = "Qlik Sense";

@ExploreHeaderMixin
export class ExploreHeader extends PureComponent {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    datasetSql: PropTypes.string,
    pageType: pageTypesProp,
    toggleRightTree: PropTypes.func.isRequired,
    grid: PropTypes.object,
    space: PropTypes.object,
    rightTreeVisible: PropTypes.bool,
    location: PropTypes.object,
    exploreViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    intl: PropTypes.object.isRequired,
    approximate: PropTypes.bool,
    sqlState: PropTypes.bool,
    keyboardShortcuts: PropTypes.object,
    disableButtons: PropTypes.bool,
    router: PropTypes.object,
    getSelectedSql: PropTypes.func,
    statusesArray: PropTypes.array,
    resetSqlTabs: PropTypes.func,
    toggleSqlPaneDisplay: PropTypes.func,

    // connected
    history: PropTypes.instanceOf(Immutable.Map),
    queryContext: PropTypes.instanceOf(Immutable.List),
    getCurrentSQl: PropTypes.func,
    tableColumns: PropTypes.instanceOf(Immutable.List),
    jobProgress: PropTypes.object,
    runStatus: PropTypes.bool,
    jobId: PropTypes.string,
    showWiki: PropTypes.bool,
    activeScript: PropTypes.object,
    queryStatuses: PropTypes.array,
    isMultiQueryRunning: PropTypes.bool,
    actionState: PropTypes.string,

    // actions
    transformHistoryCheck: PropTypes.func.isRequired,
    performNextAction: PropTypes.func.isRequired,
    performTransform: PropTypes.func.isRequired,
    performTransformAndRun: PropTypes.func.isRequired,
    runDatasetSql: PropTypes.func.isRequired,
    previewDatasetSql: PropTypes.func.isRequired,
    saveDataset: PropTypes.func.isRequired,
    saveAsDataset: PropTypes.func.isRequired,
    startDownloadDataset: PropTypes.func.isRequired,
    showConfirmationDialog: PropTypes.func,
    cancelJob: PropTypes.func,
    setQueryStatuses: PropTypes.func,
    createScript: PropTypes.func,
    fetchScripts: PropTypes.func,
    updateScript: PropTypes.func,
    setActiveScript: PropTypes.func,
    setActionState: PropTypes.func,
    resetQueryState: PropTypes.func,
    supportFlagsObj: PropTypes.object,
    activeScriptPermissions: PropTypes.array,
    user: PropTypes.instanceOf(Immutable.Map),
    numberOfMineScripts: PropTypes.number,
    addNotification: PropTypes.func,
    resetTableState: PropTypes.func,
  };

  componentDidUpdate(prevProps) {
    const { supportFlagsObj } = this.props;
    if (
      supportFlagsObj &&
      JSON.stringify(supportFlagsObj) !==
        JSON.stringify(prevProps.supportFlagsObj)
    ) {
      this.setState({ supportFlags: supportFlagsObj });
    }
  }

  static contextTypes = {
    router: PropTypes.object.isRequired,
  };

  static getFullPathListForDisplay(dataset) {
    if (!dataset) {
      return;
    }
    const fullPath = dataset.get("displayFullPath");
    return modelUtils.isNamedDataset(dataset) ? fullPath : undefined;
  }

  static getNameForDisplay(dataset) {
    const defaultName = formatMessage("NewQuery.NewQuery");
    if (!dataset) {
      return defaultName;
    }
    const displayFullPath = dataset.get("displayFullPath");
    return modelUtils.isNamedDataset(dataset) && displayFullPath
      ? displayFullPath.get(-1)
      : defaultName;
  }

  closeAndCreateScript = async (id) => {
    if (ScriptsResource.getResource().value.length < 2) {
      await newTab();
    }
    closeTab(id);
  };

  constructor(props) {
    super(props);

    this.doButtonAction = this.doButtonAction.bind(this);
    this.downloadDataset = this.downloadDataset.bind(this);

    this.state = {
      isSaveAsModalOpen: false,
      saveAsDialogError: null,
      isSQLScriptDeletedDialogOpen: false,
      SQLScriptDeletedDialogProps: {
        onCancel: async () => {
          await this.closeAndCreateScript(this.props.activeScript.id);
          await ScriptsResource.fetch();
          fetchAllAndMineScripts(this.props.fetchScripts, null);
          handleOpenTabScript(this.props.router)(
            ScriptsResource.getResource().value[0],
          );
          this.setState({
            isSQLScriptDeletedDialogOpen: false,
          });
        },
        onConfirm: () => {
          this.setState({
            isSaveAsModalOpen: true,
            isSQLScriptDeletedDialogOpen: false,
          });
          this.closeAndCreateScript(this.props.activeScript.id);
        },
      },
      supportFlags: {},
      nextAction: null,
    };

    window.sqlUtils = {
      handleDeletedScript: this.handleDeletedScript,
      handleSaveScriptAs: this.handleSaveScriptAs,
      handleSaveViewAs: this.handleSaveViewAs,
    };
  }

  doButtonAction(actionType) {
    const { cancelJob, jobId, resetSqlTabs, setActionState, setQueryStatuses } =
      this.props;

    switch (actionType) {
      case ExploreHeaderActions.RUN:
        setActionState({ actionState: ExploreHeaderActions.RUN });
        resetSqlTabs();
        return this.handleRunClick();
      case ExploreHeaderActions.PREVIEW:
        setActionState({ actionState: ExploreHeaderActions.PREVIEW });
        resetSqlTabs();
        return this.handlePreviewClick();
      case ExploreHeaderActions.DISCARD:
        return this.handleDiscardConfirm();
      case ExploreHeaderActions.SAVE:
        return this.handleSaveView();
      case ExploreHeaderActions.SAVE_AS:
        return this.handleSaveViewAs();
      case ExploreHeaderActions.SAVE_SCRIPT:
        return this.handleSaveScript();
      case ExploreHeaderActions.SAVE_SCRIPT_AS:
        return this.handleSaveScriptAs();
      case ExploreHeaderActions.CANCEL:
        setQueryStatuses(this.handleCancelAllJobs());
        return cancelJob(jobId);
      case DOWNLOAD_TYPES.json:
      case DOWNLOAD_TYPES.csv:
      case DOWNLOAD_TYPES.parquet:
        return this.downloadDataset(actionType);
      default:
        break;
    }
  }

  handleDeletedScript = () => {
    this.setState({
      isSQLScriptDeletedDialogOpen: true,
    });
  };

  handleRunClick() {
    const { getSelectedSql, runDatasetSql } = this.props;
    this.navigateToExploreTableIfNecessary();
    runDatasetSql({ selectedSql: getSelectedSql() });
  }

  handlePreviewClick() {
    const { getSelectedSql, previewDatasetSql } = this.props;
    this.navigateToExploreTableIfNecessary();
    previewDatasetSql({ selectedSql: getSelectedSql() });
  }

  handleCancelAllJobs() {
    const { queryStatuses } = this.props;

    const updatedStatuses = [];
    for (const status of queryStatuses) {
      updatedStatuses.push({
        ...status,
        cancelled: status.jobId ? status.cancelled : true,
      });
    }

    return { statuses: updatedStatuses };
  }

  //TODO: DX-14762 - refactor to use runDatasetSql and performTransform saga;
  // investigate replacing pathutils.navigateToExploreTableIfNecessary with pageTypeUtils methods

  isTransformNeeded() {
    const { dataset, queryContext, getCurrentSql } = this.props;
    return needsTransform(dataset, queryContext, getCurrentSql());
  }

  transformIfNecessary(callback, forceDataLoad, isSaveViewAs) {
    const { dataset, getCurrentSql, queryContext, exploreViewState } =
      this.props;

    const doPerformTransform = () => {
      return this.props.performTransform({
        dataset,
        currentSql: getCurrentSql(),
        queryContext,
        viewId: exploreViewState.get("viewId"),
        callback,
        // forces preview to reload a data if nothing is changed. Primary use case is
        // when a user clicks a preview button
        forceDataLoad,
        isSaveViewAs,
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

  showErrorMsgAsModal = (errorTitle, errorMsg, retryCallback) => {
    this.setState({
      showErrorMsgAsModal: true,
      errorTitle,
      errorMsg,
      retryCallback,
    });
  };

  hideErrorMsgAsModal = () => {
    this.setState({ showErrorMsgAsModal: false });
  };

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

  isNewDataset() {
    const { mode } = this.props.location.query;
    return modelUtils.isNewDataset(this.props.dataset, mode);
  }

  // Note: similar to but different from ExplorePageControllerComponent#shouldShowUnsavedChangesPopup
  isEditedDataset() {
    const { dataset, history, getCurrentSql } = this.props;
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

    if (isSqlChanged(dataset.get("sql"), getCurrentSql())) {
      return true;
    }

    return history ? history.get("isEdited") : false;
  }

  handleSaveView = () => {
    const nextAction = this.state.nextAction;
    this.setState({ nextAction: undefined });
    this.transformIfNecessary(
      (didTransform, dataset) => {
        // transformIfNecessary does a transformHistoryCheck if a transform is necessary.
        // if not, here we need to another transformHistoryCheck because save will lose the future history.
        // No need to worry about doing it twice because if transformIfNecessary does a transform, the next
        // transformHistoryCheck will never trigger.
        return this.props.transformHistoryCheck(dataset, () => {
          return this.props.saveDataset(
            dataset,
            this.props.exploreViewState.get("viewId"),
            nextAction,
          );
        });
      },
      undefined,
      true,
    );
  };

  handleSaveViewAs = () => {
    const { setActionState } = this.props;
    setActionState({ actionState: ExploreHeaderActions.SAVE_AS });

    const nextAction = this.state.nextAction;
    this.setState({ nextAction: undefined });
    this.transformIfNecessary(
      () => this.props.saveAsDataset(nextAction),
      undefined,
      true,
    );
  };

  handleSaveScript = () => {
    const { activeScript, getCurrentSql, queryContext, intl } = this.props;
    if (!activeScript.id) {
      this.handleSaveScriptAs();
    } else {
      const payload = {
        name: activeScript.name,
        content: getCurrentSql(),
        context: queryContext.toJS(),
        description: "",
      };
      return this.props.updateScript(payload, activeScript.id).then((res) => {
        if (!res.error) {
          this.props.setActiveScript({ script: res.payload });
          this.props.addNotification(
            intl.formatMessage({ id: "NewQuery.ScriptSaved" }),
            "success",
          );
          fetchAllAndMineScripts(this.props.fetchScripts, null);
        }
        return null;
      });
    }
  };

  handleSaveScriptAs = () => {
    this.setState({ isSaveAsModalOpen: true });
  };

  handleDiscard = () => {
    this.props.resetQueryState();
    this.props.setActiveScript({ script: {} });
    const projectId = getSonarContext()?.getSelectedProjectId?.();
    this.props.router.push({
      pathname: sqlPaths.sqlEditor.link({ projectId }),
      state: { discard: true },
    });
  };

  handleDiscardConfirm = () => {
    const { intl } = this.props;
    this.props.showConfirmationDialog({
      title: intl.formatMessage({ id: "Script.DiscardConfirm" }),
      confirmText: intl.formatMessage({ id: "Common.Discard" }),
      text: intl.formatMessage({ id: "Script.DiscardConfirmMessage" }),
      confirm: () => this.handleDiscard(),
      className: "discardConfirmDialog --newModalStyles",
      headerIcon: (
        <dremio-icon
          name="interface/warning"
          alt="Warning"
          class={classes["warning-icon"]}
        />
      ),
    });
  };

  handleShowBI = (nextAction) => {
    const { dataset } = this.props;
    if (!modelUtils.isNamedDataset(dataset)) {
      this.transformIfNecessary(() => this.props.saveAsDataset(nextAction));
    } else {
      this.props.performNextAction(this.props.dataset, nextAction);
    }
  };

  isCreatedAndNamedDataset() {
    const { dataset } = this.props;
    return (
      dataset.get("datasetVersion") !== undefined &&
      modelUtils.isNamedDataset(dataset)
    );
  }

  // unlike acceleration button, settings button is always shown, but it is disabled when
  // show acceleration button is hidden or disabled.
  shouldEnableSettingsButton() {
    return this.isCreatedAndNamedDataset() && !this.isEditedDataset();
  }

  renderCopyToClipBoard(fullPath) {
    return fullPath ? (
      <CopyButton
        text={fullPath}
        title={this.props.intl.formatMessage({ id: "Path.Copy" })}
        style={{ transform: "translateY(1px)" }}
      />
    ) : null;
  }

  renderDatasetLabel(dataset) {
    const nameForDisplay = ExploreHeader.getNameForDisplay(dataset);
    const isEditedDataset = this.isEditedDataset();
    const nameStyle = isEditedDataset ? { fontStyle: "italic" } : {};
    const fullPath = ExploreHeader.getFullPathListForDisplay(dataset);
    const edited = this.props.intl.formatMessage({ id: "Dataset.Edited" });
    return (
      <DatasetItemLabel
        customNode={
          // todo: string replace loc
          <div className="flexbox-truncate-text-fix">
            <div style={{ ...style.dbName }} data-qa={nameForDisplay}>
              <EllipsedText
                style={nameStyle}
                text={`${nameForDisplay}${isEditedDataset ? edited : ""}`}
                className="heading"
              >
                <span>{nameForDisplay}</span>
                <span data-qa="dataset-edited">
                  {isEditedDataset ? edited : ""}
                </span>
              </EllipsedText>
              {this.renderCopyToClipBoard(constructFullPath(fullPath))}
            </div>
            {fullPath && (
              <BreadCrumbs
                hideLastItem
                fullPath={fullPath}
                pathname={this.props.location.pathname}
              />
            )}
            {
              <DocumentTitle
                title={
                  fullPath
                    ? formatFullPath(fullPath).join(".") +
                      (isEditedDataset ? "*" : "")
                    : nameForDisplay
                }
              />
            }
          </div>
        }
        isNewQuery={dataset.get("isNewQuery")}
        showFullPath
        fullPath={fullPath}
        placement="right"
        typeIcon={getIconDataTypeFromDatasetType(dataset.get("datasetType"))}
        shouldShowOverlay={false}
      />
    );
  }

  wrapWithTooltip(button, title, cmd, disabled, disabledTooltip) {
    const tooltip = cmd ? (
      <div
        className="exploreHeaderLeft__tooltip-content"
        style={{ height: 28 }}
      >
        {title}
        <span>{cmd}</span>
      </div>
    ) : (
      title
    );

    // https://stackoverflow.com/questions/57527896/material-ui-tooltip-doesnt-display-on-custom-component-despite-spreading-props
    return (
      <Tooltip title={disabledTooltip ?? tooltip} placement="top">
        <span
          {...(disabled &&
            !disabledTooltip && { style: { pointerEvents: "none" } })}
        >
          {button}
        </span>
      </Tooltip>
    );
  }

  renderHeader() {
    const {
      actionState,
      statusesArray,
      isMultiQueryRunning,
      jobId,
      disableButtons,
      intl,
    } = this.props;

    // Fix later: jobProgress watchers are inconsistent since multi queries now run sequentially
    // const isJobCancellable = this.props.jobProgress ? this.getCancellable(this.props.jobProgress.status) : null;
    const { disablePreviewButton, disableRunButton } =
      explorePageInfoHeaderConfig;
    const cancellableJobs =
      statusesArray &&
      statusesArray.length &&
      statusesArray.filter((status) => exploreUtils.getCancellable(status));
    const isCancellable = !!cancellableJobs.length || isMultiQueryRunning;
    const disableEnginePickMenu =
      isCancellable && DisabledEngineActions.includes(actionState);
    const cancelText = intl.formatMessage({ id: "Common.Cancel" });
    const runText = intl.formatMessage({ id: "Common.Run" });
    const previewText = intl.formatMessage({ id: "Common.Preview" });
    const discardText = intl.formatMessage({ id: "Common.Discard" });

    const disabledTooltip = disableRunButton
      ? intl.formatMessage({ id: "Explore.RunPreview.Disabled.Support" })
      : disableButtons
        ? intl.formatMessage({ id: "Explore.RunPreview.Disabled.Empty" })
        : undefined;

    return (
      <>
        <div className="ExploreHeader__left">
          {isCancellable && jobId && actionState === ExploreHeaderActions.RUN
            ? this.wrapWithTooltip(
                <Button
                  variant="secondary"
                  data-qa="qa-cancel"
                  onClick={() =>
                    this.doButtonAction(ExploreHeaderActions.CANCEL)
                  }
                >
                  {cancelText}
                </Button>,
                cancelText,
              )
            : this.wrapWithTooltip(
                <Button
                  variant="primary"
                  className="run-btn"
                  data-qa="qa-run"
                  onClick={() => this.doButtonAction(ExploreHeaderActions.RUN)}
                  disabled={disableRunButton || disableButtons}
                >
                  <dremio-icon
                    name="sql-editor/run"
                    alt={runText}
                    class={
                      disableRunButton || disableButtons
                        ? classes["run-icon--disabled"]
                        : classes["run-icon"]
                    }
                  />
                  {runText}
                </Button>,
                runText,
                this.props.keyboardShortcuts.run,
                disableRunButton || disableButtons,
                disabledTooltip,
              )}
          {isCancellable &&
          jobId &&
          actionState === ExploreHeaderActions.PREVIEW
            ? this.wrapWithTooltip(
                <Button
                  variant="secondary"
                  data-qa="qa-cancel"
                  onClick={() =>
                    this.doButtonAction(ExploreHeaderActions.CANCEL)
                  }
                >
                  {cancelText}
                </Button>,
                cancelText,
              )
            : this.wrapWithTooltip(
                <Button
                  variant="secondary"
                  className="preview-btn"
                  data-qa="qa-preview"
                  style={{ minWidth: "auto" }}
                  onClick={() =>
                    this.doButtonAction(ExploreHeaderActions.PREVIEW)
                  }
                  disabled={disablePreviewButton || disableButtons}
                >
                  <dremio-icon
                    name="sql-editor/preview"
                    alt={previewText}
                    class={
                      disablePreviewButton || disableButtons
                        ? classes["preview-icon--disabled"]
                        : classes["preview-icon"]
                    }
                  />
                  <span className="noText">{previewText}</span>
                </Button>,
                previewText,
                this.props.keyboardShortcuts.preview,
                disablePreviewButton || disableButtons,
                disabledTooltip,
              )}

          {!this.props.isMultiTabEnabled &&
            exploreUtils.isSqlEditorTab(this.props.location) &&
            this.wrapWithTooltip(
              <Button
                className="discard-btn"
                variant="secondary"
                data-qa="qa-discard"
                onClick={() =>
                  this.doButtonAction(ExploreHeaderActions.DISCARD)
                }
                style={{ minWidth: "auto" }}
                disabled={isCancellable || disableButtons}
              >
                <dremio-icon
                  name="interface/discard"
                  alt={discardText}
                  class={
                    isCancellable || disableButtons
                      ? classes["discard-icon--disabled"]
                      : classes["discard-icon"]
                  }
                />
                <span className="noText">{discardText}</span>
              </Button>,
              discardText,
              undefined,
              isCancellable || disableButtons,
            )}

          <ExploreActions
            dataset={this.props.dataset}
            pageType={this.props.pageType}
            exploreViewState={this.props.exploreViewState}
            disableEnginePickMenu={!!disableEnginePickMenu}
          />
          {showQuerySpinner() && !this.props.isMultiTabEnabled && (
            <ExploreTableJobStatusSpinner
              jobProgress={this.props.jobProgress}
              jobId={this.props.jobId}
              action={actionState}
              message="Running"
            />
          )}
        </div>
        <div className="ExploreHeader__right">
          {this.props.isMultiTabEnabled &&
            exploreUtils.isSqlEditorTab(this.props.location) && (
              <NoticeTag
                inProgressMessage={
                  <>
                    <div
                      style={{
                        display: "inline-flex",
                        blockSize: "20px",
                        inlineSize: "20px",
                        flexShrink: "0",
                        verticalAlign: "middle",
                      }}
                    ></div>
                    Saving...
                  </>
                }
                completedMessage={
                  <>
                    <dremio-icon
                      name="interface/check"
                      alt=""
                      style={{
                        marginTop: "-2px",
                        color: "var(--icon--primary)",
                      }}
                    ></dremio-icon>{" "}
                    Saved!
                  </>
                }
                inProgress={this.props.scriptsSyncPending.has(
                  this.props.activeScript?.id,
                )}
                hideDelay={2000}
              />
            )}
          {this.renderShowHideSQLPane()}
          {this.renderPrivilegesIconButton()}
          {this.renderAnalyzeButtons()}
          {this.renderSaveButton()}
        </div>
      </>
    );
  }

  openTableau = () => {
    this.handleShowBI(NEXT_ACTIONS.openTableau);
  };
  openPowerBi = () => {
    this.handleShowBI(NEXT_ACTIONS.openPowerBI);
  };

  renderAnalyzeButton = (name, icon, onclick, iconSize, className) => {
    const { dataset } = this.props;
    return this.wrapWithTooltip(
      <Button
        variant="secondary"
        onClick={onclick}
        className={className}
        disabled={this.getExtraSaveDisable(dataset)}
        aria-label={icon}
        style={{ minWidth: "auto" }}
      >
        {icon === "corporate/tableau" ? (
          <dremio-icon
            name={icon}
            alt={name}
            style={{ height: iconSize, width: iconSize }}
            data-qa={icon}
          />
        ) : (
          <img
            src={getIconPath(icon)}
            data-qa={icon}
            alt={name}
            style={{ height: iconSize, width: iconSize }}
          />
        )}
      </Button>,
      name,
    );
  };

  renderPrivilegesIconButton = () => {
    const {
      activeScriptPermissions,
      activeScript,
      location,
      router,
      getCurrentSql,
      intl: { formatMessage },
    } = this.props;

    const hasPermission =
      activeScriptPermissions &&
      activeScriptPermissions.includes("MANAGE_GRANTS");

    const currentSql = getCurrentSql();

    const allowsPrivModal =
      hasPermission &&
      !exploreUtils.isEditedScript(activeScript, getCurrentSql());

    let tooltipWording;
    if (allowsPrivModal) {
      tooltipWording = "Privileges.Icon.Tooltip.Script";
    } else if (
      !allowsPrivModal &&
      exploreUtils.isEditedScript(activeScript, currentSql)
    ) {
      tooltipWording = "Privileges.Icon.Disabled.Tooltip.Script.Unsaved";
    }

    return (
      hasPermission && (
        <Tooltip
          title={formatMessage({ id: tooltipWording })}
          placement="top"
          enterDelay={500}
          enterNextDelay={500}
        >
          <dremio-icon
            name="interface/privilege"
            alt="Open privileges window"
            onClick={
              allowsPrivModal
                ? () => {
                    openPrivilegesModalForScript({
                      router,
                      location,
                      script: activeScript,
                      VIEW_ID: SCRIPTS_VIEW_ID,
                    });
                  }
                : null
            }
            class={
              allowsPrivModal
                ? classes["privilegesIcon"]
                : classes["privilegesIcon--disabled"]
            }
          ></dremio-icon>
        </Tooltip>
      )
    );
  };

  renderAnalyzeButtons = () => {
    const { dataset, showWiki } = this.props;
    const { supportFlags } = this.state;

    const versionContext = getVersionContextFromId(dataset.get("entityId"));

    if (!showWiki || !hideForNonDefaultBranch(versionContext)) return;

    const analyzeToolsConfig = getAnalyzeToolsConfig(config);
    let showTableau = analyzeToolsConfig.tableau.enabled;
    let showPowerBI = analyzeToolsConfig.powerbi.enabled;
    if (HANDLE_THROUGH_API) {
      let supportFlag = localStorage.getItem("supportFlags")
        ? JSON.parse(localStorage.getItem("supportFlags"))
        : null;
      if (Object.keys(supportFlags).length > 0) {
        supportFlag = { ...supportFlags };
      }
      showTableau = supportFlag && supportFlag["client.tools.tableau"];
      showPowerBI = supportFlag && supportFlag["client.tools.powerbi"];
    }

    if (!showTableau && !showPowerBI) return null;

    return (
      <Fragment>
        {showTableau &&
          this.renderAnalyzeButton(
            laDeprecated("Tableau"),
            "corporate/tableau",
            this.openTableau,
            24,
            "-noImgHover -noMinWidth",
          )}
        {showPowerBI &&
          this.renderAnalyzeButton(
            laDeprecated("Power BI"),
            "corporate/power-bi",
            this.openPowerBi,
            24,
            "-noImgHover -noMinWidth",
          )}
      </Fragment>
    );
  };

  renderShowHideSQLPane = () => {
    const { intl, sqlState, toggleSqlPaneDisplay } = this.props;
    const message = intl.formatMessage({
      id: `SQL.SQLEditor.${sqlState ? "Hide" : "Show"}SQLPane`,
    });
    return (
      <Tooltip
        title={message}
        placement="top"
        enterDelay={500}
        enterNextDelay={500}
      >
        <div
          data-qa="show-hide-sql-btn"
          className="show-hide-sql-btn"
          onClick={toggleSqlPaneDisplay}
        >
          <dremio-icon
            name={`sql-editor/panel-${sqlState ? "hide" : "show"}`}
            alt="+"
            class="show-hide-sql-btn__icon"
          />
          <span className="noText">{message}</span>
        </div>
      </Tooltip>
    );
  };

  getTabSaveButton = () => {
    const { intl } = this.props;
    return {
      qaLabel: "save-view-btn",
      text: intl.formatMessage({ id: "NewQuery.SaveAsViewBtn" }),
      onClick: this.handleSaveViewAs,
    };
  };

  getDefaultSaveButton = () => {
    const {
      location,
      activeScript,
      numberOfMineScripts,
      intl,
      dataset,
      isMultiTabEnabled,
    } = this.props;
    const isUntitledScript = !activeScript.id;
    const isSqlEditorTab = exploreUtils.isSqlEditorTab(location);
    const canAlter = dataset.getIn(["permissions", "canAlter"]);
    const canSelect = dataset.getIn(["permissions", "canSelect"]);
    const canAddMoreScripts = numberOfMineScripts < MAX_MINE_SCRIPTS_ALLOWANCE;
    const canModify = activeScript?.permissions
      ? exploreUtils.hasPermissionToModify(activeScript)
      : !!activeScript.id; // DX-55721: should be able to update if user owns the script (CE edition)

    if (
      (isMultiTabEnabled && isTabbableUrl(location)) ||
      (!canAlter && canSelect)
    ) {
      return {
        qaLabel: "save-view-btn",
        text: intl.formatMessage({ id: "NewQuery.SaveViewAsBtn" }),
        onClick: this.handleSaveViewAs,
      };
    } else if (
      isSqlEditorTab &&
      !isMultiTabEnabled &&
      (!isUntitledScript || canAddMoreScripts)
    ) {
      return {
        text: intl.formatMessage({
          id: canModify ? "NewQuery.SaveScript" : "NewQuery.SaveScriptAsBtn",
        }),
        onClick: canModify ? this.handleSaveScript : this.handleSaveScriptAs,
      };
    } else {
      return {
        text: intl.formatMessage({ id: "NewQuery.SaveView" }),
        onClick: this.handleSaveView,
      };
    }
  };

  renderSaveButton = () => {
    const {
      dataset,
      disableButtons,
      location,
      activeScript,
      numberOfMineScripts,
      isMultiTabEnabled,
    } = this.props;
    const canAlter = dataset.getIn(["permissions", "canAlter"]);
    const canSelect = dataset.getIn(["permissions", "canSelect"]);
    const mustSaveDatasetAs =
      dataset.getIn(["fullPath", 0]) === "tmp" || (!canAlter && canSelect);
    const isUntitledScript = !activeScript.id;
    const isExtraDisabled = this.getExtraSaveDisable(dataset);
    const isSqlEditorTab = exploreUtils.isSqlEditorTab(location);

    return (
      <>
        <DropdownMenu
          className="explore-save-button"
          disabled={disableButtons}
          isButton
          groupDropdownProps={
            isMultiTabEnabled && isTabbableUrl(location)
              ? this.getTabSaveButton()
              : this.getDefaultSaveButton()
          }
          menu={
            <SaveMenu
              action={this.doButtonAction}
              scriptPermissions={activeScript.permissions}
              mustSaveDatasetAs={mustSaveDatasetAs}
              isUntitledScript={isUntitledScript}
              isSqlEditorTab={isSqlEditorTab}
              disableBoth={isExtraDisabled}
              numberOfMineScripts={numberOfMineScripts}
            />
          }
        />
      </>
    );
  };

  renderHeaders() {
    switch (this.props.pageType) {
      case PageTypes.graph:
      case PageTypes.details:
      case PageTypes.reflections:
      case PageTypes.wiki:
      case PageTypes.history:
        return;
      case PageTypes.default:
        return <div className="ExploreHeader">{this.renderHeader()}</div>;
      default:
        throw new Error(`not supported page type; '${this.props.pageType}'`);
    }
  }

  submitScript = async (payload) => {
    const { router, intl, addNotification } = this.props;
    try {
      const result = await newPopulatedTab(payload);
      this.setState({ isSaveAsModalOpen: false });
      addNotification(
        intl.formatMessage({ id: "NewQuery.ScriptSaved" }),
        "success",
      );
      handleOpenTabScript(router)(result);

      ScriptsResource.fetch();
      fetchAllAndMineScripts(this.props.fetchScripts, null);
      this.setState({ saveAsDialogError: null });
    } catch (e) {
      this.setState({ saveAsDialogError: e });
      sentryUtil.logException(e);
    }
  };

  render() {
    const { dataset, location, router } = this.props;
    const {
      isSaveAsModalOpen,
      SQLScriptDeletedDialogProps,
      isSQLScriptDeletedDialogOpen,
    } = this.state;
    const isDatasetPage = exploreUtils.isExploreDatasetPage(location);
    const projectId = getSonarContext()?.getSelectedProjectId?.();
    return (
      <div className="ExploreHeader__container">
        {this.renderHeaders()}
        {isSQLScriptDeletedDialogOpen && (
          <SQLScriptDeletedDialog
            isOpen={isSQLScriptDeletedDialogOpen}
            getSqlContent={() => {
              const currentSql = this.props.getCurrentSql();
              return currentSql != null ? currentSql : this.props.datasetSql;
            }}
            {...SQLScriptDeletedDialogProps}
          />
        )}
        {isSaveAsModalOpen && (
          <SQLScriptDialog
            title="Save Script as..."
            mustSaveAs={dataset.getIn(["fullPath", 0]) === "tmp"}
            isOpen={isSaveAsModalOpen}
            {...(this.props.isMultiTabEnabled && {
              submit: this.submitScript,
            })}
            onCancel={() =>
              this.setState({
                isSaveAsModalOpen: false,
                saveAsDialogError: null,
              })
            }
            getScriptToSave={() => {
              const currentSql = this.props.getCurrentSql();
              return {
                context: this.props.queryContext,
                content:
                  currentSql != null ? currentSql : this.props.datasetSql,
              };
            }}
            onSubmit={this.props.createScript}
            postSubmit={(payload) => {
              this.props.resetTableState();
              fetchAllAndMineScripts(this.props.fetchScripts, null);
              this.props.setActiveScript({ script: payload });
            }}
            saveAsDialogError={this.state.saveAsDialogError}
            push={(payload) => {
              isDatasetPage
                ? router.push({
                    pathname: sqlPaths.sqlEditor.link({ projectId }),
                    query: { scriptId: payload.id },
                    state: { renderScriptTab: true },
                  })
                : router.push({
                    pathname: location.pathname,
                    query: { ...location.query, scriptId: payload.id },
                  });
            }}
          />
        )}
      </div>
    );
  }
}

function mapStateToProps(state, props) {
  const { location = {} } = props;
  const version = location.query && location.query.version;
  const explorePageState = getExploreState(state);
  const jobProgress = getJobProgress(state, version);
  const runStatus = getRunStatus(state).isRun;
  const jobId = getExploreJobId(state);
  return {
    location: state.routing.locationBeforeTransitions || {},
    history: getHistory(state, props.dataset.get("tipVersion")),
    tableColumns: getTableColumns(state, props.dataset.get("datasetVersion")),
    jobProgress,
    runStatus,
    jobId,
    queryContext: explorePageState.view.queryContext,
    showWiki: isWikAvailable(state, location),
    activeScript: getActiveScript(state),
    numberOfMineScripts: getNumberOfMineScripts(state),
    activeScriptPermissions: getActiveScriptPermissions(state),
    queryStatuses: explorePageState.view.queryStatuses,
    isMultiQueryRunning: explorePageState.view.isMultiQueryRunning,
    actionState: explorePageState.view.actionState,
    user: state.account.get("user"),
    scriptsSyncPending: getScriptsSyncPending(state),
  };
}

export default compose(
  injectIntl,
  withRouter,
  withIsMultiTabEnabled,
  connect(mapStateToProps, {
    transformHistoryCheck,
    performTransform,
    performTransformAndRun,
    runDatasetSql,
    previewDatasetSql,
    saveDataset,
    saveAsDataset,
    startDownloadDataset,
    performNextAction,
    showConfirmationDialog,
    cancelJob: cancelJobAndShowNotification,
    setQueryStatuses,
    createScript,
    fetchScripts,
    updateScript,
    setActiveScript,
    setActionState,
    resetQueryState,
    addNotification,
    resetTableState,
  }),
  withDatasetChanges,
)(ExploreHeader);

const style = {
  dbName: {
    maxWidth: 300,
    display: "flex",
    alignItems: "center",
    color: "var(--text--primary)",
    fontWeight: 500,
  },
  pullout: {
    backgroundColor: "transparent",
    borderColor: "transparent",
    position: "relative",
    width: 30,
  },
};
