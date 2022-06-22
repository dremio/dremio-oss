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
import { connect } from "react-redux";
import Radium from "radium";
import PropTypes from "prop-types";
import Immutable from "immutable";
import DocumentTitle from "react-document-title";
import { FormattedMessage, injectIntl } from "react-intl";
import { withRouter } from "react-router";
import { Tooltip } from "dremio-ui-lib";

import CopyButton from "@app/components/Buttons/CopyButton";
import * as ButtonTypes from "@app/components/Buttons/ButtonTypes";

import DropdownMenu from "@app/components/Menus/DropdownMenu";
import EllipsedText from "components/EllipsedText";
import modelUtils from "utils/modelUtils";
import {
  constructFullPath,
  navigateToExploreDefaultIfNecessary,
} from "utils/pathUtils";
import { formatMessage } from "utils/locale";
import { needsTransform, isSqlChanged } from "sagas/utils";

import { PHYSICAL_DATASET_TYPES } from "@app/constants/datasetTypes";
import { replace } from "react-router-redux";
import explorePageInfoHeaderConfig from "@inject/pages/ExplorePage/components/explorePageInfoHeaderConfig";

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
import { PageTypes, pageTypesProp } from "@app/pages/ExplorePage/pageTypes";
import { withDatasetChanges } from "@app/pages/ExplorePage/DatasetChanges";
import { showUnsavedChangesConfirmDialog } from "@app/actions/confirmation";

import { startDownloadDataset } from "actions/explore/download";
import { performNextAction, NEXT_ACTIONS } from "actions/explore/nextAction";
import { editOriginalSql } from "actions/explore/dataset/reapply";

import ExploreHeaderMixin from "@app/pages/ExplorePage/components/ExploreHeaderMixin";
import config from "dyn-load/utils/config";
import { getAnalyzeToolsConfig } from "@app/utils/config";
import exploreUtils from "@app/utils/explore/exploreUtils";
import { VIEW_ID as SCRIPTS_VIEW_ID } from "@app/components/SQLScripts/SQLScripts";

import SaveMenu, {
  DOWNLOAD_TYPES,
} from "components/Menus/ExplorePage/SaveMenu";
import BreadCrumbs, { formatFullPath } from "components/BreadCrumbs";
import FontIcon from "components/Icon/FontIcon";
import DatasetItemLabel from "components/Dataset/DatasetItemLabel";
import Art from "@app/components/Art";
import { Button } from "dremio-ui-lib";
import { showQuerySpinner } from "@inject/pages/ExplorePage/utils";
import { getIconDataTypeFromDatasetType } from "utils/iconUtils";

import {
  getHistory,
  getTableColumns,
  getJobProgress,
  getRunStatus,
  getExploreJobId,
  getExploreState,
  isWikAvailable,
} from "selectors/explore";
import {
  getActiveScript,
  getActiveScriptPermissions,
  getNumberOfMineScripts,
} from "selectors/scripts";
import { HANDLE_THROUGH_API } from "@inject/pages/HomePage/components/HeaderButtonConstants";
import { cancelJobAndShowNotification } from "@app/actions/jobs/jobs";
import SQLScriptDialog from "@app/components/SQLScripts/components/SQLScriptDialog/SQLScriptDialog";
import { setQueryStatuses, resetQueryState } from "actions/explore/view";
import {
  createScript,
  fetchScripts,
  updateScript,
  setActiveScript,
} from "actions/resources/scripts";
import {
  fetchAllAndMineScripts,
  MAX_MINE_SCRIPTS_ALLOWANCE,
  openPrivilegesModalForScript,
} from "@app/components/SQLScripts/sqlScriptsUtils";

import openPopupNotification from "@app/components/PopupNotification/PopupNotification";
import { ExploreActions } from "./ExploreActions";
import ExploreTableJobStatusSpinner from "./ExploreTable/ExploreTableJobStatusSpinner";
import * as classes from "./ExploreHeader.module.less";
import "./ExploreHeader.less";

export const TABLEAU_TOOL_NAME = "Tableau";
export const QLIK_TOOL_NAME = "Qlik Sense";

@ExploreHeaderMixin
export class ExploreHeader extends PureComponent {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    datasetSummary: PropTypes.object,
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

    // connected
    history: PropTypes.instanceOf(Immutable.Map),
    queryContext: PropTypes.instanceOf(Immutable.List),
    currentSql: PropTypes.string,
    tableColumns: PropTypes.instanceOf(Immutable.List),
    jobProgress: PropTypes.object,
    runStatus: PropTypes.bool,
    jobId: PropTypes.string,
    showWiki: PropTypes.bool,
    activeScript: PropTypes.object,
    queryStatuses: PropTypes.array,
    isMultiQueryRunning: PropTypes.bool,

    // provided by withDatasetChanges
    getDatasetChangeDetails: PropTypes.func,

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
    editOriginalSql: PropTypes.func,
    replaceUrlAction: PropTypes.func,
    showUnsavedChangesConfirmDialog: PropTypes.func,
    setQueryStatuses: PropTypes.func,
    createScript: PropTypes.func,
    fetchScripts: PropTypes.func,
    updateScript: PropTypes.func,
    setActiveScript: PropTypes.func,
    resetQueryState: PropTypes.func,
    supportFlagsObj: PropTypes.object,
    activeScriptPermissions: PropTypes.array,
    user: PropTypes.instanceOf(Immutable.Map),
    numberOfMineScripts: PropTypes.number,
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

  constructor(props) {
    super(props);

    this.doButtonAction = this.doButtonAction.bind(this);
    this.downloadDataset = this.downloadDataset.bind(this);

    this.state = {
      actionState: null,
      isSaveAsModalOpen: false,
      supportFlags: {},
    };
  }

  doButtonAction(actionType) {
    switch (actionType) {
      case "run":
        this.setState({ actionState: "run" });
        this.props.resetSqlTabs();
        return this.handleRunClick();
      case "preview":
        this.setState({ actionState: "preview" });
        this.props.resetSqlTabs();
        return this.handlePreviewClick();
      case "discard":
        return this.handleDiscardConfirm();
      case "saveView":
        return this.handleSaveView();
      case "saveViewAs":
        return this.handleSaveViewAs();
      case "saveScript":
        return this.handleSaveScript();
      case "saveScriptAs":
        return this.handleSaveScriptAs();
      case "cancel":
        this.props.setQueryStatuses(this.handleCancelAllJobs());
        return this.props.cancelJob(this.props.jobId);
      case DOWNLOAD_TYPES.json:
      case DOWNLOAD_TYPES.csv:
      case DOWNLOAD_TYPES.parquet:
        return this.downloadDataset(actionType);
      default:
        break;
    }
  }

  handleRunClick() {
    const { getSelectedSql } = this.props;

    this.navigateToExploreTableIfNecessary();
    if (getSelectedSql() !== "") {
      this.props.runDatasetSql({ selectedSql: getSelectedSql() });
    } else {
      this.props.runDatasetSql();
    }
  }

  handlePreviewClick() {
    const { getSelectedSql } = this.props;
    this.navigateToExploreTableIfNecessary();
    this.props.previewDatasetSql({ selectedSql: getSelectedSql() });
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
    const { dataset, queryContext, currentSql } = this.props;
    return needsTransform(dataset, queryContext, currentSql);
  }

  transformIfNecessary(callback, forceDataLoad, isSaveViewAs) {
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
      this.context.router
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
            nextAction
          );
        });
      },
      undefined,
      true
    );
  };

  handleSaveViewAs = () => {
    const nextAction = this.state.nextAction;
    this.setState({ nextAction: undefined });
    this.transformIfNecessary(
      () => this.props.saveAsDataset(nextAction),
      undefined,
      true
    );
  };

  handleSaveScript = () => {
    const { activeScript, currentSql, queryContext, intl } = this.props;
    if (!activeScript.id) {
      this.handleSaveScriptAs();
    } else {
      const payload = {
        name: activeScript.name,
        content: currentSql,
        context: queryContext.toJS(),
        description: "",
      };
      return this.props.updateScript(payload, activeScript.id).then((res) => {
        if (!res.error) {
          this.props.setActiveScript({ script: res.payload });
          openPopupNotification({
            type: "success",
            message: intl.formatMessage({ id: "NewQuery.ScriptSaved" }),
          });
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
    this.props.router.push({
      pathname: "/new_query",
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
      closeButtonType: "XBig",
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
        iconSize="LARGE"
        placement="right"
        typeIcon={getIconDataTypeFromDatasetType(dataset.get("datasetType"))}
        shouldShowOverlay={false}
      />
    );
  }

  wrapWithTooltip(button, title, cmd, disabled) {
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
      <Tooltip title={tooltip} placement="top">
        <span {...(disabled && { style: { pointerEvents: "none" } })}>
          {button}
        </span>
      </Tooltip>
    );
  }

  renderHeader() {
    const { statusesArray, isMultiQueryRunning, jobId, disableButtons, intl } =
      this.props;

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
      isCancellable &&
      (this.state.actionState === "run" ||
        this.state.actionState === "preview");
    const cancelText = intl.formatMessage({ id: "Common.Cancel" });
    const runText = intl.formatMessage({ id: "Common.Run" });
    const previewText = intl.formatMessage({ id: "Common.Preview" });
    const discardText = intl.formatMessage({ id: "Common.Discard" });

    return (
      <>
        <div className="ExploreHeader__left">
          {isCancellable && jobId && this.state.actionState === "run"
            ? this.wrapWithTooltip(
                <Button
                  color="secondary"
                  type={ButtonTypes.SECONDARY}
                  data-qa="qa-cancel"
                  onClick={() => this.doButtonAction("cancel")}
                  style={{ width: 75, fontSize: 14 }}
                  disableMargin
                >
                  {cancelText}
                </Button>,
                cancelText
              )
            : this.wrapWithTooltip(
                <Button
                  className="run-btn"
                  color="primary"
                  type={ButtonTypes.PRIMARY}
                  data-qa="qa-run"
                  onClick={() => this.doButtonAction("run")}
                  style={{ width: 75, fontSize: 14 }}
                  disabled={disableRunButton || disableButtons}
                  disableMargin
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
                disableRunButton || disableButtons
              )}
          {isCancellable && jobId && this.state.actionState === "preview"
            ? this.wrapWithTooltip(
                <Button
                  color="secondary"
                  type={ButtonTypes.SECONDARY}
                  data-qa="qa-cancel"
                  onClick={() => this.doButtonAction("cancel")}
                  style={{ width: 98, fontSize: 14 }}
                  disableMargin
                >
                  {cancelText}
                </Button>,
                cancelText
              )
            : this.wrapWithTooltip(
                <Button
                  color="primary"
                  className="preview-btn"
                  variant={ButtonTypes.OUTLINED}
                  data-qa="qa-preview"
                  onClick={() => this.doButtonAction("preview")}
                  style={{ width: 98, fontSize: 14 }}
                  disabled={disablePreviewButton || disableButtons}
                  disableMargin
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
                  {previewText}
                </Button>,
                previewText,
                this.props.keyboardShortcuts.preview,
                disablePreviewButton || disableButtons
              )}

          {exploreUtils.isSqlEditorTab(this.props.location) &&
            this.wrapWithTooltip(
              <Button
                color="primary"
                className="discard-btn"
                variant={ButtonTypes.OUTLINED}
                data-qa="qa-discard"
                onClick={() => this.doButtonAction("discard")}
                style={{ width: 98, fontSize: 14 }}
                disableMargin
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
                {discardText}
              </Button>,
              discardText,
              undefined,
              isCancellable || disableButtons
            )}

          {this.renderEditOriginalButton()}

          <ExploreActions
            dataset={this.props.dataset}
            pageType={this.props.pageType}
            exploreViewState={this.props.exploreViewState}
            disableEnginePickMenu={!!disableEnginePickMenu}
          />
          {showQuerySpinner() && (
            <ExploreTableJobStatusSpinner
              jobProgress={this.props.jobProgress}
              jobId={this.props.jobId}
              action={this.state.actionState}
              message="Running"
            />
          )}
        </div>
        <div className="ExploreHeader__right">
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
    return (
      <Button
        variant="outlined"
        color="primary"
        size="medium"
        onClick={onclick}
        className={className}
        disabled={this.getExtraSaveDisable(dataset)}
        disableRipple
        disableMargin
      >
        {/* FIXME: PowerBI icon does not render when using <dremio-icon> */}
        <Art
          src={icon}
          alt={name}
          title={name}
          style={{ height: iconSize, width: iconSize }}
        />
      </Button>
    );
  };

  renderPrivilegesIconButton = () => {
    const {
      activeScriptPermissions,
      activeScript,
      location,
      router,
      currentSql,
      intl: { formatMessage },
    } = this.props;

    const hasPermission =
      activeScriptPermissions &&
      activeScriptPermissions.includes("MANAGE_GRANTS");

    const allowsPrivModal =
      hasPermission && !exploreUtils.isEditedScript(activeScript, currentSql);

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
                      noDataText: formatMessage({
                        id: "Privileges.Script.Not.Shared",
                      }),
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
    const { showWiki } = this.props;
    const { supportFlags } = this.state;
    if (!showWiki) return;

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
            la("Tableau"),
            "Tableau.svg",
            this.openTableau,
            20,
            "-noImgHover -noMinWidth"
          )}
        {showPowerBI &&
          this.renderAnalyzeButton(
            la("Power BI"),
            "PBI Logo.svg",
            this.openPowerBi,
            20,
            "-noImgHover -noMinWidth"
          )}
      </Fragment>
    );
  };

  renderSaveButton = () => {
    const {
      dataset,
      disableButtons,
      location,
      activeScript,
      numberOfMineScripts,
    } = this.props;
    const mustSaveDatasetAs = dataset.getIn(["fullPath", 0]) === "tmp";
    const isUntitledScript = !activeScript.id;
    const isExtraDisabled = this.getExtraSaveDisable(dataset);
    const isSqlEditorTab = exploreUtils.isSqlEditorTab(location);

    const canModifyScriptDefaultText = exploreUtils.hasPermissionToModify(
      activeScript
    )
      ? "Save Script"
      : "Save Script As";
    const canModifyScriptDefaultOnClick = exploreUtils.hasPermissionToModify(
      activeScript
    )
      ? this.handleSaveScript
      : this.handleSaveScriptAs;

    const groupDropdownProps = {
      text:
        isSqlEditorTab &&
        (!isUntitledScript || numberOfMineScripts < MAX_MINE_SCRIPTS_ALLOWANCE)
          ? canModifyScriptDefaultText
          : "Save View",
      onClick:
        isSqlEditorTab &&
        (!isUntitledScript || numberOfMineScripts < MAX_MINE_SCRIPTS_ALLOWANCE)
          ? canModifyScriptDefaultOnClick
          : this.handleSaveView,
    };

    return (
      <>
        <DropdownMenu
          className="explore-save-button"
          disabled={disableButtons}
          isButton
          groupDropdownProps={groupDropdownProps}
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

  // this feature disabled for now
  renderRightTreeToggler() {
    return !this.props.rightTreeVisible ? (
      <button
        className="info-button toogler"
        style={[style.pullout]}
        onClick={this.props.toggleRightTree}
      >
        <FontIcon type="Expand" />
      </button>
    ) : null;
  }

  renderHeaders() {
    switch (this.props.pageType) {
      case PageTypes.graph:
      case PageTypes.details:
      case PageTypes.reflections:
      case PageTypes.wiki:
        return;
      case PageTypes.default:
        return <div className="ExploreHeader">{this.renderHeader()}</div>;
      default:
        throw new Error(`not supported page type; '${this.props.pageType}'`);
    }
  }

  handleEditOriginal = () => {
    const {
      dataset,
      editOriginalSql: editSql,
      datasetSummary,
      replaceUrlAction,
      getDatasetChangeDetails,
      showUnsavedChangesConfirmDialog: showConfirm,
    } = this.props;

    const reapply = () => {
      editSql(dataset.get("id"), dataset.getIn(["apiLinks", "self"]));
    };

    if (this.isDatasetReadyForReapply()) {
      const { sqlChanged, historyChanged } = getDatasetChangeDetails();

      if (sqlChanged || historyChanged) {
        showConfirm({
          confirm: reapply,
        });
      } else {
        reapply();
      }
    } else if (
      datasetSummary &&
      datasetSummary.links &&
      datasetSummary.links.edit
    ) {
      replaceUrlAction(datasetSummary.links.edit);
    }
  };

  isDatasetReadyForReapply = () => {
    const { dataset } = this.props;
    return (
      dataset.get("canReapply") &&
      dataset.getIn(["apiLinks", "namespaceEntity"])
    );
  };

  renderEditOriginalButton() {
    const { datasetSummary, intl, location } = this.props;
    const editText = intl.formatMessage({ id: "SQL.EditOriginal" });
    if (
      (this.isDatasetReadyForReapply() || datasetSummary) &&
      !exploreUtils.isSqlEditorTab(location)
    ) {
      return this.wrapWithTooltip(
        <Button
          className="edit-btn"
          variant="outlined"
          color="primary"
          size="medium"
          onClick={this.handleEditOriginal}
          disableRipple
          disableMargin
          style={{ fontSize: 14 }}
        >
          <dremio-icon
            name="interface/edit"
            alt={editText}
            class={classes["edit-icon"]}
          />
          <FormattedMessage id="SQL.EditOriginal" />
        </Button>,
        editText
      );
    }
  }

  render() {
    const { dataset, location } = this.props;
    const { isSaveAsModalOpen } = this.state;
    const isDatasetPage = exploreUtils.isExploreDatasetPage(location);
    return (
      <div className="ExploreHeader__container">
        {this.renderHeaders()}
        {isSaveAsModalOpen && (
          <SQLScriptDialog
            title="Save Script as..."
            mustSaveAs={dataset.getIn(["fullPath", 0]) === "tmp"}
            isOpen={isSaveAsModalOpen}
            onCancel={() => this.setState({ isSaveAsModalOpen: false })}
            // eslint-disable-next-line
            script={{
              context: this.props.queryContext,
              content:
                this.props.currentSql != null
                  ? this.props.currentSql
                  : this.props.datasetSql,
            }}
            onSubmit={this.props.createScript}
            postSubmit={(payload) => {
              fetchAllAndMineScripts(this.props.fetchScripts, null);
              this.props.setActiveScript({ script: payload });
            }}
            {...(isDatasetPage && {
              push: () =>
                this.props.router.push({
                  pathname: "/new_query",
                  state: { renderScriptTab: true },
                }),
            })}
          />
        )}
      </div>
    );
  }
}
ExploreHeader = injectIntl(Radium(ExploreHeader));

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
    datasetSummary: state.resources.entities.get("datasetSummary"),
    queryContext: explorePageState.view.queryContext,
    showWiki: isWikAvailable(state, location),
    activeScript: getActiveScript(state),
    numberOfMineScripts: getNumberOfMineScripts(state),
    activeScriptPermissions: getActiveScriptPermissions(state),
    queryStatuses: explorePageState.view.queryStatuses,
    isMultiQueryRunning: explorePageState.view.isMultiQueryRunning,
    user: state.account.get("user"),
  };
}

export default withRouter(
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
    editOriginalSql,
    replaceUrlAction: replace,
    showUnsavedChangesConfirmDialog,
    setQueryStatuses,
    createScript,
    fetchScripts,
    updateScript,
    setActiveScript,
    resetQueryState,
  })(withDatasetChanges(ExploreHeader))
);

const style = {
  base: {
    display: "flex",
    justifyContent: "flex-end",
    height: 52,
    padding: 0,
    margin: 0,
    borderBottom: "none",
    borderTop: "none",
    borderLeft: "none",
    borderRight: "none",
  },
  disabledStyle: {
    pointerEvents: "none",
    opacity: 0.7,
  },
  query: {
    textDecoration: "none",
    width: 100,
    height: 28,
    display: "flex",
    justifyContent: "center",
    alignItems: "center",
    backgroundColor: "#43B8C9",
    borderBottom: "1px solid #3399A8",
    borderRadius: 2,
    color: "#fff",
    ":hover": {
      backgroundColor: "rgb(104, 198, 211)",
    },
  },
  leftWrap: {
    display: "flex",
    maxWidth: 250,
    flexWrap: "wrap",
    userSelect: "text",
    marginRight: 150, // distance between a title and navigation buttons
  },
  leftPart: {
    display: "flex",
    alignContent: "center",
    alignItems: "center",
  },
  dbName: {
    maxWidth: 300,
    display: "flex",
    alignItems: "center",
    color: "#333",
    fontWeight: 500,
  },
  pullout: {
    backgroundColor: "transparent",
    borderColor: "transparent",
    position: "relative",
    width: 30,
  },
  noTextButton: {
    minWidth: 50,
    paddingRight: 10,
    paddingLeft: 5,
  },
  actionBtnWrap: {
    marginBottom: 0,
    marginLeft: 0,
    minWidth: 80,
  },
  narwhal: {
    Icon: {
      width: 22,
      height: 22,
    },
    Container: {
      width: 24,
      height: 24,
      marginRight: 10,
    },
  },
  titleWrap: {
    display: "flex",
    alignItems: "center",
  },
  triangle: {
    width: 0,
    height: 0,
    borderStyle: "solid",
    borderWidth: "0 4px 6px 4px",
    borderColor: "transparent transparent #fff transparent",
    position: "absolute",
    zIndex: 99999,
    right: 6,
    top: -6,
  },
  popover: {
    padding: 0,
  },
  iconButton: {
    minWidth: 40,
    outline: 0,
  },
  icon: {
    width: 20,
    height: 20,
    display: "flex",
    margin: "0 auto",
  },
};
