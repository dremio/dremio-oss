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
import { connect } from "react-redux";
import PropTypes from "prop-types";
import Immutable from "immutable";
import DocumentTitle from "react-document-title";
import { injectIntl } from "react-intl";
import { browserHistory } from "react-router";
import CopyButton from "@app/components/Buttons/CopyButton";
import { Tooltip } from "dremio-ui-lib";

import DropdownMenu from "@app/components/Menus/DropdownMenu";
import EllipsedText from "components/EllipsedText";
import modelUtils from "utils/modelUtils";
import {
  constructFullPath,
  navigateToExploreDefaultIfNecessary,
} from "utils/pathUtils";
import { formatMessage } from "utils/locale";
import { needsTransform, isSqlChanged } from "sagas/utils";

import { PHYSICAL_DATASET_TYPES, SCRIPT } from "@app/constants/datasetTypes";
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
import { PageTypeButtons } from "@app/pages/ExplorePage/components/PageTypeButtons";
import { PageTypes, pageTypesProp } from "@app/pages/ExplorePage/pageTypes";

import { startDownloadDataset } from "actions/explore/download";
import { performNextAction, NEXT_ACTIONS } from "actions/explore/nextAction";

// import DatasetAccelerationButton from 'dyn-load/components/Acceleration/DatasetAccelerationButton'; // To Be Removed
import ExploreInfoHeaderMixin from "dyn-load/pages/ExplorePage/components/ExploreInfoHeaderMixin";
import exploreUtils from "@app/utils/explore/exploreUtils";

import SaveMenu, {
  DOWNLOAD_TYPES,
} from "components/Menus/ExplorePage/SaveMenu";
import BreadCrumbs, { formatFullPath } from "components/BreadCrumbs";
import FontIcon from "components/Icon/FontIcon";
import { DatasetItemLabel } from "components/Dataset/DatasetItemLabel"; // {} for testing purposes since store is not needed here
import { checkTypeToShowOverlay } from "utils/datasetUtils";
import { IconButton } from "dremio-ui-lib/components";
import {
  getIconByEntityType,
  getIconDataTypeFromDatasetType,
} from "utils/iconUtils";

import { getVersionContextFromId } from "dremio-ui-common/utilities/datasetReference.js";

import {
  getHistory,
  getTableColumns,
  getExploreState,
} from "selectors/explore";
import { getActiveScript } from "@app/selectors/scripts";
import { TagContent } from "@app/pages/HomePage/components/BranchPicker/components/BranchPickerTag/BranchPickerTag";

import "./ExploreInfoHeader.less";

export const TABLEAU_TOOL_NAME = "Tableau";
export const QLIK_TOOL_NAME = "Qlik Sense";

@ExploreInfoHeaderMixin
// TODO: this file has a lot of unused code since it has been moved to ExploreHeader.js
export class ExploreInfoHeader extends PureComponent {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    pageType: pageTypesProp,
    toggleRightTree: PropTypes.func.isRequired,
    grid: PropTypes.object,
    space: PropTypes.object,
    rightTreeVisible: PropTypes.bool,
    location: PropTypes.object,
    exploreViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    intl: PropTypes.object.isRequired,
    nessieState: PropTypes.object,

    // connected
    history: PropTypes.instanceOf(Immutable.Map),
    queryContext: PropTypes.instanceOf(Immutable.List),
    currentSql: PropTypes.string,
    tableColumns: PropTypes.instanceOf(Immutable.List),
    activeScript: PropTypes.object,

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
  };

  static contextTypes = {
    router: PropTypes.object.isRequired,
  };

  static getFullPathListForDisplay(dataset, location) {
    const unsavedVDS = !dataset.getIn(["apiLinks", "namespaceEntity"]);
    const hasDatasetName = modelUtils.isNamedDataset(dataset);
    if (!dataset || (unsavedVDS && !hasDatasetName)) {
      return;
    }
    const fullPath = dataset.get("displayFullPath");
    return modelUtils.isNamedDataset(dataset) &&
      exploreUtils.isExploreDatasetPage(location)
      ? fullPath
      : undefined;
  }

  static getNameForDisplay(dataset, activeScript = {}, location) {
    const isSqlEditorTab = exploreUtils.isSqlEditorTab(location);
    const isDatasetPage = exploreUtils.isExploreDatasetPage(location);
    const defaultName = formatMessage(
      `NewQuery.${isSqlEditorTab ? "UntitledScript" : "NewQuery"}`
    );
    const unsavedVDS = !dataset.getIn(["apiLinks", "namespaceEntity"]);
    const hasDatasetName = modelUtils.isNamedDataset(dataset);

    if ((!dataset || (unsavedVDS && !hasDatasetName)) && !activeScript.name) {
      return defaultName;
    }

    if (activeScript.name && !modelUtils.isNamedDataset(dataset))
      return activeScript.name;

    const displayFullPath = dataset.get("displayFullPath");
    return modelUtils.isNamedDataset(dataset) &&
      displayFullPath &&
      isDatasetPage
      ? displayFullPath.get(-1)
      : defaultName;
  }

  constructor(props) {
    super(props);

    this.doButtonAction = this.doButtonAction.bind(this);
    this.downloadDataset = this.downloadDataset.bind(this);
    this.state = {};
  }

  doButtonAction(actionType) {
    switch (actionType) {
      case "saveAs":
        return this.handleSaveAs();
      case "run":
        return this.handleRunClick();
      case "preview":
        return this.handlePreviewClick();
      case "save":
        return this.handleSave();
      case DOWNLOAD_TYPES.json:
      case DOWNLOAD_TYPES.csv:
      case DOWNLOAD_TYPES.parquet:
        return this.downloadDataset(actionType);
      default:
        break;
    }
  }

  handleRunClick() {
    this.navigateToExploreTableIfNecessary();
    this.props.runDatasetSql();
  }

  handlePreviewClick() {
    this.navigateToExploreTableIfNecessary();
    this.props.previewDatasetSql();
  }

  //TODO: DX-14762 - refactor to use runDatasetSql and performTransform saga;
  // investigate replacing pathutils.navigateToExploreTableIfNecessary with pageTypeUtils methods

  isTransformNeeded() {
    const { dataset, queryContext, currentSql } = this.props;
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

  openDatasetSettings = () => {
    const { dataset, pageType } = this.props;

    const versionContext = getVersionContextFromId(dataset.get("entityId"));

    browserHistory.push({
      ...location,
      state: {
        modal: "DatasetSettingsModal",
        datasetUrl:
          !versionContext && dataset.getIn(["apiLinks", "namespaceEntity"]),
        datasetType: dataset.get("datasetType"),
        type: dataset.get("datasetType"),
        entityName: dataset.get("displayFullPath").last(),
        isHomePage: false,
        isDatasetReflectionPage: pageType === PageTypes.reflections,
      },
    });
  };
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

  handleSave = () => {
    const nextAction = this.state.nextAction;
    this.setState({ nextAction: undefined });
    this.transformIfNecessary((didTransform, dataset) => {
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
    });
  };

  handleSaveAs = () => {
    const nextAction = this.state.nextAction;
    this.setState({ nextAction: undefined });
    this.transformIfNecessary(() => this.props.saveAsDataset(nextAction));
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
      />
    ) : null;
  }

  renderDatasetLabel(dataset) {
    const { activeScript, location, intl, currentSql, nessieState } =
      this.props;
    const nameForDisplay = ExploreInfoHeader.getNameForDisplay(
      dataset,
      activeScript,
      location
    );
    const isEditedDataset = this.isEditedDataset();
    const isUnsavedScript = exploreUtils.isEditedScript(
      activeScript,
      currentSql
    );
    const isSqlEditorTab = exploreUtils.isSqlEditorTab(location);
    const fullPath = ExploreInfoHeader.getFullPathListForDisplay(
      dataset,
      location
    );
    const edited = intl.formatMessage({
      id: isSqlEditorTab ? "NewQuery.Unsaved" : "Dataset.Edited",
    });
    const datasetType = dataset.get("datasetType");
    const versionContext = getVersionContextFromId(dataset.get("entityId"));
    const typeIcon = isSqlEditorTab
      ? getIconDataTypeFromDatasetType(SCRIPT)
      : getIconByEntityType(datasetType, !!versionContext);
    const showOverlay = checkTypeToShowOverlay(datasetType);

    const isUntitledScript = isSqlEditorTab && !this.props.activeScript.name;
    const labelText = `${nameForDisplay}${
      isEditedDataset || isUnsavedScript ? edited : ""
    }`;
    const LabelElement = (
      <EllipsedText text={labelText} className="heading">
        <span
          className={`page-title ${isUntitledScript ? "--untitledScript" : ""}`}
        >
          {nameForDisplay}
        </span>
        <span className="dataset-edited" data-qa="dataset-edited">
          {isEditedDataset || isUnsavedScript ? edited : ""}
        </span>
      </EllipsedText>
    );

    const refFromVersionContext = versionContext && {
      type: versionContext.type,
      name: versionContext.value,
    };

    const nessieStateRef =
      nessieState?.reference ??
      nessieState?.defaultReference ??
      refFromVersionContext;

    return (
      <DatasetItemLabel
        showSummaryOverlay={false}
        customNode={
          // todo: string replace loc
          <div className="flexbox-truncate-text-fix">
            <div
              style={{
                ...style.dbName,
                ...(isSqlEditorTab ? style.scriptHeader : {}),
              }}
              data-qa={nameForDisplay}
            >
              {isUntitledScript || showOverlay ? (
                LabelElement
              ) : (
                <Tooltip enterDelay={1000} title={labelText}>
                  {LabelElement}
                </Tooltip>
              )}
              {this.renderCopyToClipBoard(constructFullPath(fullPath))}
            </div>
            {fullPath && (
              <>
                <BreadCrumbs
                  hideLastItem
                  longCrumbs={false}
                  fullPath={fullPath}
                  pathname={location.pathname}
                />
                {nessieStateRef && (
                  <div className="referenceTag">
                    <TagContent
                      reference={nessieStateRef}
                      hash={nessieStateRef.hash}
                    />
                  </div>
                )}
              </>
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
        typeIcon={typeIcon}
        shouldShowOverlay={false}
      />
    );
  }

  // renders a generic header when looking at new failed queries in the SQL editor tab
  renderGenericHeader(dataset) {
    const { activeScript, currentSql, intl, location } = this.props;

    const nameForDisplay = ExploreInfoHeader.getNameForDisplay(
      dataset,
      activeScript,
      location
    );

    const isUnsavedScript = exploreUtils.isEditedScript(
      activeScript,
      currentSql
    );

    const edited = intl.formatMessage({ id: "NewQuery.Unsaved" });
    const typeIcon = getIconDataTypeFromDatasetType(SCRIPT);
    const isUntitledScript = !this.props.activeScript.name;
    const labelText = `${nameForDisplay}${isUnsavedScript ? edited : ""}`;

    const LabelElement = (
      <EllipsedText text={labelText} className="heading">
        <span
          className={`page-title ${isUntitledScript ? "--untitledScript" : ""}`}
        >
          {nameForDisplay}
        </span>
        <span className="dataset-edited" data-qa="dataset-edited">
          {isUnsavedScript ? edited : ""}
        </span>
      </EllipsedText>
    );

    return (
      <DatasetItemLabel
        customNode={
          <div className="flexbox-truncate-text-fix">
            <div
              style={{
                ...style.dbName,
                ...style.scriptHeader,
              }}
              data-qa={nameForDisplay}
            >
              {isUntitledScript ? (
                LabelElement
              ) : (
                <Tooltip enterDelay={1000} title={labelText}>
                  {LabelElement}
                </Tooltip>
              )}
            </div>
            {<DocumentTitle title={nameForDisplay} />}
          </div>
        }
        showFullPath
        placement="right"
        typeIcon={typeIcon}
        shouldShowOverlay={false}
      />
    );
  }

  renderLeftPartOfHeader(dataset) {
    const { location, pageType } = this.props;

    if (!dataset.get("datasetType")) {
      if (exploreUtils.isSqlEditorTab(location)) {
        return (
          <div style={style.leftPart}>
            <div style={style.leftWrap}>
              <div className="title-wrap" style={style.titleWrap}>
                {this.renderGenericHeader(dataset)}
              </div>
            </div>
          </div>
        );
      } else {
        return <div style={style.leftPart}></div>;
      }
    }

    return (
      <div style={style.leftPart}>
        <div style={style.leftWrap}>
          <div className="title-wrap" style={style.titleWrap}>
            {this.renderDatasetLabel(dataset)}
          </div>
        </div>
        <PageTypeButtons
          dataQa="page-type-buttons"
          selectedPageType={pageType}
          dataset={dataset}
        />
      </div>
    );
  }

  openTableau = () => {
    this.handleShowBI(NEXT_ACTIONS.openTableau);
  };
  openPowerBi = () => {
    this.handleShowBI(NEXT_ACTIONS.openPowerBI);
  };

  renderSaveButton = () => {
    const { dataset } = this.props;
    const shouldEnableButtons =
      dataset.get("isNewQuery") || dataset.get("datasetType"); // new query or loaded
    const mustSaveDatasetAs = dataset.getIn(["fullPath", 0]) === "tmp";
    const isExtraDisabled = this.getExtraSaveDisable(dataset);

    return (
      <DropdownMenu
        className="explore-save-button"
        text={this.props.intl.formatMessage({ id: "Common.Save" })}
        disabled={!shouldEnableButtons}
        isButton
        hideDivider
        menu={
          <SaveMenu
            action={this.doButtonAction}
            mustSaveDatasetAs={mustSaveDatasetAs}
            disableBoth={isExtraDisabled}
          />
        }
      />
    );
  };

  // this feature disabled for now
  renderRightTreeToggler() {
    return !this.props.rightTreeVisible ? (
      <button
        className="info-button toogler"
        style={style.pullout}
        onClick={this.props.toggleRightTree}
      >
        <FontIcon type="Expand" />
      </button>
    ) : null;
  }

  render() {
    const {
      dataset,
      intl: { formatMessage },
    } = this.props;
    const isDataset = exploreUtils.isExploreDatasetPage(location);
    return (
      <div className="explore-info-header">
        {this.renderLeftPartOfHeader(dataset)}
        {isDataset && (
          <div style={style.rightPart}>
            <IconButton
              tooltip={formatMessage({ id: "Dataset.Settings" })}
              onClick={() => this.openDatasetSettings()}
              data-qa="dataset-settings"
            >
              <dremio-icon name="interface/settings" />
            </IconButton>
          </div>
        )}
      </div>
    );
  }
}
ExploreInfoHeader = injectIntl(ExploreInfoHeader);

function mapStateToProps(state, ownProps) {
  const explorePageState = getExploreState(state);
  return {
    location: state.routing.locationBeforeTransitions || {},
    history: getHistory(state, ownProps.dataset.get("tipVersion")),
    currentSql: explorePageState.view.currentSql,
    queryContext: explorePageState.view.queryContext,
    tableColumns: getTableColumns(
      state,
      ownProps.dataset.get("datasetVersion")
    ),
    activeScript: getActiveScript(state),
  };
}

export default connect(mapStateToProps, {
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
})(ExploreInfoHeader);

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
  },
  leftWrap: {
    display: "flex",
    width: 408,
    flexWrap: "wrap",
    userSelect: "text",
  },
  leftPart: {
    display: "flex",
    alignContent: "center",
    alignItems: "center",
    width: "100%",
  },
  rightPart: {
    margin: "16px 8px 16px auto",
    color: "var(--dremio--color--icon--main)",
  },
  dbName: {
    maxWidth: 266,
    display: "flex",
    alignItems: "center",
    color: "#333",
    fontWeight: 500,
  },
  scriptHeader: {
    marginTop: 2,
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
