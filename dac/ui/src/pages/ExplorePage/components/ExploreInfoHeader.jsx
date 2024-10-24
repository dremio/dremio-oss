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
import DocumentTitle from "react-document-title";
import { injectIntl } from "react-intl";
import { browserHistory } from "react-router";
import CopyButton from "#oss/components/Buttons/CopyButton";
import { Tooltip } from "dremio-ui-lib";

import EllipsedText from "components/EllipsedText";
import modelUtils from "utils/modelUtils";
import { constructFullPath } from "utils/pathUtils";
import { formatMessage } from "utils/locale";
import { isSqlChanged } from "sagas/utils";

import { PHYSICAL_DATASET_TYPES, SCRIPT } from "#oss/constants/datasetTypes";
import { PageTypeButtons } from "#oss/pages/ExplorePage/components/PageTypeButtons";
import { PageTypes, pageTypesProp } from "#oss/pages/ExplorePage/pageTypes";

import exploreUtils from "#oss/utils/explore/exploreUtils";

import BreadCrumbs, { formatFullPath } from "components/BreadCrumbs";
import { DatasetItemLabel } from "components/Dataset/DatasetItemLabel"; // {} for testing purposes since store is not needed here
import { checkTypeToShowOverlay } from "utils/datasetUtils";
import { IconButton } from "dremio-ui-lib/components";
import {
  getIconByEntityType,
  getIconDataTypeFromDatasetType,
} from "utils/iconUtils";

import { getVersionContextFromId } from "dremio-ui-common/utilities/datasetReference.js";

import { getHistory, getExploreState } from "selectors/explore";
import { getActiveScript } from "#oss/selectors/scripts";
import { TagContent } from "#oss/pages/HomePage/components/BranchPicker/components/BranchPickerTag/BranchPickerTag";

import "./ExploreInfoHeader.less";

export class ExploreInfoHeader extends PureComponent {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    pageType: pageTypesProp,
    nessieState: PropTypes.object,

    // connected
    location: PropTypes.object,
    history: PropTypes.instanceOf(Immutable.Map),
    currentSql: PropTypes.string,
    activeScript: PropTypes.object,
    intl: PropTypes.object.isRequired,
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
      `NewQuery.${isSqlEditorTab ? "UntitledScript" : "NewQuery"}`,
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
      location,
    );
    const isEditedDataset = this.isEditedDataset();
    const isUnsavedScript = exploreUtils.isEditedScript(
      activeScript,
      currentSql,
    );
    const isSqlEditorTab = exploreUtils.isSqlEditorTab(location);
    const fullPath = ExploreInfoHeader.getFullPathListForDisplay(
      dataset,
      location,
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
      <>
        <EllipsedText text={labelText} className="heading">
          <span
            className={`page-title ${isUntitledScript ? "--untitledScript" : ""}`}
          >
            {nameForDisplay}
          </span>
        </EllipsedText>
        <span className="dataset-edited" data-qa="dataset-edited">
          {isEditedDataset || isUnsavedScript ? edited : ""}
        </span>
      </>
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
      location,
    );

    const isUnsavedScript = exploreUtils.isEditedScript(
      activeScript,
      currentSql,
    );

    const edited = intl.formatMessage({ id: "NewQuery.Unsaved" });
    const typeIcon = getIconDataTypeFromDatasetType(SCRIPT);
    const isUntitledScript = !this.props.activeScript.name;
    const labelText = `${nameForDisplay}${isUnsavedScript ? edited : ""}`;

    const LabelElement = (
      <>
        <EllipsedText text={labelText} className="heading">
          <span
            className={`page-title ${isUntitledScript ? "--untitledScript" : ""}`}
          >
            {nameForDisplay}
          </span>
        </EllipsedText>
        <span className="dataset-edited" data-qa="dataset-edited">
          {isUnsavedScript ? edited : ""}
        </span>
      </>
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
              <dremio-icon name="interface/settings" alt="" />
            </IconButton>
          </div>
        )}
      </div>
    );
  }
}

function mapStateToProps(state, ownProps) {
  const explorePageState = getExploreState(state);
  return {
    location: state.routing.locationBeforeTransitions || {},
    history: getHistory(state, ownProps.dataset.get("tipVersion")),
    currentSql: explorePageState.view.currentSql,
    activeScript: getActiveScript(state),
  };
}

export default compose(connect(mapStateToProps), injectIntl)(ExploreInfoHeader);

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
    backgroundColor: "var(--color--brand--300)",
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
    color: "var(--icon--primary)",
  },
  dbName: {
    maxWidth: 266,
    display: "flex",
    alignItems: "center",
    color: "var(--text--primary)",
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
