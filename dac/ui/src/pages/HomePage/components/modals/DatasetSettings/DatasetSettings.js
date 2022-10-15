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
import Immutable from "immutable";
import PropTypes from "prop-types";
import { createSelector } from "reselect";
import { injectIntl } from "react-intl";
import { LINE_NOROW_START_STRETCH } from "uiTheme/radium/flexStyle";
import { getSortedSources } from "@app/selectors/home";
import { isDcsEdition } from "dyn-load/utils/versionUtils";

import { loadDatasetForDatasetType } from "actions/resources";
//@ts-ignore
import Message from "@app/components/Message";
import ViewStateWrapper from "components/ViewStateWrapper";
import { getViewState, getEntity } from "selectors/resources";
import { getHomeEntityOrChild } from "@app/selectors/home";

import AccelerationController from "components/Acceleration/AccelerationController";

import DatasetSettingsMixin from "dyn-load/pages/HomePage/components/modals/DatasetSettings/DatasetSettingsMixin";

import { showUnsavedChangesConfirmDialog } from "actions/confirmation";
import NavPanel from "components/Nav/NavPanel";
import FileFormatController from "./FileFormatController";
import AccelerationUpdatesController from "./AccelerationUpdates/AccelerationUpdatesController";
import DatasetOverviewForm from "./DatasetOverviewForm";
import DatasetCompactionForm from "@inject/pages/HomePage/components/modals/DatasetSettings/DatasetCompaction/DatasetCompactionForm";
import {
  postCompactionData,
  getAllCompactionData,
  patchCompactionData,
  startCompaction,
  clearCompactionStateData,
} from "@inject/actions/resources/compaction";
import {
  getSourceName,
  getArcticProjectId,
  getBranchName,
} from "@inject/pages/HomePage/components/modals/DatasetSettings/compactionUtils";

const COMPACTION = "COMPACTION";
const DATASET_SETTINGS_VIEW_ID = "DATASET_SETTINGS_VIEW_ID";

@DatasetSettingsMixin
export class DatasetSettings extends PureComponent {
  static contextTypes = {
    router: PropTypes.object,
    location: PropTypes.object,
  };

  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map),
    isHomePage: PropTypes.bool,
    tab: PropTypes.string,
    datasetType: PropTypes.string,
    datasetUrl: PropTypes.string,
    location: PropTypes.object,
    viewState: PropTypes.instanceOf(Immutable.Map),
    intl: PropTypes.object.isRequired,
    hide: PropTypes.func,
    updateFormDirtyState: PropTypes.func,
    showUnsavedChangesConfirmDialog: PropTypes.func,
    loadDatasetForDatasetType: PropTypes.func.isRequired,
    branchName: PropTypes.string,
    postCompactionData: PropTypes.func,
    startCompaction: PropTypes.func,
    getAllCompactionData: PropTypes.func,
    patchCompactionData: PropTypes.func,
    getCompactionData: PropTypes.func,
    clearCompactionStateData: PropTypes.func,
    arcticProjectId: PropTypes.string,
    compactionTasks: PropTypes.array,
    enableCompaction: PropTypes.bool,
    isAdmin: PropTypes.bool,
    isIcebergTable: PropTypes.bool,
  };

  state = {
    isFormDirty: false,
    loadingTask: true,
    loadingCompactionError: null,
  };

  componentWillMount() {
    const { datasetUrl, datasetType } = this.props;
    if (datasetUrl) {
      return this.props
        .loadDatasetForDatasetType(
          datasetType,
          datasetUrl,
          DATASET_SETTINGS_VIEW_ID
        )
        .then((response) => {
          if (!response.error) {
            const entity = response.payload.get("entities");
            const entityType = entity.keySeq().first();
            const entityId = response.payload.get("result");
            const { location } = this.props;
            this.context.router.replace({
              ...location,
              state: {
                ...location.state,
                entityType,
                entityId,
              },
            });
          }
          return null;
        });
    }
  }

  componentDidMount() {
    const {
      location,
      tab,
      arcticProjectId,
      getAllCompactionData: dispatchGetAllCompactionData,
      branchName,
      entity,
      isAdmin,
      enableCompaction,
    } = this.props;

    if (enableCompaction && isAdmin && arcticProjectId && entity) {
      const spaceName = [...entity.get("fullPathList").remove(0)].join(".");
      dispatchGetAllCompactionData({
        projectId: arcticProjectId,
        filter: `tableIdentifier=='${spaceName}'&&ref=='${branchName}'&&actionType=='${COMPACTION}'`,
        maxResults: "1",
      })
        .then(() => this.setState({ taskLoading: false }))
        .catch((error) => {
          this.setState({
            taskLoading: false,
            loadingCompactionError: error.errorMessage,
          });
        });
    }

    if (!tab) {
      this.context.router.replace({
        ...location,
        state: {
          ...location.state,
          tab: this.getTabs().keySeq().first(),
        },
      });
    }
  }

  getActiveTab() {
    let { tab } = this.props;
    if (!tab) {
      // go to a default
      const first = this.getTabs().keySeq().first();
      if (first) {
        tab = first;
      }
    }
    return tab;
  }

  updateFormDirtyState = (isFormDirty) => {
    this.setState({ isFormDirty }, () =>
      this.props.updateFormDirtyState(isFormDirty)
    );
  };

  handleChangeTab = (tab) => {
    const { location } = this.props;
    const confirm = () => {
      this.context.router.push({
        ...location,
        state: { ...location.state, tab },
      });
      this.updateFormDirtyState(false);
    };
    if (this.state.isFormDirty) {
      this.props.showUnsavedChangesConfirmDialog({ confirm });
    } else {
      confirm();
    }
  };

  getFullPath = createSelector(
    (fullPathImmutable) => fullPathImmutable,
    (path) => (path ? path.toJS() : null)
  );

  renderContent() {
    const {
      hide,
      location,
      entity,
      branchName,
      arcticProjectId,
      isIcebergTable,
      compactionTasks,
      clearCompactionStateData: dispatchClearCompactionStateData,
      startCompaction: dispatchStartCompaction,
      patchCompactionData: dispatchPatchCompactionData,
      postCompactionData: dispatchPostCompactionData,
      isAdmin,
    } = this.props;

    if (!entity) {
      return null;
    }

    const commonProps = {
      onCancel: hide, // slowing getting off of needing this one
      onDone: hide,
      location,
    };

    const renderDataOptimization =
      arcticProjectId && isAdmin && isIcebergTable
        ? {
            dataOptimization: () => {
              return this.state.loadingCompactionError ? (
                <Message
                  messageType="error"
                  inFlow={false}
                  message={this.state.loadingCompactionError}
                  messageId={this.state.loadingCompactionError}
                />
              ) : (
                <DatasetCompactionForm
                  {...commonProps}
                  compactionTasks={compactionTasks}
                  spaceName={[...entity.get("fullPathList").remove(0)]}
                  entity={entity}
                  projectId={arcticProjectId}
                  dispatchClearCompactionStateData={
                    dispatchClearCompactionStateData
                  }
                  taskLoading={this.state.taskLoading}
                  dispatchStartCompaction={dispatchStartCompaction}
                  dispatchPatchCompactionData={dispatchPatchCompactionData}
                  dispatchPostCompactionData={dispatchPostCompactionData}
                  branchName={branchName}
                  updateFormDirtyState={this.updateFormDirtyState}
                />
              );
            },
          }
        : {};

    let contentRenderers = {
      format: () => {
        return (
          <FileFormatController
            onDone={hide}
            updateFormDirtyState={this.updateFormDirtyState}
            entityType={entity.get("entityType")}
            fullPath={
              entity ? this.getFullPath(entity.get("fullPathList")) : null
            }
            query={location.state.query}
          />
        );
      },
      acceleration: () => {
        return (
          <AccelerationController
            {...commonProps}
            updateFormDirtyState={this.updateFormDirtyState}
            onDone={hide}
            datasetId={entity.get("id")}
          />
        );
      },
      accelerationUpdates: () => {
        // TODO refactor - uses only: id, fullPathList, entityType
        return (
          <AccelerationUpdatesController
            updateFormDirtyState={this.updateFormDirtyState}
            entity={entity}
            {...commonProps}
          />
        );
      },
      overview: () => {
        // TODO refactor - uses only: name, fullPathList, fileType, queryable
        return <DatasetOverviewForm {...commonProps} entity={entity} />;
      },
      ...renderDataOptimization,
    };

    contentRenderers = this.extendContentRenderers(
      contentRenderers,
      commonProps
    );
    const activeTab = this.getActiveTab();
    return contentRenderers[activeTab] && contentRenderers[activeTab]();
  }

  render() {
    const { viewState } = this.props;

    return (
      <div
        style={{ ...LINE_NOROW_START_STRETCH, height: "100%" }}
        data-qa="dataset-settings"
      >
        <NavPanel
          changeTab={this.handleChangeTab}
          activeTab={this.getActiveTab()}
          tabs={this.getTabs()}
          showSingleTab
        />
        <ViewStateWrapper viewState={viewState} style={styles.wrap}>
          {this.renderContent()}
        </ViewStateWrapper>
      </div>
    );
  }
}
DatasetSettings = injectIntl(DatasetSettings);

const mapStateToProps = (state, { isHomePage }) => {
  // Loaction Related variables
  const location = state.routing.locationBeforeTransitions;
  const { entityType, entityId } = location.state || {};
  const isCloudEdition = isDcsEdition();
  // Compaction Related variables, isCloudEdition is not necessary but just incase
  const sources = isCloudEdition && getSortedSources(state);
  const nessieSources = state.nessie || {};
  const sourceName = isCloudEdition && getSourceName(nessieSources, location);
  const branchName =
    (isCloudEdition && getBranchName(nessieSources, sourceName)) || "";
  const arcticProjectId =
    (isCloudEdition && getArcticProjectId(sources, sourceName)) || null;
  const compactionTasks = state.compactionTasks;
  const isAdmin =
    (isCloudEdition && state.privileges.organization.isAdmin) || false;
  const enableCompaction =
    isCloudEdition && state.featureFlag.data_optimization === "ENABLED";
  const isIcebergTable = entityType === "physicalDataset";
  // Entity could be stored in different places of redux state, depending on current page
  // Entity is used to be stored in resources, but now for home page it is stored in separate place.
  // We need support both options. At this moment an only place where entity is stored in resources
  // is explore page ExploreSettingsButton
  const finalEntitySelector = isHomePage ? getHomeEntityOrChild : getEntity;
  return {
    arcticProjectId,
    branchName,
    compactionTasks,
    location,
    isAdmin,
    enableCompaction,
    isIcebergTable,
    entity: entityId && finalEntitySelector(state, entityId, entityType),
    viewState: getViewState(state, DATASET_SETTINGS_VIEW_ID),
  };
};

export default connect(mapStateToProps, {
  loadDatasetForDatasetType,
  showUnsavedChangesConfirmDialog,
  startCompaction,
  postCompactionData,
  getAllCompactionData,
  patchCompactionData,
  clearCompactionStateData,
})(DatasetSettings);

const styles = {
  wrap: {
    width: "100%",
    height: "100%",
    overflow: "auto", // just in case
    position: "relative", // todo: somehow makes acceleration form render buttons correctly
  },
};
