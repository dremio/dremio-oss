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

import { Component } from "react";
import { compose } from "redux";
import { connect } from "react-redux";
import Immutable from "immutable";
//@ts-ignore
import { intl } from "@app/utils/intl";
import { withRouter, WithRouterProps } from "react-router";
import ViewStateWrapper from "components/ViewStateWrapper";
import { getViewState } from "selectors/resources";
import { showUnsavedChangesConfirmDialog } from "actions/confirmation";
//@ts-ignore
import Message from "@app/components/Message";
import NavPanel from "components/Nav/NavPanel";
//@ts-ignore
import localStorageUtils from "@inject/utils/storageUtils/localStorageUtils";
//@ts-ignore
import DatasetCompactionForm from "@inject/pages/HomePage/components/modals/DatasetSettings/DatasetCompaction/DatasetCompactionForm";
//@ts-ignore
import {
  postCompactionData,
  getAllCompactionData,
  patchCompactionData,
  startCompaction,
  clearCompactionStateData,
} from "@inject/actions/resources/compaction";

import * as classes from "./NessieDatasetSettings.module.less";

type NessieDatasetSettingsProps = {
  hide: (arg0?: any, arg1?: boolean) => any;
  entityName: string[];
  branchName: string;
  viewState: boolean;
  arcticProjectId: string;
  compactionTasks: Record<string, any>[];
  updateFormDirtyState: (dirty: boolean) => void;
  showUnsavedChangesConfirmDialog: (arg: any) => void;
  getAllCompactionData: typeof getAllCompactionData;
  clearCompactionStateData: typeof clearCompactionStateData;
  startCompaction: typeof startCompaction;
  patchCompactionData: typeof patchCompactionData;
  postCompactionData: typeof postCompactionData;
  tab?: string | unknown;
};

const NESSIE_DATASET_SETTINGS_VIEW_ID = "NESSIE_DATASET_SETTINGS_VIEW_ID";
const COMPACTION = "COMPACTION";

export class NessieDatasetSettings extends Component<
  NessieDatasetSettingsProps & WithRouterProps,
  any
> {
  state = {
    isFormDirty: false,
    loadingTask: true,
    loadingCompactionError: null,
  };

  tabs = Immutable.OrderedMap([
    ["dataOptimization", intl.formatMessage({ id: "Data.Optimization" })],
  ]);

  componentDidMount() {
    const {
      location,
      tab,
      arcticProjectId,
      getAllCompactionData: dispatchGetAllCompactionData,
      branchName,
      entityName,
      router,
    } = this.props;

    dispatchGetAllCompactionData({
      projectId: arcticProjectId,
      filter: `tableIdentifier=='${entityName.join(
        "."
      )}'&&ref=='${branchName}'&&actionType=='${COMPACTION}'`,
      maxResults: "1",
    })
      .then(() =>
        this.setState({
          loadingTask: false,
        })
      )
      .catch((error: any) => {
        this.setState({
          loadingTask: false,
          loadingCompactionError: error.errorMessage,
        });
      });

    if (!tab) {
      router.replace({
        ...location,
        state: {
          ...location.state,
          tab: this.tabs.keySeq().first(),
        },
      });
    }
  }

  getActiveTab() {
    let { tab } = this.props;
    if (!tab) {
      // go to a default
      const first = this.tabs.keySeq().first();
      if (first) {
        tab = first;
      }
    }
    return tab;
  }

  updateFormDirtyState = (isFormDirty: boolean) => {
    this.setState({ isFormDirty }, () =>
      this.props.updateFormDirtyState(isFormDirty)
    );
  };

  handleChangeTab = (tab: string) => {
    const { location, router } = this.props;
    const confirm = () => {
      router.push({
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

  renderContent() {
    const {
      hide,
      location,
      entityName,
      branchName,
      arcticProjectId,
      compactionTasks,
      updateFormDirtyState,
      clearCompactionStateData: dispatchClearCompactionStateData,
      startCompaction: dispatchStartCompaction,
      patchCompactionData: dispatchPatchCompactionData,
      postCompactionData: dispatchPostCompactionData,
    } = this.props;

    const contentRenderers = {
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
            onCancel={hide}
            onDone={hide}
            location={location}
            projectId={arcticProjectId}
            dispatchClearCompactionStateData={dispatchClearCompactionStateData}
            dispatchStartCompaction={dispatchStartCompaction}
            dispatchPatchCompactionData={dispatchPatchCompactionData}
            dispatchPostCompactionData={dispatchPostCompactionData}
            branchName={branchName}
            loadingTask={this.state.loadingTask}
            updateFormDirtyState={updateFormDirtyState}
            compactionTasks={compactionTasks}
            spaceName={entityName}
          />
        );
      },
    };

    const activeTab = this.getActiveTab();
    //@ts-ignore
    return contentRenderers[activeTab] && contentRenderers[activeTab]();
  }

  render() {
    const { viewState } = this.props;
    return (
      <div className={classes["container"]} data-qa="dataset-settings">
        <NavPanel
          changeTab={this.handleChangeTab}
          activeTab={this.getActiveTab()}
          tabs={this.tabs}
          showSingleTab
        />
        <ViewStateWrapper viewState={viewState} className={classes["wrap"]}>
          {this.renderContent()}
        </ViewStateWrapper>
      </div>
    );
  }
}

const mapStateToProps = (state: any) => {
  //@ts-ignore
  const project = localStorageUtils && localStorageUtils.getProjectContext();
  // Loaction Related variables
  const location = state.routing.locationBeforeTransitions;
  const { entityType, entityName } = location.state || {};
  const sourceName = project && project.name;
  const branchName = sourceName && state.nessie[sourceName]?.reference?.name;
  const arcticProjectId = project && project.id;
  const compactionTasks = state.compactionTasks;

  return {
    arcticProjectId,
    branchName,
    compactionTasks,
    location,
    entityType,
    entityName,
    viewState: getViewState(state, NESSIE_DATASET_SETTINGS_VIEW_ID),
  };
};

export default compose(
  withRouter,
  connect(mapStateToProps, {
    showUnsavedChangesConfirmDialog,
    startCompaction,
    postCompactionData,
    getAllCompactionData,
    patchCompactionData,
    clearCompactionStateData,
  })
)(NessieDatasetSettings);
