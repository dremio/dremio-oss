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
import Immutable from "immutable";
import { ReactNode, useEffect } from "react";
import { connect } from "react-redux";
import { withRouter, WithRouterProps } from "react-router";
import { loadSummaryDataset } from "actions/resources/dataset";
import { getSummaryDataset } from "selectors/datasets";
import { getViewState } from "selectors/resources";
import { constructFullPath } from "#oss/utils/pathUtils";
import DatasetSummary from "../DatasetSummary/DatasetSummary";
import DatasetSummaryError from "../DatasetSummary/DatasetSummaryError/DatasetSummaryError";
import { VersionContextType } from "dremio-ui-common/components/VersionContext.js";
import { getVersionContextFromId } from "dremio-ui-common/utilities/datasetReference.js";

const VIEW_ID = "SummaryDataset";

type DatasetSummaryOverlayProps = {
  fullPath: Immutable.List<string>;
  datasetType?: string;
  summaryDataset: Immutable.Map<any, any>;
  inheritedTitle?: string;
  viewState: Immutable.Map<any, any>;
  loadSummaryDataset: typeof loadSummaryDataset;
  detailsView?: boolean;
  tagsComponent?: ReactNode;
  openWikiDrawer: (dataset: any) => void;
  showColumns?: boolean;
  hideSqlEditorIcon?: boolean;
  versionContext?: VersionContextType;
  isPanel?: boolean;
  hideGoToButton?: boolean;
  handlePanelDetails?: (dataset: any) => void;
  hideMainActionButtons?: boolean;
};

const DatasetSummaryOverlay = (
  props: DatasetSummaryOverlayProps & WithRouterProps,
) => {
  const {
    fullPath,
    summaryDataset,
    datasetType,
    viewState,
    inheritedTitle,
    location,
    loadSummaryDataset: dispatchLoadSummaryDataset,
    detailsView,
    tagsComponent,
    openWikiDrawer,
    showColumns,
    hideSqlEditorIcon,
    versionContext,
    isPanel,
    hideGoToButton,
    handlePanelDetails,
    hideMainActionButtons,
  } = props;

  const { type: contextType, value: contextValue } = versionContext ?? {};

  useEffect(() => {
    dispatchLoadSummaryDataset(
      fullPath,
      VIEW_ID,
      undefined,
      undefined,
      undefined,
      contextType && contextValue
        ? { value: contextValue, type: contextType }
        : undefined,
    );
  }, [dispatchLoadSummaryDataset, fullPath, contextType, contextValue]);

  useEffect(() => {
    if (
      summaryDataset.size &&
      contextValue ===
        getVersionContextFromId(summaryDataset.get("entityId"))?.value
    ) {
      handlePanelDetails?.(summaryDataset);
    }
    if (viewState.get("isFailed"))
      handlePanelDetails?.(Immutable.fromJS({ error: true }));
  }, [summaryDataset, handlePanelDetails, viewState, contextValue]);

  const title = fullPath?.get(fullPath.size - 1);
  const constructedFullPath = constructFullPath(fullPath);
  const is403 =
    viewState.get("error") && viewState.getIn(["error", "status"]) === 403;
  const showError = viewState.get("isFailed");
  const disableActionButtons = viewState.get("isInProgress");
  return showError ? (
    <DatasetSummaryError
      is403={is403}
      fullPath={fullPath}
      title={inheritedTitle}
      datasetType={datasetType}
      versionContext={versionContext}
      isOverlay={!detailsView}
    />
  ) : (
    <DatasetSummary
      location={location}
      title={title}
      disableActionButtons={disableActionButtons}
      fullPath={constructedFullPath}
      dataset={summaryDataset}
      detailsView={detailsView}
      tagsComponent={tagsComponent}
      openWikiDrawer={openWikiDrawer}
      showColumns={showColumns}
      hideSqlEditorIcon={hideSqlEditorIcon}
      versionContext={versionContext}
      isPanel={isPanel}
      hideGoToButton={hideGoToButton}
      hideMainActionButtons={hideMainActionButtons}
    />
  );
};

const mapStateToProps = (
  state: Record<string, any>,
  props: { fullPath: string[] },
) => {
  const fullPath = props.fullPath?.join(",");
  return {
    summaryDataset: getSummaryDataset(state, fullPath),
    viewState: getViewState(state, VIEW_ID),
  };
};

export default withRouter(
  connect(mapStateToProps, { loadSummaryDataset })(DatasetSummaryOverlay),
);
