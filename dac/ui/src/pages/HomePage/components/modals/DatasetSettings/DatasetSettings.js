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
import { selectSourceByName } from "@app/selectors/home";
import { getDataset } from "@app/selectors/explore";
import { loadDatasetForDatasetType } from "actions/resources";
import Message from "@app/components/Message";
import ViewStateWrapper from "components/ViewStateWrapper";
import { getViewState, getEntity } from "selectors/resources";
import { getHomeEntityOrChild } from "@app/selectors/home";
import AccelerationController from "components/Acceleration/AccelerationController";
import DatasetSettingsMixin from "dyn-load/pages/HomePage/components/modals/DatasetSettings/DatasetSettingsMixin";
import { showUnsavedChangesConfirmDialog } from "actions/confirmation";
import { fetchSupportFlags } from "@app/actions/supportFlags";
import NavPanel from "components/Nav/NavPanel";
import FileFormatController from "./FileFormatController";
import AccelerationUpdatesController from "./AccelerationUpdates/AccelerationUpdatesController";
import DatasetOverviewForm from "./DatasetOverviewForm";
import { ARCTIC } from "@app/constants/sourceTypes";
import { rmProjectBase } from "dremio-ui-common/utilities/projectBase.js";
import { isCommunity } from "dyn-load/utils/versionUtils";
import config from "@inject/utils/config";
import { REFLECTION_ARCTIC_ENABLED } from "@app/exports/endpoints/SupportFlags/supportFlagConstants";
import { TableOptimizationWrapper } from "@inject/pages/ArcticCatalog/components/ArcticCatalogDataItemSettings/ArcticTableOptimization/TableOptimizationWrapper/TableOptimizationWrapper";
import ArcticEntityPrivilegesWrapper from "@inject/pages/ArcticCatalog/components/ArcticCatalogDataItemSettings/ArcticEntityPrivileges/ArcticEntityPrivilegesWrapper/ArcticEntityPrivilegesWrapper";

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
    source: PropTypes.object,
    enableCompaction: PropTypes.bool,
    isAdmin: PropTypes.bool,
    fetchSupportFlags: PropTypes.func,
    isDatasetReflectionPage: PropTypes.bool,
  };

  state = {
    isFormDirty: false,
    loadingTask: true,
    loadingCompactionError: null,
  };

  UNSAFE_componentWillMount() {
    const { datasetUrl, datasetType } = this.props;

    if (datasetUrl) {
      return this.props
        .loadDatasetForDatasetType(
          datasetType,
          datasetUrl,
          DATASET_SETTINGS_VIEW_ID,
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
      entity,
      source,
      enableCompaction,
      fetchSupportFlags,
    } = this.props;

    if (!isCommunity?.()) {
      fetchSupportFlags?.(REFLECTION_ARCTIC_ENABLED);
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
      this.props.updateFormDirtyState(isFormDirty),
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
    (path) => (path ? path.toJS() : null),
  );

  renderContent() {
    const { hide, location, entity, source } = this.props;

    if (!entity) {
      return null;
    }

    const commonProps = {
      onCancel: hide, // slowing getting off of needing this one
      onDone: hide,
      location,
    };

    const renderDataOptimizationForArctic =
      source?.type === ARCTIC && entity.get("entityType") === "physicalDataset"
        ? {
            dataOptimization: () => {
              return <TableOptimizationWrapper tableId={entity.get("id")} />;
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
        return (
          <DatasetOverviewForm
            {...commonProps}
            entity={entity}
            source={source}
          />
        );
      },
      ...renderDataOptimizationForArctic,

      entityPrivileges: () => {
        return (
          <ArcticEntityPrivilegesWrapper
            entity={entity}
            setChildDirtyState={this.updateFormDirtyState}
          />
        );
      },
    };

    contentRenderers = this.extendContentRenderers(
      contentRenderers,
      commonProps,
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
  const { version } = location.query || {};
  const loc = rmProjectBase(location.pathname || "");
  const sourceName = loc.split("/")[2] || "";
  const source = selectSourceByName(sourceName)(state);
  const isAdmin = state.privileges?.organization?.isAdmin;
  // Entity could be stored in different places of redux state, depending on current page
  // Entity is used to be stored in resources, but now for home page it is stored in separate place.
  // We need support both options. At this moment an only place where entity is stored in resources
  // is explore page ExploreSettingsButton
  const finalEntitySelector = isHomePage ? getHomeEntityOrChild : getEntity;

  let supportFlags = state.supportFlags;
  if (isCommunity?.()) {
    supportFlags = {
      [REFLECTION_ARCTIC_ENABLED]: config.arcticReflectionsEnabled,
    };
  }

  return {
    source,
    location,
    isAdmin,
    entity: entityId
      ? finalEntitySelector(state, entityId, entityType)
      : getDataset(state, version),
    viewState: getViewState(state, DATASET_SETTINGS_VIEW_ID),
    supportFlags,
  };
};

export default connect(mapStateToProps, {
  loadDatasetForDatasetType,
  showUnsavedChangesConfirmDialog,
  fetchSupportFlags,
})(DatasetSettings);

const styles = {
  wrap: {
    width: "100%",
    height: "100%",
    overflow: "auto", // just in case
    position: "relative", // todo: somehow makes acceleration form render buttons correctly
  },
};
