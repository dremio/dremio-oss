/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import { Component } from 'react';
import { connect } from 'react-redux';
import Immutable from 'immutable';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Radium from 'radium';
import { injectIntl } from 'react-intl';
import { LINE_NOROW_START_STRETCH } from 'uiTheme/radium/flexStyle';

import { loadDatasetForDatasetType } from 'actions/resources';

import ViewStateWrapper from 'components/ViewStateWrapper';
import { getViewState, getEntity } from 'selectors/resources';


import AccelerationController from 'components/Acceleration/AccelerationController';

import DatasetSettingsMixin from 'dyn-load/pages/HomePage/components/modals/DatasetSettings/DatasetSettingsMixin';

import { showUnsavedChangesConfirmDialog } from 'actions/confirmation';
import NavPanel from 'components/Nav/NavPanel';
import FileFormatController from './FileFormatController';
import AccelerationUpdatesController from './AccelerationUpdates/AccelerationUpdatesController';
import DatasetOverviewForm from './DatasetOverviewForm';

const DATASET_SETTINGS_VIEW_ID = 'DATASET_SETTINGS_VIEW_ID';

@injectIntl
@pureRender
@Radium
@DatasetSettingsMixin
export class DatasetSettings extends Component {
  static contextTypes = {
    router: PropTypes.object,
    location: PropTypes.object
  }

  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map),
    tab: PropTypes.string,
    datasetType: PropTypes.string,
    datasetUrl: PropTypes.string,
    location: PropTypes.object,
    viewState: PropTypes.instanceOf(Immutable.Map),
    intl: PropTypes.object.isRequired,
    hide: PropTypes.func,
    updateFormDirtyState: PropTypes.func,
    showUnsavedChangesConfirmDialog: PropTypes.func,
    loadDatasetForDatasetType: PropTypes.func.isRequired
  }

  state = {
    isFormDirty: false
  };

  componentWillMount() {
    const {datasetUrl, datasetType} = this.props;
    if (datasetUrl) {
      this.props.loadDatasetForDatasetType(
        datasetType,
        datasetUrl,
        DATASET_SETTINGS_VIEW_ID
      ).then((response) => {
        if (!response.error) {
          const entity = response.payload.get('entities');
          const entityType = entity.keySeq().first();
          const entityId = response.payload.get('result');
          const { location } = this.props;
          this.context.router.replace({
            ...location, state: {
              ...location.state,
              entityType,
              entityId
            }
          });
        }
      });
    }
  }

  componentDidMount() {
    const { location, tab } = this.props;
    if (!tab) {
      this.context.router.replace({
        ...location, state: {
          ...location.state,
          tab: this.getTabs().keySeq().first()
        }
      });
    }
  }

  getActiveTab() {
    let { tab } = this.props;
    if (!tab) { // go to a default
      const first = this.getTabs().keySeq().first();
      if (first) {
        tab = first;
      }
    }
    return tab;
  }

  updateFormDirtyState = (isFormDirty) => {
    this.setState({isFormDirty});
    this.props.updateFormDirtyState(isFormDirty);
  }

  handleChangeTab = (tab) => {
    const { location } = this.props;
    const confirm = () => {
      this.context.router.push({...location, state: {...location.state, tab}});
      this.updateFormDirtyState(false);
    };
    if (this.state.isFormDirty) {
      this.props.showUnsavedChangesConfirmDialog({ confirm });
    } else {
      confirm();
    }
  }

  renderContent() {
    const { hide, location, entity } = this.props;

    if (!entity) {
      return null;
    }

    const commonProps = {
      onCancel: hide, // slowing getting off of needing this one
      onDone: hide,
      location
    };

    let contentRenderers = {
      format: () => {
        return <FileFormatController
          onDone={hide}
          updateFormDirtyState={this.updateFormDirtyState}
          entityType={entity.get('entityType')}
          entityId={entity.get('id')}
          query={location.state.query}
        />;
      },
      acceleration: () => {
        return <AccelerationController
          {...commonProps}
          updateFormDirtyState={this.updateFormDirtyState}
          onDone={hide}
          datasetId={entity.get('id')}
        />;
      },
      accelerationUpdates: () => {
        return <AccelerationUpdatesController
          updateFormDirtyState={this.updateFormDirtyState}
          entity={entity}
          {...commonProps} />;
      },
      overview: () => {
        return <DatasetOverviewForm {...commonProps} entity={entity}/>;
      }
    };

    contentRenderers = this.extendContentRenderers(contentRenderers, commonProps);
    const activeTab = this.getActiveTab();
    return contentRenderers[activeTab] && contentRenderers[activeTab]();
  }

  render() {
    const { viewState } = this.props;

    return (
      <div style={[LINE_NOROW_START_STRETCH, {height: '100%'}]} data-qa='dataset-settings'>
        <NavPanel
          changeTab={this.handleChangeTab}
          activeTab={this.getActiveTab()}
          tabs={this.getTabs()}/>
        <ViewStateWrapper viewState={viewState} style={styles.wrap}>
          {this.renderContent()}
        </ViewStateWrapper>
      </div>
    );
  }
}

const mapStateToProps = (state) => {
  const location = state.routing.locationBeforeTransitions;
  const { entityType, entityId } = location.state || {};

  return {
    location,
    entity: entityId && getEntity(state, entityId, entityType),
    viewState: getViewState(state, DATASET_SETTINGS_VIEW_ID)
  };
};

export default connect(mapStateToProps, {
  loadDatasetForDatasetType,
  showUnsavedChangesConfirmDialog
})(DatasetSettings);

const styles = {
  wrap: {
    width: '100%',
    height: '100%',
    overflow: 'auto', // just in case
    position: 'relative' // todo: somehow makes acceleration form render buttons correctly
  }
};
