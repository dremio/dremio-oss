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
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import Immutable from 'immutable';
import ViewStateWrapper from 'components/ViewStateWrapper';
import { constructFullPath } from 'utils/pathUtils';
import {
  loadDatasetAccelerationSettings,
  updateDatasetAccelerationSettings
} from 'actions/resources/datasetAccelerationSettings';
import ApiUtils from 'utils/apiUtils/apiUtils';
import { getEntity } from 'selectors/resources';
import { INCREMENTAL_TYPES } from 'constants/columnTypeGroups';
import AccelerationUpdatesForm from './AccelerationUpdatesForm';

const VIEW_ID = 'AccelerationUpdatesController';

export class AccelerationUpdatesController extends Component {
  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map),
    onCancel: PropTypes.func,
    onDone: PropTypes.func,
    loadDatasetAccelerationSettings: PropTypes.func,
    updateDatasetAccelerationSettings: PropTypes.func,
    updateFormDirtyState: PropTypes.func,
    accelerationSettings: PropTypes.instanceOf(Immutable.Map)
  }

  state = {
    dataset: null,
    viewState: Immutable.fromJS({})
  }

  componentWillMount() {
    this.receiveProps(this.props, {});
  }

  componentWillReceiveProps(nextProps) {
    this.receiveProps(nextProps, this.props);
  }

  receiveProps(nextProps, oldProps) {
    const { entity } = nextProps;
    if (entity !== oldProps.entity) {
      const id = entity.get('id');
      this.loadDataset(id).then((dataset) => {
        this.setState({dataset, viewState: Immutable.fromJS({})});
        this.props.loadDatasetAccelerationSettings(entity, VIEW_ID);
      }).catch((e) => {
        this.setState(
          {
            viewState: Immutable.fromJS({
              isFailed: true,
              error: {
                message: la('Failed to load dataset: ' + e.statusText)
              }
            })
          }
        );
      });
    }
  }

  loadDataset(id) {
    // We fetch to the full schema using the v3 catalog api here so we can filter out types.  v2 collapses types
    // by display types instead of returning the actual type.
    return new Promise((resolve, reject) => {
      ApiUtils.fetch(`catalog/${id}`).then((response) => {
        response.json().then((dataset) => {
          resolve(dataset);
        });
      }).catch(reject);
    });
  }

  schemaToColumns(dataset) {
    const schema = (dataset && dataset.fields) || [];
    const columns = schema.filter(i => INCREMENTAL_TYPES.indexOf(i.type.name) > -1)
      .map((item, index) => {
        return {name: item.name, type: item.type.name, index};
      });
    return Immutable.fromJS(columns);
  }

  submit = (form) => {
    return ApiUtils.attachFormSubmitHandlers(
      this.props.updateDatasetAccelerationSettings(this.props.entity, form)
    ).then(() => this.props.onDone(null, true));
  }

  render() {
    const { onCancel, accelerationSettings, updateFormDirtyState } = this.props;

    const viewState = this.state.viewState;

    return (
      <ViewStateWrapper viewState={viewState} hideChildrenWhenInProgress>
        {accelerationSettings && <AccelerationUpdatesForm
          accelerationSettings={accelerationSettings}
          datasetFields={this.schemaToColumns(this.state.dataset)}
          entityType={this.props.entity.get('entityType')}
          entity={this.props.entity}
          onCancel={onCancel}
          updateFormDirtyState={updateFormDirtyState}
          submit={this.submit} />}
      </ViewStateWrapper>
    );
  }
}

function mapStateToProps(state, ownProps) {
  const { entity } = ownProps;
  const fullPath = constructFullPath(entity.get('fullPathList'));
  // TODO: this is a workaround for accelerationSettings not having its own id
  return {
    accelerationSettings: getEntity(state, fullPath, 'datasetAccelerationSettings')
  };
}

export default connect(mapStateToProps, {
  loadDatasetAccelerationSettings,
  updateDatasetAccelerationSettings
})(AccelerationUpdatesController);
