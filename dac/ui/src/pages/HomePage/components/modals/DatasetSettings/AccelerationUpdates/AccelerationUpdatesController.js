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
import { Component } from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import Immutable from 'immutable';
import {createSelector} from 'reselect';

import ViewStateWrapper from '@app/components/ViewStateWrapper';
import {constructFullPath} from '@app/utils/pathUtils';
import {getEntity, getViewState} from '@app/selectors/resources';
import {updateViewState} from '@app/actions/resources';
import {
  loadDatasetAccelerationSettings,
  updateDatasetAccelerationSettings
} from '@app/actions/resources/datasetAccelerationSettings';
import ApiUtils from '@app/utils/apiUtils/apiUtils';
import {formatMessage} from '@app/utils/locale';
import { INCREMENTAL_TYPES } from '@app/constants/columnTypeGroups';
import {getCurrentFormatUrl} from '@app/selectors/home';
import {loadFileFormat} from '@app/actions/modals/addFileModal';

import AccelerationUpdatesForm from './AccelerationUpdatesForm';

const VIEW_ID = 'AccelerationUpdatesController';
const updateViewStateWrapper = viewState => updateViewState(VIEW_ID, {
  // apply defaults
  isInProgress: false,
  isFailed: false,
  error: null,
  //----------------
  ...viewState
});

export class AccelerationUpdatesController extends Component {
  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map),
    fileFormat: PropTypes.instanceOf(Immutable.Map),
    formatUrl: PropTypes.string,
    viewState: PropTypes.instanceOf(Immutable.Map),
    onCancel: PropTypes.func,
    onDone: PropTypes.func,
    loadFileFormat: PropTypes.func,
    loadDatasetAccelerationSettings: PropTypes.func,
    updateDatasetAccelerationSettings: PropTypes.func,
    updateFormDirtyState: PropTypes.func,
    accelerationSettings: PropTypes.instanceOf(Immutable.Map),
    updateViewState: PropTypes.func.isRequired // (viewState) => void
  };

  state = {
    dataset: null
  };

  componentWillMount() {
    this.receiveProps(this.props, {});
  }

  componentWillReceiveProps(nextProps) {
    if (!this.props.formatUrl) {
      this.loadFormat(nextProps);
    }
    this.receiveProps(nextProps, this.props);
  }

  componentDidMount() {
    this.loadFormat(this.props);
  }

  loadFormat(props) {
    const { formatUrl } = props;
    if (formatUrl) {
      props.loadFileFormat(formatUrl, VIEW_ID);
    }
  }

  receiveProps(nextProps, oldProps) {
    const { entity } = nextProps;
    if (entity !== oldProps.entity) {
      const id = entity.get('id');
      this.loadDataset(id, entity);
    }
  }

  loadDataset(id, entity) {
    const updateVS = this.props.updateViewState;

    // We fetch to the full schema using the v3 catalog api here so we can filter out types.  v2 collapses types
    // by display types instead of returning the actual type.
    updateVS({ isInProgress: true });
    return ApiUtils.fetchJson(`catalog/${id}`, json => {
      updateVS({isInProgress: false});
      this.setState({dataset: json});
      this.props.loadDatasetAccelerationSettings(entity.get('fullPathList'), VIEW_ID);
    }, error => {
      // DX-22985: Server might return a valid error message.
      //    - In case it does, we need to extract it out of the JSON and display the message.
      //    - If not we need to display a generic error message.
      error.json().then(json => updateVS({
        isFailed: true,
        error: { message: json.errorMessage }
      })).catch(jsonError => updateVS({
        // JSON parsing failed. So we are displaying a generic Error Message.
        isFailed: true,
        error: {
          message: formatMessage('Message.ApiErr.Load.Dataset', {err: jsonError.statusText})
        }
      }));
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
      this.props.updateDatasetAccelerationSettings(this.props.entity.get('fullPathList'), form)
    ).then(() => this.props.onDone(null, true));
  };

  render() {
    const {
      onCancel,
      accelerationSettings,
      updateFormDirtyState,
      fileFormat,
      viewState,
      entity
    } = this.props;

    let fileFormatType = '';
    if (fileFormat) {
      fileFormatType = fileFormat.get('type');
    }

    return (
      <ViewStateWrapper viewState={viewState} hideChildrenWhenInProgress>
        {accelerationSettings && <AccelerationUpdatesForm
          accelerationSettings={accelerationSettings}
          datasetFields={this.schemaToColumns(this.state.dataset)}
          entityType={entity.get('entityType')}
          fileFormatType={fileFormatType}
          entityId={entity.get('id')}
          onCancel={onCancel}
          updateFormDirtyState={updateFormDirtyState}
          submit={this.submit} />}
      </ViewStateWrapper>
    );
  }
}



function mapStateToProps(state, ownProps) {
  const { entity } = ownProps;
  const fullPathList = entity.get('fullPathList');
  const fullPath = constructFullPath(fullPathList);
  // TODO: this is a workaround for accelerationSettings not having its own id

  const entityType = entity.get('entityType');

  // If entity type is folder, then get fileFormat. It will be used to disable incremental reflection option.
  let formatUrl, fileFormat;
  if (['folder'].indexOf(entityType) !== -1) {
    const getFullPathForFileFormat = createSelector(fullPathImmutable => fullPathImmutable, path => path ? path.toJS() : null);
    formatUrl = fullPathList ? getCurrentFormatUrl(getFullPathForFileFormat(fullPathList), true) : null;
    fileFormat = getEntity(state, formatUrl, 'fileFormat');
  }

  return {
    fileFormat,
    formatUrl,
    viewState: getViewState(state, VIEW_ID),
    accelerationSettings: getEntity(state, fullPath, 'datasetAccelerationSettings')
  };
}

export default connect(mapStateToProps, {
  loadFileFormat,
  loadDatasetAccelerationSettings,
  updateDatasetAccelerationSettings,
  updateViewState: updateViewStateWrapper
})(AccelerationUpdatesController);
