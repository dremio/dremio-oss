/*
 * Copyright (C) 2017 Dremio Corporation
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
import { Component, PropTypes } from 'react';
import { connect } from 'react-redux';
import Immutable from 'immutable';
import ViewStateWrapper from 'components/ViewStateWrapper';
import { constructFullPath } from 'utils/pathUtils';
import {
  loadDatasetAccelerationSettings, updateDatasetAccelerationSettings
} from 'actions/resources/datasetAccelerationSettings';
import ApiUtils from 'utils/apiUtils/apiUtils';
import { loadSummaryDataset } from 'actions/resources/dataset';
import { getSummaryDataset } from 'selectors/datasets';
import { getViewState, getEntity } from 'selectors/resources';
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
    loadSummaryDataset: PropTypes.func,
    updateFormDirtyState: PropTypes.func,
    summaryDataset: PropTypes.instanceOf(Immutable.Map),
    accelerationSettings: PropTypes.instanceOf(Immutable.Map),
    viewState: PropTypes.instanceOf(Immutable.Map)
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
      this.props.loadSummaryDataset(entity.get('fullPathList').join('/'), VIEW_ID).then(() => {
        this.props.loadDatasetAccelerationSettings(entity, VIEW_ID);
      });
    }
  }

  schemaToColumns(dataset) {
    const schema = dataset.get('fields') || Immutable.List();
    const columns = schema.toJS().filter(i => INCREMENTAL_TYPES.indexOf(i.type) > -1)
      .map((item, index) => {
        const { name, type } = item;
        return {name, type, index};
      });
    return Immutable.fromJS(columns);
  }

  submit = (form) => {
    return ApiUtils.attachFormSubmitHandlers(
      this.props.updateDatasetAccelerationSettings(this.props.entity, form)
    ).then(() => this.props.onDone(null, true));
  }

  render() {
    const { onCancel, accelerationSettings, summaryDataset, updateFormDirtyState } = this.props;
    return (
      <ViewStateWrapper viewState={this.props.viewState} hideChildrenWhenInProgress>
        {accelerationSettings && <AccelerationUpdatesForm
          accelerationSettings={accelerationSettings}
          datasetFields={this.schemaToColumns(summaryDataset)}
          entityType={this.props.entity.get('entityType')}
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
    accelerationSettings: getEntity(state, fullPath, 'datasetAccelerationSettings'),
    summaryDataset: getSummaryDataset(state, entity.get('fullPathList').join(',')),
    viewState: getViewState(state, VIEW_ID)
  };
}

export default connect(mapStateToProps, {
  loadDatasetAccelerationSettings,
  updateDatasetAccelerationSettings,
  loadSummaryDataset
})(AccelerationUpdatesController);
