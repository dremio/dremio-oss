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
import Immutable from 'immutable';
import Radium from 'radium';

import PropTypes from 'prop-types';

import { CONTAINER_ENTITY_TYPES } from 'constants/Constants';
import { PHYSICAL_DATASET_TYPES } from 'constants/datasetTypes';
import ExistingForm from 'components/formsForAddData/ExistingForm';
import { constructFullPath, splitFullPath } from 'utils/pathUtils';
import InnerJoinController from './JoinTypes/InnerJoinController';

@Radium
export default class CustomJoin extends Component {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    joinStep: PropTypes.number,
    location: PropTypes.object,
    fields: PropTypes.object,
    leftColumns: PropTypes.object,
    rightColumns: PropTypes.object,
    style: PropTypes.object,
    clearJoinDataset: PropTypes.func
  };

  static contextTypes = {
    router: PropTypes.object
  }

  constructor(props) {
    super(props);
    this.selectDataset = this.selectDataset.bind(this);
  }

  selectDataset(name, data) {
    if (!name || !data) {
      return null;
    }
    if (CONTAINER_ENTITY_TYPES.has(data.get('type'))) {
      return null;
    }

    const isPhysicalDataset = PHYSICAL_DATASET_TYPES.has(data.get('type'));
    const fullPath = data.get('fullPath') && constructFullPath(data.get('fullPath'));

    if (!fullPath && !isPhysicalDataset) {
      return null;
    }

    this.props.fields.activeDataset.onChange(fullPath && splitFullPath(fullPath));
  }

  renderDatasetsTree() {
    return (
      <ExistingForm
        noNameField
        changeSelectedNode={this.selectDataset}
        style={this.props.style}
      />
    );
  }

  renderJoinCustomTable() {
    return (
      this.props.rightColumns.size !== 0 && (
        <InnerJoinController
          dataset={this.props.dataset}
          location={this.props.location}
          fields={this.props.fields}
          rightColumns={this.props.rightColumns}
          leftColumns={this.props.leftColumns}
          style={this.props.style}
        />
      )
    );
  }

  render() {
    return this.props.joinStep === 2
      ? this.renderJoinCustomTable()
      : this.renderDatasetsTree();
  }
}
