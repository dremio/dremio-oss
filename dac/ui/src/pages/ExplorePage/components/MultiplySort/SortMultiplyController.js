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
import Immutable from 'immutable';

import SortMultiply from './SortMultiply';

const DEFAULT_DIRECTION = 'DESC';

export default class SortMultiplyController extends Component {
  static propTypes = {
    fields: PropTypes.object,
    dataset: PropTypes.instanceOf(Immutable.Map),
    columns: PropTypes.instanceOf(Immutable.List),
    defaultItem: PropTypes.instanceOf(Immutable.Map),
    location: PropTypes.object,
    handleModelChange: PropTypes.func
  };

  constructor(props) {
    super(props);

    this.state = {
      isDragInProgress: false
    };
  }

  componentDidMount() {
    this.initModel();
  }

  handleDrop = (type, dropData) => {
    const { fields } = this.props;
    const columnName = dropData.id;

    if (!dropData.index && dropData.index !== 0) {
      const newColumn = {
        name: columnName,
        direction: DEFAULT_DIRECTION
      };

      fields.sortColumns.addField(newColumn);
      this.handleDragStop();
    }
  }

  handleDragStart = () => this.setState({ isDragInProgress: true });

  handleDragStop = () => this.setState({ isDragInProgress: false });

  addAnother = () => {
    const { fields } = this.props;
    fields.sortColumns.addField({direction: DEFAULT_DIRECTION});
  }

  initModel() {
    if (this.props.location.state.columnName) {
      const item = {
        name: this.props.location.state.columnName,
        direction: DEFAULT_DIRECTION
      };
      this.props.fields.sortColumns.addField(item);
    }
  }

  render() {
    return (
      <SortMultiply
        columnsField={this.props.fields.sortColumns}
        dataset={this.props.dataset}
        isDragInProgress={this.state.isDragInProgress}
        handleDragStart={this.handleDragStart}
        columns={this.props.columns}
        dragType='groupBy'
        handleDragStop={this.handleDragStop}
        handleDrop={this.handleDrop}
        addAnother={this.addAnother}
      />
    );
  }
}
