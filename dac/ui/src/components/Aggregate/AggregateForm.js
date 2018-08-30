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
import Radium from 'radium';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import { WHITE } from 'uiTheme/radium/colors';
import { getColumnByName, isAlreadySelected } from 'utils/explore/aggregateUtils';
import ColumnDragItem from 'utils/ColumnDragItem';

import AggregateContent from './AggregateContent';
import AggregateHeader from './AggregateHeader';
import AggregateFooter from './AggregateFooter';

@Radium
class AggregateForm extends Component {
  static getFields = () => ([
    'columnsDimensions[].column',
    'columnsMeasures[].measure',
    'columnsMeasures[].column'
  ])

  static validate = () => {
    return {};
  }

  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map), // NOTE: some users pass a fake DS with just displayFullPath
    fields: PropTypes.object,
    values: PropTypes.object,
    columns: PropTypes.instanceOf(Immutable.List),
    canSelectMeasure: PropTypes.bool,
    // when `false` field can be selected only once
    canUseFieldAsBothDimensionAndMeasure: PropTypes.bool,
    location: PropTypes.object,
    style: PropTypes.object,
    footerStyle: PropTypes.object,
    contentStyle: PropTypes.object,
    headerStyle: PropTypes.object
  };

  static defaultProps = {
    canSelectMeasure: true,
    canUseFieldAsBothDimensionAndMeasure: true
  }

  constructor(props) {
    super(props);
    this.state = {
      isDragInProgress: false,
      dragItem: new ColumnDragItem()
    };
  }

  onDragStart = (e = {}) => {
    this.setState({
      isDragInProgress: true,
      dragItem: new ColumnDragItem(e.id, e.type)
    });
  }

  handleDrop = (dragOrigin, dropData) => {
    const {columns, fields} = this.props;
    const { columnsMeasures, columnsDimensions } = fields;
    const { id: columnName } = dropData;
    const columnToAdd = getColumnByName(columns, columnName);
    const newColumn = {
      column: columnName
    };

    if (dragOrigin !== dropData.type) {
      if (dragOrigin === 'measures') {
        if (dropData.type === 'dimensions') {
          columnsDimensions.removeField(dropData.index);
        }
        newColumn.measure = ['FLOAT', 'DECIMAL', 'INTEGER', 'BIGINT', 'DOUBLE'].includes(columnToAdd && columnToAdd.getIn(['type', 'name']))
          ? 'Sum' : 'Count';
        columnsMeasures.addField(newColumn);
      } else if (!isAlreadySelected(fields.columnsDimensions, columnName)) {
        if (dropData.type === 'measures') {
          columnsMeasures.removeField(dropData.index);
        }
        columnsDimensions.addField(newColumn);
      }
    }


    this.stopDrag();
  }

  addAnother = (type) => {
    const { fields } = this.props;
    if (type === 'measures') {
      fields.columnsMeasures.addField({measure: 'Sum'});
    } else {
      fields.columnsDimensions.addField({});
    }
  }

  // TODO: ugly hack, we run into timing issues where columsn are re-added as we are removing since its atomic removal
  handleClearAllDimensions = () => {
    const { fields: { columnsDimensions } } = this.props;

    const target = columnsDimensions.length;
    let count = 0;

    function doit() {
      columnsDimensions.removeField().then(() => {
        count++;

        if (count < target) {
          doit();
        }
      });
    }

    doit();
  }

  // TODO: ugly hack, we run into timing issues where columsn are re-added as we are removing since its atomic removal
  handleClearAllMeasures = () => {
    const { fields: { columnsMeasures } } = this.props;

    const target = columnsMeasures.length;
    let count = 0;

    function doit() {
      columnsMeasures.removeField().then(() => {
        count++;

        if (count < target) {
          doit();
        }
      });
    }

    doit();
  }

  stopDrag = () => {
    this.setState({
      isDragInProgress: false,
      dragItem: new ColumnDragItem()
    });
  }

  render() {
    return (
      <div className='aggregate-form' style={[styles.base, this.props.style]}>
        <AggregateHeader
          style={this.props.headerStyle}
          dataset={this.props.dataset}
          onClearAllDimensions={this.handleClearAllDimensions}
          onClearAllMeasures={this.handleClearAllMeasures}
        />
        <AggregateContent
          fields={this.props.fields}
          values={this.props.values}
          canSelectMeasure={this.props.canSelectMeasure}
          canUseFieldAsBothDimensionAndMeasure={this.props.canUseFieldAsBothDimensionAndMeasure}
          style={this.props.contentStyle}
          handleDragStart={this.onDragStart}
          onDragEnd={this.stopDrag}
          onDrop={this.handleDrop}
          isDragInProgress={this.state.isDragInProgress}
          dragItem={this.state.dragItem}
          dragType='aggregate'
          allColumns={this.props.columns}/>
        <AggregateFooter
          style={this.props.footerStyle}
          addAnother={this.addAnother}
        />
      </div>
    );
  }
}

export default AggregateForm;

const styles = {
  base: {
    display: 'flex',
    flexWrap: 'wrap',
    flexGrow: 1,
    backgroundColor: WHITE,
    overflow: 'hidden'
  }
};
