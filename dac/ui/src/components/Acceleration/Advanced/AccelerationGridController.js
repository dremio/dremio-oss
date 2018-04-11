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
import { formLabel } from 'uiTheme/radium/typography';
import { WHITE } from 'uiTheme/radium/colors';
import FontIcon from 'components/Icon/FontIcon';

import AccelerationGridControllerMixin from 'dyn-load/components/Acceleration/Advanced/AccelerationGridControllerMixin';

import AccelerationGrid from './AccelerationGrid';
import CellPopover from './CellPopover';

// AccelerationGridController is used for behavior definition cells on Raw/Aggregation
// Aggregation:
//   User can enable one or both of 'Dimension' or 'Measure' per field
//   If user didn't enable 'Dimension' field but enables sort/partition/distribution field, we should enable 'Dimension'.
//   If user disabled 'Dimension' field, we should disable sort/partition/distribution fields.
// Raw:
//   If user doesn't enable 'Display' field but enables sort/partition/distribution field, we should enable 'Display' field.
//   If user disabled 'Display' field, we should disable sort/partition/distribution fields.
// -------------------------------------------------------------------------
// In Aggregate mode: at least one dimension + measure per layout is required
// In Raw mode: at least one display per layout is required

@AccelerationGridControllerMixin
@Radium
export default class AccelerationGridController extends Component {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    reflections: PropTypes.instanceOf(Immutable.Map).isRequired,
    layoutFields: PropTypes.array,
    activeTab: PropTypes.string
  };

  state = {
    filter: '',
    currentCell: {
      labelCell: '',
      columnIndex: null,
      rowIndex: null
    },
    anchorEl: null
  };

  onFilterChange = (filter) => {
    this.setState({
      filter
    });
  }

  getRowByIndex = (rowIndex) => {
    const allColumns = this.props.dataset.get('fields');
    const datasetSchema = this.filterFieldList(allColumns).toJS();
    return datasetSchema.find((row, i) => i === rowIndex);
  };

  filterFieldList = (allColumns) => {
    const { filter } = this.state;
    return allColumns.filter((item) => item.get('name').toLowerCase().includes(filter.trim().toLowerCase()));
  }

  findCurrentIndexInFieldsList = (currentColumn, field, currentColumnIndex) => {
    const { columnIndex } = this.state.currentCell;
    const colIndex = currentColumnIndex || currentColumnIndex === 0 ? currentColumnIndex : columnIndex;

    if (!this.props.layoutFields[colIndex] || !this.props.layoutFields[colIndex][field]) {
      return -1;
    }

    return this.props.layoutFields[colIndex][field].findIndex((col) => {
      if (col.name && col.name.value === currentColumn.name) {
        return true;
      }
    });
  }

  findCurrentColumnInLayouts = (field, rowIndex, columnIndex) => {
    const { layoutFields, dataset } = this.props;
    const allColumns = dataset.get('fields');
    const datasetSchema = this.filterFieldList(allColumns).toJS();
    const currentColumn = datasetSchema.find((column, i) => i === rowIndex);

    return layoutFields[columnIndex]
      && layoutFields[columnIndex][field].find(col => col.name.value === currentColumn.name);
  }

  removeFieldByIndex = (currentColumn, field, columnIndex) => {
    const index = this.findCurrentIndexInFieldsList(currentColumn, field, columnIndex);
    if (index !== -1) {
      this.props.layoutFields[columnIndex][field].removeField(index);
    }
  };

  addFieldByIndex = (currentColumn, field, columnIndex) => {
    const index = this.findCurrentIndexInFieldsList(currentColumn, field, columnIndex);
    if (index === -1) {
      this.props.layoutFields[columnIndex][field].addField(currentColumn);
    }
  }

  handleOpenPopover = (e, currentCell) => this.setState({ currentCell, anchorEl: e.currentTarget })

  handleRequestClose = () => this.setState({
    anchorEl: null,
    currentCell: {
      labelCell: '',
      field: '',
      columnIndex: null,
      rowIndex: null
    }
  });

  toggleField = (field, columnIndex, rowIndex) => {
    const { layoutFields } = this.props;
    const currentRow = this.getRowByIndex(rowIndex);
    const isFieldSelected = this.findCurrentColumnInLayouts(field, rowIndex, columnIndex);
    if (isFieldSelected) {
      this.removeFieldByIndex(currentRow, field, columnIndex);
    } else {
      layoutFields[columnIndex][field].addField(currentRow);
    }
  };

  toggleSortField = (field, columnIndex, rowIndex) => {
    const { layoutFields } = this.props;
    const currentField = this.state.currentCell.field;
    const currentRow = this.getRowByIndex(rowIndex);

    if (field) {
      if (!this.findCurrentColumnInLayouts(field, rowIndex, columnIndex)) {
        layoutFields[columnIndex][field].addField(currentRow);
      }
    } else {
      this.removeFieldByIndex(currentRow, currentField, columnIndex);
    }
  };

  handleOnSelectSortItem = (field) => {
    const { columnIndex, rowIndex } = this.state.currentCell;
    this.toggleSortField(field, columnIndex, rowIndex);
    this.applyConstraintsNext(field, columnIndex, rowIndex);
    this.handleRequestClose();
  }

  handleOnCheckboxItem = (field, columnIndex, rowIndex) => {
    this.toggleField(field, columnIndex, rowIndex);
    this.applyConstraintsNext(field, columnIndex, rowIndex);
  };

  applyConstraintsNext = (field, columnIndex, rowIndex) => {
    //addField and removeField work async so constraints should calculated on next run
    setTimeout(() => this.applyConstraints(field, columnIndex, rowIndex), 0);
  };

  applyConstraints = (field, columnIndex, rowIndex) => {
    const { activeTab } = this.props;
    if (activeTab !== 'raw') {
      this.applyAggregationConstraints(field, columnIndex, rowIndex);
    } else {
      this.applyRawConstraints(field, columnIndex, rowIndex);
    }
  };

  applyAggregationConstraints = (field, columnIndex, rowIndex) => {
    const currentRow = this.getRowByIndex(rowIndex);
    const dimensionSelected = this.findCurrentColumnInLayouts('dimensionFields', rowIndex, columnIndex);
    // const measureSelected = this.findCurrentColumnInLayouts('measureFields', rowIndex, columnIndex);
    const fieldSelected = field && this.findCurrentColumnInLayouts(field, rowIndex, columnIndex);

    if (['dimensionFields'].includes(field)) {
      if (!dimensionSelected) {
        this.removeFieldByIndex(currentRow, 'sortFields', columnIndex);
        this.removeFieldByIndex(currentRow, 'partitionFields', columnIndex);
        this.removeFieldByIndex(currentRow, 'distributionFields', columnIndex);
      }
    }
    if (['partitionFields', 'distributionFields', 'sortFields'].includes(field) && fieldSelected) {
      this.addFieldByIndex(currentRow, 'dimensionFields', columnIndex);
    }
  };

  applyRawConstraints = (field, columnIndex, rowIndex) => {
    const { layoutFields } = this.props;
    const currentRow = this.getRowByIndex(rowIndex);
    const displaySelected = this.findCurrentColumnInLayouts('displayFields', rowIndex, columnIndex);
    const displayDependentOn = ['sortFields', 'partitionFields', 'distributionFields'];

    if (displayDependentOn.includes(field) && !displaySelected) {
      layoutFields[columnIndex].displayFields.addField(currentRow);
    }
    if (field === 'displayFields' && !displaySelected) {
      for (const subField of displayDependentOn) {
        this.removeFieldByIndex(currentRow, subField, columnIndex);
      }
    }
  };

  renderRawDisplayCell = (rowIndex, columnIndex) => {
    const IconType = this.findCurrentColumnInLayouts('displayFields', rowIndex, columnIndex)
      ? 'OKSolid'
      : 'MinusSimple';
    return (
      <div style={styles.subCell} className='subCell'
        onClick={() => this.handleOnCheckboxItem('displayFields', columnIndex, rowIndex)}
      >
        <FontIcon
          theme={styles.iconTheme}
          type={IconType}/>
      </div>
    );
  }

  renderAggregationDisplayCell = (field, rowIndex, columnIndex) => {
    const iconType = this.findCurrentColumnInLayouts(field, rowIndex, columnIndex) ? 'OKSolid' : 'MinusSimple';
    return (
      <div style={styles.subCell} className='subCell'
        onClick={() => this.handleOnCheckboxItem(field, columnIndex, rowIndex)}
      >
        <FontIcon
          type={iconType}
          theme={styles.iconTheme}/>
      </div>
    );
  }

  renderSortCell = (rowIndex, columnIndex) => {
    const allColumns = this.props.dataset.get('fields');
    const datasetSchema = this.filterFieldList(allColumns).toJS();
    const currentColumn = datasetSchema.find((column, i) => i === rowIndex);
    const order = this.findCurrentIndexInFieldsList(currentColumn, 'sortFields', columnIndex);
    return (
      <div style={styles.subCell} className='subCell'
        onClick={(e) => this.handleOpenPopover(e, {columnIndex, rowIndex, labelCell: 'sort', field: 'sortFields'})}
      >
        <FontIcon type={order === -1 ? 'MinusSimple' : 'OKSolid'} theme={styles.iconTheme}/>
        {order !== -1 && this.findCurrentIndexInFieldsList(currentColumn, 'sortFields', columnIndex) + 1}
      </div>
    );
  }

  renderPartitionCell = (rowIndex, columnIndex) => {
    const partitionValue = this.findCurrentColumnInLayouts('partitionFields', rowIndex, columnIndex);
    const iconType = partitionValue ? 'OKSolid' : 'MinusSimple';
    return (
      <div style={this.shouldShowDistribution() ? styles.subCell : styles.lastSubCell} className='subCell'
        onClick={() => this.handleOnCheckboxItem('partitionFields', columnIndex, rowIndex)}
      >
        <FontIcon
          type={iconType}
          theme={styles.iconTheme}/>
      </div>
    );
  }

  renderDistributionCell = (rowIndex, columnIndex) => {
    if (!this.shouldShowDistribution()) {
      return;
    }
    const IconType = this.findCurrentColumnInLayouts('distributionFields', rowIndex, columnIndex)
      ? 'OKSolid'
      : 'MinusSimple';
    return (
      <div style={styles.lastSubCell} className='subCell'
        onClick={() => this.handleOnCheckboxItem('distributionFields', columnIndex, rowIndex)}
      >
        <FontIcon
          theme={styles.iconTheme}
          type={IconType}/>
      </div>
    );
  }

  renderBodyCell = (rowIndex, columnIndex) => {
    const allColumns = this.props.dataset.get('fields');
    const columns = this.filterFieldList(allColumns).toJS();
    const borderBottom = rowIndex === columns.length - 1 ? '1px solid #a8e0f1' : '';
    const backgroundColor = rowIndex % 2 ? '#eff6f9' : '#f5fcff';
    const opacity = this.props.layoutFields[columnIndex].shouldDelete.value ? 0.5 : 1;
    const isRaw = this.props.activeTab === 'raw';
    return (
      <div
        style={{backgroundColor, borderBottom, opacity, ...styles.cell}}
        key={`${rowIndex}-${columnIndex}`}
        data-qa={`acceleration-cell-${rowIndex + 1}-${columnIndex + 1}`}
      >
        {isRaw && this.renderRawDisplayCell(rowIndex, columnIndex)}
        {!isRaw && this.renderAggregationDisplayCell('dimensionFields', rowIndex, columnIndex)}
        {!isRaw && this.renderAggregationDisplayCell('measureFields', rowIndex, columnIndex)}
        {this.renderSortCell(rowIndex, columnIndex)}
        {this.renderPartitionCell(rowIndex, columnIndex)}
        {this.renderDistributionCell(rowIndex, columnIndex)}
      </div>
    );
  }

  render() {
    const { layoutFields, dataset, reflections, activeTab } = this.props;
    const { columnIndex } = this.state.currentCell;
    const allColumns = dataset.get('fields');
    const columns = this.filterFieldList(allColumns);

    return (
      <div style={styles.base}>
        <AccelerationGrid
          activeTab={activeTab}
          renderBodyCell={this.renderBodyCell}
          columns={columns}
          shouldShowDistribution={this.shouldShowDistribution()}
          filter={this.state.filter}
          onFilterChange={this.onFilterChange}
          layoutFields={layoutFields}
          reflections={reflections}
        />
        <CellPopover
          currentCell={this.state.currentCell && this.state.currentCell.labelCell}
          anchorEl={this.state.anchorEl}
          sortFields={layoutFields[columnIndex] && layoutFields[columnIndex].sortFields}
          onRequestClose={this.handleRequestClose}
          onSelectSortItem={this.handleOnSelectSortItem}
        />
      </div>
    );
  }
}

const styles = {
  base: {
    display: 'flex',
    flexGrow: 1
  },
  cell: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    height: 30,
    borderRight: '1px solid #a8e0f1',
    borderLeft: '1px solid #a8e0f1',
    marginLeft: 10,
    ...formLabel
  },
  subCell: {
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    width: '100%',
    height: '100%',
    padding: '0 auto',
    borderRight: '1px solid #e1e1e1'
  },
  displayButton: {
    marginLeft: 0,
    paddingLeft: 7,
    lineHeight: '22px',
    height: 20,
    width: 30,
    minWidth: 0,
    fontWeight: 500,
    fontSize: 13,
    color: WHITE,
    backgroundColor: '#558fdb',
    ':hover': {
      backgroundColor: '#558fdb'
    }
  },
  measureButton: {
    backgroundColor: '#9f70bb',
    ':hover': {
      backgroundColor: '#9f70bb'
    }
  },
  dimensionButton: {
    ':hover': {
      backgroundColor: '#558fdb'
    }
  },
  iconTheme: {
    Container: {
      cursor: 'pointer',
      height: 24
    }
  }
};

styles.lastSubCell = {
  ...styles.subCell,
  borderRight: 0
};
