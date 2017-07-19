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
import Immutable from 'immutable';
import Radium from 'radium';
import { formLabel } from 'uiTheme/radium/typography';
import { WHITE } from 'uiTheme/radium/colors';
import FontIcon from 'components/Icon/FontIcon';

import AccelerationGridControllerMixin from 'dyn-load/components/Acceleration/Advanced/AccelerationGridControllerMixin';

import AccelerationGrid from './AccelerationGrid';
import CellPopover from './CellPopover';

// AccelerationGridController is used for behavior definition cells on Raw/Aggregation
// Aggregation:
//   User can enable one of 'Dimension' or 'Measure' per field
//   If user enabled 'Dimension' we should turn off 'Measure', when 'Measure' enabled - turn off 'Dimension'.
//   If user didn't enable 'Dimension' field but enables sort/partition/distribution field, we should enable 'Dimension' by default.
//   If user disabled 'Dimension' and 'Measure' fields, we should disable sort/partition/distribution fields.
//   If user enables sort, we should make enable 'Dimension' field.
//   If user disable 'Dimension' and 'Measure' enabled then we keep partition/distribution, but turn off sort.
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
    layoutFields: PropTypes.array,
    activeTab: PropTypes.string,
    acceleration: PropTypes.instanceOf(Immutable.Map),
    removeLayout: PropTypes.func
  };

  state = {
    filter: '',
    fieldList: this.props.acceleration.getIn(['context', 'datasetSchema', 'fieldList']),
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
    const allColumns = this.props.acceleration.getIn(['context', 'datasetSchema', 'fieldList']);
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

    if (!this.props.layoutFields[colIndex] || !this.props.layoutFields[colIndex].details[field]) {
      return -1;
    }

    return this.props.layoutFields[colIndex].details[field].findIndex((col) => {
      if (col.name && col.name.value === currentColumn.name) {
        return true;
      }
    });
  }

  findCurrentColumnInLayouts = (field, rowIndex, columnIndex) => {
    const { layoutFields, acceleration } = this.props;
    const allColumns = acceleration.getIn(['context', 'datasetSchema', 'fieldList']);
    const datasetSchema = this.filterFieldList(allColumns).toJS();
    const currentColumn = datasetSchema.find((column, i) => i === rowIndex);
    return layoutFields[columnIndex]
      && layoutFields[columnIndex].details[field].find(col => col.name.value === currentColumn.name);
  }

  removeFieldByIndex = (currentColumn, field, columnIndex) => {
    const index = this.findCurrentIndexInFieldsList(currentColumn, field, columnIndex);
    if (index !== -1) {
      this.props.layoutFields[columnIndex].details[field].removeField(index);
    }
  };

  addFieldByIndex = (currentColumn, field, columnIndex) => {
    const index = this.findCurrentIndexInFieldsList(currentColumn, field, columnIndex);
    if (index === -1) {
      this.props.layoutFields[columnIndex].details[field].addField(currentColumn);
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
      layoutFields[columnIndex].details[field].addField(currentRow);
    }
  };

  toggleSortField = (field, columnIndex, rowIndex) => {
    const { layoutFields } = this.props;
    const currentField = this.state.currentCell.field;
    const currentRow = this.getRowByIndex(rowIndex);

    if (field) {
      if (!this.findCurrentColumnInLayouts(field, rowIndex, columnIndex)) {
        layoutFields[columnIndex].details[field].addField(currentRow);
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
    const dimensionSelected = this.findCurrentColumnInLayouts('dimensionFieldList', rowIndex, columnIndex);
    const measureSelected = this.findCurrentColumnInLayouts('measureFieldList', rowIndex, columnIndex);
    const fieldSelected = field && this.findCurrentColumnInLayouts(field, rowIndex, columnIndex);

    if (['dimensionFieldList', 'measureFieldList'].includes(field)) {
      if (dimensionSelected && measureSelected) {
        if (field === 'dimensionFieldList') {
          this.removeFieldByIndex(currentRow, 'measureFieldList', columnIndex);
        } else {
          this.removeFieldByIndex(currentRow, 'dimensionFieldList', columnIndex);
          this.removeFieldByIndex(currentRow, 'sortFieldList', columnIndex);
          this.removeFieldByIndex(currentRow, 'partitionFieldList', columnIndex);
          this.removeFieldByIndex(currentRow, 'distributionFieldList', columnIndex);
        }
      }
      if (!dimensionSelected) {
        this.removeFieldByIndex(currentRow, 'sortFieldList', columnIndex);
        this.removeFieldByIndex(currentRow, 'partitionFieldList', columnIndex);
        this.removeFieldByIndex(currentRow, 'distributionFieldList', columnIndex);
      }
    }
    if (['partitionFieldList', 'distributionFieldList', 'sortFieldList'].includes(field) && fieldSelected) {
      this.removeFieldByIndex(currentRow, 'measureFieldList', columnIndex);
      this.addFieldByIndex(currentRow, 'dimensionFieldList', columnIndex);
    }
  };

  applyRawConstraints = (field, columnIndex, rowIndex) => {
    const { layoutFields } = this.props;
    const currentRow = this.getRowByIndex(rowIndex);
    const displaySelected = this.findCurrentColumnInLayouts('displayFieldList', rowIndex, columnIndex);
    const displayDependentOn = ['sortFieldList', 'partitionFieldList', 'distributionFieldList'];

    if (displayDependentOn.includes(field) && !displaySelected) {
      layoutFields[columnIndex].details.displayFieldList.addField(currentRow);
    }
    if (field === 'displayFieldList' && !displaySelected) {
      for (const subField of displayDependentOn) {
        this.removeFieldByIndex(currentRow, subField, columnIndex);
      }
    }
  };

  renderRawDisplayCell = (rowIndex, columnIndex) => {
    const IconType = this.findCurrentColumnInLayouts('displayFieldList', rowIndex, columnIndex)
      ? 'OKSolid'
      : 'MinusSimple';
    return (
      <div style={styles.subCell} className='subCell'
        onClick={() => this.handleOnCheckboxItem('displayFieldList', columnIndex, rowIndex)}
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
    const { acceleration } = this.props;
    const allColumns = acceleration.getIn(['context', 'datasetSchema', 'fieldList']);
    const datasetSchema = this.filterFieldList(allColumns).toJS();
    const currentColumn = datasetSchema.find((column, i) => i === rowIndex);
    const order = this.findCurrentIndexInFieldsList(currentColumn, 'sortFieldList', columnIndex);
    return (
      <div style={styles.subCell} className='subCell'
        onClick={(e) => this.handleOpenPopover(e, {columnIndex, rowIndex, labelCell: 'sort', field: 'sortFieldList'})}
      >
        <FontIcon type={order === -1 ? 'MinusSimple' : 'OKSolid'} theme={styles.iconTheme}/>
        {order !== -1 && this.findCurrentIndexInFieldsList(currentColumn, 'sortFieldList', columnIndex) + 1}
      </div>
    );
  }

  renderPartitionCell = (rowIndex, columnIndex) => {
    const partitionValue = this.findCurrentColumnInLayouts('partitionFieldList', rowIndex, columnIndex);
    const iconType = partitionValue ? 'OKSolid' : 'MinusSimple';
    return (
      <div style={this.shouldShowDistribution() ? styles.subCell : styles.lastSubCell} className='subCell'
        onClick={() => this.handleOnCheckboxItem('partitionFieldList', columnIndex, rowIndex)}
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
    const IconType = this.findCurrentColumnInLayouts('distributionFieldList', rowIndex, columnIndex)
      ? 'OKSolid'
      : 'MinusSimple';
    return (
      <div style={styles.lastSubCell} className='subCell'
        onClick={() => this.handleOnCheckboxItem('distributionFieldList', columnIndex, rowIndex)}
      >
        <FontIcon
          theme={styles.iconTheme}
          type={IconType}/>
      </div>
    );
  }

  renderBodyCell = (rowIndex, columnIndex) => {
    const allColumns = this.props.acceleration.getIn(['context', 'datasetSchema', 'fieldList']);
    const columns = this.filterFieldList(allColumns).toJS();
    const borderBottom = rowIndex === columns.length - 1 ? '1px solid #a8e0f1' : '';
    const backgroundColor = rowIndex % 2 ? '#eff6f9' : '#f5fcff';
    const isRaw = this.props.activeTab === 'raw';
    return (
      <div
        style={{backgroundColor, borderBottom, ...styles.cell}}
        key={`${rowIndex}-${columnIndex}`}
        data-qa={`acceleration-cell-${rowIndex + 1}-${columnIndex + 1}`}
      >
        {isRaw && this.renderRawDisplayCell(rowIndex, columnIndex)}
        {!isRaw && this.renderAggregationDisplayCell('dimensionFieldList', rowIndex, columnIndex)}
        {!isRaw && this.renderAggregationDisplayCell('measureFieldList', rowIndex, columnIndex)}
        {this.renderSortCell(rowIndex, columnIndex)}
        {this.renderPartitionCell(rowIndex, columnIndex)}
        {this.renderDistributionCell(rowIndex, columnIndex)}
      </div>
    );
  }

  render() {
    const { layoutFields, acceleration, activeTab } = this.props;
    const { columnIndex } = this.state.currentCell;
    const allColumns = acceleration.getIn(['context', 'datasetSchema', 'fieldList']);
    const columns = this.filterFieldList(allColumns);

    return (
      <div>
        <AccelerationGrid
          activeTab={activeTab}
          renderBodyCell={this.renderBodyCell}
          columns={columns}
          shouldShowDistribution={this.shouldShowDistribution()}
          filter={this.state.filter}
          onFilterChange={this.onFilterChange}
          layoutFields={layoutFields}
          acceleration={acceleration}
          removeLayout={this.props.removeLayout}
        />
        <CellPopover
          currentCell={this.state.currentCell && this.state.currentCell.labelCell}
          anchorEl={this.state.anchorEl}
          sortFieldList={layoutFields[columnIndex] && layoutFields[columnIndex].details.sortFieldList}
          onRequestClose={this.handleRequestClose}
          onSelectSortItem={this.handleOnSelectSortItem}
        />
      </div>
    );
  }
}

const styles = {
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
      cursor: 'pointer'
    }
  }
};

styles.lastSubCell = {
  ...styles.subCell,
  borderRight: 0
};
