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
import ReactDOM from 'react-dom';
import Paper from 'material-ui/Paper';
import Menu from 'material-ui/Menu';
import MenuItem from 'material-ui/MenuItem';
import { get } from 'lodash/object';
import { formLabel } from 'uiTheme/radium/typography';
import { ACTIVE_DRAG_AREA } from 'uiTheme/radium/colors';
import { Overlay } from 'react-overlays';
import DragTarget from 'components/DragComponents/DragTarget';
import DragSource from 'components/DragComponents/DragSource';
import { Checkbox } from 'components/Fields';
import {
  fieldTypes,
  measureTypeLabels,
  cellType,
  granularityValue} from 'constants/AccelerationConstants';

import { checkboxStandalone } from '@app/components/Fields/Checkbox.less';

const WIDTH_MENU = 170;
const WIDTH_MENU_MEASURE = 190;

export default class CellPopover extends Component {
  static propTypes = {
    anchorEl: PropTypes.object,
    currentCell: PropTypes.shape({
      columnIndex: PropTypes.number,
      rowIndex: PropTypes.number,
      labelCell: PropTypes.oneOf(Object.values(cellType)),
      field: PropTypes.oneOf(Object.values(fieldTypes)),
      value: PropTypes.string,
      measureTypeList: PropTypes.array,
      measureTypeAll: PropTypes.array
    }),
    sortFields: PropTypes.array,
    onRequestClose: PropTypes.func.isRequired,
    partitionFields: PropTypes.array,
    onSelectPartitionItem: PropTypes.func,
    onSelectMenuItem: PropTypes.func
  };

  state = {
    dragIndex: -1,
    hoverIndex: -1,
    dragColumns: {
      partitionFields: [],
      sortFields: []
    },
    isMeasureCell: false,
    measureTypeList: []
  };

  componentWillMount() {
    this.receiveProps(this.props);
  }

  componentWillReceiveProps(nextProps) {
    this.receiveProps(nextProps, this.props);
  }

  handleDragEnd = (fieldName, column) => {
    const { dragIndex, hoverIndex } = this.state;
    this.props[fieldName].removeField(dragIndex);
    this.props[fieldName].addField(column, hoverIndex);
    this.setState({dragIndex: -1, hoverIndex: -1});
  };

  receiveProps(nextProps, oldProps) {
    const sortFieldsChanged = this.compareColumnAreaFields(fieldTypes.sort, nextProps, oldProps);
    const partitionFieldsChanged = this.compareColumnAreaFields(fieldTypes.partition, nextProps, oldProps);
    if (sortFieldsChanged || partitionFieldsChanged) {
      this.updateFields(nextProps);
    }
    this.setState({
      isMeasureCell: get(nextProps, 'currentCell.labelCell') === cellType.measure,
      measureTypeList: get(nextProps, 'currentCell.measureTypeList', [])
    });
  }

  compareColumnAreaFields(fieldName, nextProps, oldProps) {
    const newFields = nextProps && nextProps[fieldName] || [];
    const oldFields = oldProps && oldProps[fieldName] || [];
    // compare value of fields without attributes added by redux-form
    const newFieldList = Immutable.fromJS(this.mapColumnAreaFields(newFields));
    const oldFieldList = Immutable.fromJS(this.mapColumnAreaFields(oldFields));
    return newFieldList.size && !newFieldList.equals(oldFieldList);
  }

  /**
   * Returns plain list contained `fieldName`:`value` pair without additional attributes added by redux-form.
   * @type {Array}
   */
  mapColumnAreaFields(fields = []) {
    return fields.map(({ name }) => ({name: name.value}));
  }

  updateFields(props) {
    const sortFields = this.mapColumnAreaFields(props.sortFields);
    const partitionFields = this.mapColumnAreaFields(props.partitionFields);
    this.setState({
      dragColumns: {
        sortFields,
        partitionFields
      }
    });
  }

  handleDragStart = config => this.setState({
    dragIndex: config.index, hoverIndex: config.index
  });

  handleMoveColumn = (fieldName, dragIndex, hoverIndex) => {
    const column = this.state.dragColumns[fieldName][dragIndex];
    this.setState((state) =>  {
      state.dragColumns[fieldName].splice(dragIndex, 1);
      state.dragColumns[fieldName].splice(hoverIndex, 0, column);
      return {
        dragColumns: {
          [fieldName]: state.dragColumns[fieldName]
        },
        hoverIndex
      };
    });
  };

  handleHide = () => {
    this.props.onRequestClose();
  };

  renderColumnArea = fieldName => {
    const columns = this.state.dragColumns[fieldName];

    const indexes = {};
    for (const [i, column] of this.props[fieldName].entries()) {
      indexes[column.name.value] = i;
    }

    return (
      <div style={styles.columnList}>
        {
          columns.map((column, index) => {
            const columnName = column.name;
            const dragSourceStyle = this.state.hoverIndex === index ? styles.columnDragHover : { cursor: 'ns-resize' };
            return (
              <div style={styles.columnWrap} key={columnName}>
                <DragTarget
                  dragType='sortColumns'
                  moveColumn={(dragIndex, hoverIndex) => this.handleMoveColumn(fieldName, dragIndex, hoverIndex)}
                  index={index}
                >
                  <div style={dragSourceStyle}>
                    <DragSource
                      dragType='sortColumns'
                      index={index}
                      onDragStart={this.handleDragStart}
                      onDragEnd={() => this.handleDragEnd(fieldName, column)}
                      isFromAnother
                      id={columnName}>
                      <div style={styles.column}>
                        <div style={styles.columnIndex}>{indexes[columnName] + 1}</div>
                        <span style={{ marginLeft: 10 }}>{columnName}</span>
                      </div>
                    </DragSource>
                  </div>
                </DragTarget>
              </div>
            );
          })
        }
      </div>
    );
  };

  renderSortMenu = () => {
    const { sortFields } = this.props;
    return (
      <div style={{ width: WIDTH_MENU }}>
        { sortFields.length > 0 &&
          <div>
            <span style={styles.menuHeader}>
              {la('Drag to change sort order:')}
            </span>
            {this.renderColumnArea('sortFields')}
          </div>
        }
      </div>
    );
  };

  renderGranularityMenu = () => {
    const { currentCell } = this.props;
    // our material-ui is old, and MenuItem does not support selected property, thus messing with styles here
    return (
      <div style={{ width: WIDTH_MENU }}>
        <span style={styles.menuHeader}>
          {la('Date Granularity:')}
        </span>
        <div style={{marginTop: 5}}>
          <MenuItem
            onClick={() => this.props.onSelectMenuItem(cellType.dimension, granularityValue.normal)}
            primaryText={la('Original')}
            style={currentCell.value === granularityValue.normal ? styles.menuItemSelected : styles.menuItem}
          />
          <MenuItem
            onClick={() => this.props.onSelectMenuItem(cellType.dimension, granularityValue.date)}
            primaryText={la('Date')}
            style={currentCell.value === granularityValue.date ? styles.menuItemSelected : styles.menuItem}
          />
        </div>
      </div>
    );
  };

  toggleMeasure = (measure) => {
    // add or remove measure to/from currentCell.measureTypeList
    const measureTypeList = this.state.measureTypeList.slice();
    const pos = measureTypeList.indexOf(measure);
    if (pos === -1) {
      measureTypeList.push(measure);
    } else if (measureTypeList.length === 1) {
      // prevent user from removing a check from the last checkbox, leaving measure type list empty
      return;
    } else {
      measureTypeList.splice(pos, 1);
    }
    this.props.onSelectMenuItem(cellType.measure, measureTypeList);
  };

  renderMeasureMenu = () => {
    const typesToDisplay = get(this.props, 'currentCell.measureTypeAll', []);
    return (
      <div style={styles.measureMenu}>
        <span style={styles.measureMenuHeader}>
          {la('Selected Measures:')}
        </span>
        <div>
          {typesToDisplay.map((measure, index) => {
            return <div style={styles.measureMenuItem} key={index}>
              <Checkbox className={checkboxStandalone}
                checked={Boolean(this.state.measureTypeList.find(item => item === measure))}
                dataQa={`checkbox-${measure}`}
                onChange={this.toggleMeasure.bind(this, measure)}
                label={measureTypeLabels[measure]}/>
            </div>;
          })
          }
        </div>
      </div>
    );
  };

  makeContent = () => {
    if (!this.props.currentCell) return '';

    switch (this.props.currentCell.labelCell) {
    case cellType.sort:
      return this.renderSortMenu();
    case cellType.dimension:
      return this.renderGranularityMenu();
    case cellType.measure:
      return this.renderMeasureMenu();
    default:
      return '';
    }
  };

  render() {
    const showOverlay = !!(get(this.props, 'currentCell.labelCell'));
    const menuWidth = (this.state.isMeasureCell) ? WIDTH_MENU_MEASURE : WIDTH_MENU;
    return (
      <Overlay
        show={showOverlay}
        container={this}
        target={() => ReactDOM.findDOMNode(this.props.anchorEl)}
        placement='bottom'
        onHide={this.handleHide}
        rootClose
      >
        <div style={{...styles.base, width: menuWidth}}>
          <Paper>
            <Menu width={menuWidth}>
              {this.makeContent()}
            </Menu>
          </Paper>
        </div>
      </Overlay>
    );
  }
}

const styles = {
  base: {
    width: WIDTH_MENU,
    position: 'absolute',
    zIndex: 30000,
    marginLeft: 80
  },
  columnList: {
    padding: '0 10px 10px 10px'
  },
  columnWrap: {
    marginTop: 10
  },
  columnDragHover: {
    width: '100%',
    height: 20,
    backgroundColor: ACTIVE_DRAG_AREA
  },
  column: {
    display: 'flex',
    height: 20,
    alignItems: 'center',
    border: '1px solid #a8e7d9',
    backgroundColor: '#ebf9f6'
  },
  columnIndex: {
    ...formLabel,
    width: 20,
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    backgroundColor: '#96e3d1',
    height: '100%'
  },
  menuHeader: {
    margin: '5px 5px 0 10px',
    ...formLabel
  },
  menuItem: {
    lineHeight: '25px',
    minHeight: '25px',
    fontSize: 12
  },
  measureMenu: {
    width: WIDTH_MENU_MEASURE,
    padding: '0 10px'
  },
  measureMenuHeader: {
    ...formLabel
  },
  measureMenuItem: {
    marginTop: 7
  }
};

styles.menuItemSelected = {
  ...styles.menuItem,
  backgroundColor: '#ebf9f6'
};
