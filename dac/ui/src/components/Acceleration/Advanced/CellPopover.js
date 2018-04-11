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
import Divider from 'material-ui/Divider';
import { formLabel } from 'uiTheme/radium/typography';
import { ACTIVE_DRAG_AREA } from 'uiTheme/radium/colors';
import { Overlay } from 'react-overlays';
import DragTarget from 'components/DragComponents/DragTarget';
import DragSource from 'components/DragComponents/DragSource';

const WIDTH_MENU = 170;

//TODO Refactor cellPopover render methods and configure externally with type
export default class CellPopover extends Component {
  static propTypes = {
    anchorEl: PropTypes.object,
    currentCell: PropTypes.string,
    sortFields: PropTypes.array,
    onRequestClose: PropTypes.func.isRequired,
    onSelectSortItem: PropTypes.func,
    partitionFields: PropTypes.array,
    onSelectPartitionItem: PropTypes.func
  };

  state = {
    dragIndex: -1,
    hoverIndex: -1,
    dragColumns: {
      partitionFields: [],
      sortFields: []
    }
  }

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
  }

  receiveProps(nextProps, oldProps) {
    const sortFieldsChanged = this.compareColumnAreaFields('sortFields', nextProps, oldProps);
    const partitionFieldsChanged = this.compareColumnAreaFields('partitionFields', nextProps, oldProps);
    if (sortFieldsChanged || partitionFieldsChanged) {
      this.updateFields(nextProps);
    }
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
    const sortFields = this.mapColumnAreaFields(props.sortFields || []);
    const partitionFields = this.mapColumnAreaFields(props.partitionFields || []);
    this.setState({
      dragColumns: {
        sortFields,
        partitionFields
      }
    });
  }

  handleDragStart = config => this.setState({dragIndex: config.index, hoverIndex: config.index})

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
  }

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
  }

  renderSortMenu = () => {
    const { sortFields } = this.props;
    return (
      <div style={{ width: WIDTH_MENU }}>
        <MenuItem onClick={() => this.props.onSelectSortItem()} primaryText={la('Off')}/>
        <MenuItem
          onClick={() => this.props.onSelectSortItem('sortFields')}
          primaryText={la('Sorted')}
        />
        { sortFields.length > 0 &&
          <div>
            <Divider />
            <span style={{ margin: '5px 5px 0 10px', ...formLabel }}>{la('Sort Order:')}</span>
            {this.renderColumnArea('sortFields')}
          </div>
        }
      </div>
    );
  }

  // note: this has been disabled pending BE support
  renderPartitionCell = () => {
    const { partitionFields } = this.props;
    return (
      <div style={{ width: WIDTH_MENU }}>
        <MenuItem
          onClick={() => this.props.onSelectPartitionItem()}
          primaryText={la('Off')}
        />
        <MenuItem
          onClick={() => this.props.onSelectPartitionItem('partitionFields')}
          primaryText={la('Partitioned')}
        />
        { partitionFields && partitionFields.length > 0 &&
        <div>
          <Divider />
          <span style={{ margin: '5px 5px 0 10px', ...formLabel }}>{la('Partition Order:')}</span>
          {this.renderColumnArea('partitionFields')}
        </div>
        }
      </div>
    );
  }

  renderContent = () => {
    const {currentCell} = this.props;
    let content;
    if (currentCell === 'sort') {
      content = this.renderSortMenu();
    }
    return (
      <Paper>
        <Menu width={WIDTH_MENU}>
          {content}
        </Menu>
      </Paper>
    );
  }

  render() {
    return (
      <Overlay
        show={Boolean(this.props.currentCell)}
        container={this}
        target={() => ReactDOM.findDOMNode(this.props.anchorEl)}
        placement='bottom'
        onHide={this.props.onRequestClose}
        rootClose
      >
        <div style={styles.base}>
          {this.renderContent()}
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
  }
};
