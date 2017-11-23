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
import { Component } from 'react';
import Radium from 'radium';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import Popover from 'material-ui/Popover';

import FontIcon from 'components/Icon/FontIcon';
import { SearchField } from 'components/Fields';
import ColumnDragItem from 'utils/ColumnDragItem';
import EllipsedText from 'components/EllipsedText';

import { formDefault } from 'uiTheme/radium/typography';
import { typeToIconType } from 'constants/DataTypes';
import { PALE_BLUE, HISTORY_ITEM_COLOR, ACTIVE_DRAG_AREA, BORDER_ACTIVE_DRAG_AREA } from 'uiTheme/radium/colors';
import { FLEX_ROW_START_CENTER } from 'uiTheme/radium/flexStyle';
import { NOT_SUPPORTED_TYPES } from './DragColumnMenu';
import DragSource from './DragSource';
import DragTarget from './DragTarget';

@Radium
class DragAreaColumn extends Component {
  static propTypes = {
    dragItem: PropTypes.instanceOf(ColumnDragItem),
    field: PropTypes.object,
    disabled: PropTypes.bool,
    canSelectColumn: PropTypes.func,
    dragColumntableType: PropTypes.string,
    ownDragColumntableType: PropTypes.string,
    dragType: PropTypes.string.isRequired,
    dragOrigin: PropTypes.string.isRequired,
    isDragInProgress: PropTypes.bool,
    index: PropTypes.number,
    onRemoveColumn: PropTypes.func,
    onDragStart: PropTypes.func,
    onDragEnd: PropTypes.func,
    moveColumn: PropTypes.func,
    icon: PropTypes.any,
    id: PropTypes.any,
    allColumns: PropTypes.instanceOf(Immutable.List)
  };

  static defaultProps = {
    allColumns: Immutable.List()
  }

  state = {
    sOpen: false,
    pattern: '',
    anchorOrigin: {
      horizontal: 'left',
      vertical: 'bottom'
    },
    targetOrigin: {
      horizontal: 'left',
      vertical: 'top'
    }
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.disabled) {
      this.setState({
        isOpen: false,
        pattern: ''
      });
    }
  }

  handleDrop = (data) => {
    if (this.checkDropPosibility()) {
      this.props.field.onChange(data.id);
    }
  }

  canSelectColumn(columnName) {
    if (this.props.canSelectColumn) {
      return this.props.canSelectColumn(columnName);
    }
    return true;
  }

  selectColumn = (selectedColumn) => {
    const { columnName } = selectedColumn;
    if (!this.canSelectColumn(columnName) ) {
      return;
    }
    this.handleRequestClose();

    this.props.field.onChange(columnName);
  }

  moveColumn = (...args) => {
    if (this.props.moveColumn) {
      this.props.moveColumn(...args);
    }
  }

  handleDragOver = () => {
    if (this.checkDropPosibility()) {
      this.setState({ isDragAreaActive: true });
    }
  }

  handleDragOut = () => {
    this.setState({ isDragAreaActive: false });
  }

  showPopover = (event) => {
    if (!this.props.disabled) {
      this.setState({
        isOpen: true,
        anchorEl: event.currentTarget
      });
    }
  }

  checkDropPosibility() {
    const { field, isDragInProgress, dragItem } = this.props;
    const canSelect = this.canSelectColumn(dragItem);
    return !field.value && isDragInProgress && canSelect;
  }

  handleRemoveColumn = () => {
    this.props.onRemoveColumn(this.props.index);
  }

  handleRequestClose = () => {
    this.setState({ isOpen: false });
  }

  handlePatternChange = (value) => {
    this.setState({
      pattern: value
    });
  }

  filterColumns() {
    const { pattern } = this.state;
    return this.props.allColumns.filter(column => column.get('name')
      .toLowerCase().includes(pattern.trim().toLowerCase()));
  }

  renderAllColumns() {
    const { dragOrigin, field, id, index } = this.props;
    const selectedColumnName = field.value;
    return this.filterColumns()
    .sort(a => (NOT_SUPPORTED_TYPES.has(a.get('type'))))
    .map(column => {
      const columnType = column.get('type');
      const columnName = column.get('name');
      const columnDisabled = !this.canSelectColumn(columnName)
        || NOT_SUPPORTED_TYPES.has(columnType);
      const disabledStyle = columnDisabled ? {color: HISTORY_ITEM_COLOR} : {};
      const columnData = {
        dragColumnId: id || selectedColumnName,
        dragOrigin,
        dragColumnIndex: index,
        columnName,
        columnType,
        columnDisabled
      };

      return (
        <div
          disabled={columnDisabled}
          data-qa={columnName}
          style={[styles.column, formDefault]}
          key={columnName}
          onClick={!columnDisabled && this.selectColumn.bind(this, columnData)}>
          <FontIcon type={typeToIconType[columnType]} theme={styles.type}/>
          <EllipsedText style={{...styles.name, ...disabledStyle}} text={columnName} />
        </div>
      );
    });
  }

  renderContent() {
    const { field, disabled } = this.props;
    if (disabled) {
      return <div/>;
    }

    const width = this.state.anchorEl && this.state.anchorEl.offsetWidth;
    if (this.checkDropPosibility()) {
      return (
        <div
          style={[styles.content, styles.largeContent, styles.empty]}
          key='custom-content'
          onClick={this.showPopover}>
          <span>Drag a field here</span>
        </div>
      );
    } else if (!field.value) {
      return (
        <div
          style={[styles.content, styles.largeContent, styles.empty, {borderWidth: 0}]}
          key='custom-content'
          onClick={this.showPopover}>
          <SearchField
            style={styles.searchStyle}
            searchIconTheme={styles.searchIcon}
            inputStyle={styles.searchInput}
            value={this.state.pattern}
            onChange={this.handlePatternChange}
            placeholder={la('Choose field…')}
          />
          <Popover
            ref='menu'
            style={{marginTop: 2, marginLeft: 0, width}}
            useLayerForClickAway={false}
            open={this.state.isOpen}
            canAutoPosition
            anchorEl={this.state.anchorEl}
            anchorOrigin={this.state.anchorOrigin}
            targetOrigin={this.state.targetOrigin}
            onRequestClose={this.handleRequestClose}>
            <div style={styles.popover} data-qa='popover'>
              {this.renderAllColumns()}
            </div>
          </Popover>
        </div>
      );
    }

    const selectedColumn = this.props.allColumns.find(c => c.get('name') === field.value);

    return (
      <div style={[styles.content, styles.largeContent]} key='custom-content'>
        <FontIcon type={typeToIconType[selectedColumn.get('type')]} key='custom-type' theme={styles.type}/>
        <EllipsedText style={styles.name} text={field.value} />
      </div>
    );
  }

  render() {
    const { field, disabled } = this.props;
    const columnName = field.value;
    const columnStyle = disabled ? {visibility: 'hidden'} : null;
    const color = this.state.isDragAreaActive ? ACTIVE_DRAG_AREA : '#fff';
    const dragStyle = this.checkDropPosibility()
      ? {...styles.dragStyle, backgroundColor: color}
      : {};

    return (
      <div className='inner-join-column' style={styles.base}
        onDragLeave={this.handleDragOut}
        onDragOver={this.handleDragOver}>
        <DragSource
          type={this.props.dragOrigin}
          dragType={this.props.dragType}
          onDragStart={this.props.onDragStart}
          index={this.props.index}
          onDragEnd={this.props.onDragEnd}
          id={columnName}>
          <DragTarget
            onDrop={this.handleDrop}
            dragType={this.props.dragType}
            moveColumn={this.moveColumn}
            index={this.props.index}
            id={columnName}>
            <div style={[styles.columnWrap]}>
              <div style={[styles.simpleColumn, styles.largeColumn, dragStyle, columnStyle]} key='custom'>
                {this.renderContent()}
              </div>
              {
                this.props.icon
                  ? this.props.icon
                  : (
                    <FontIcon
                      type='CanceledGray'
                      theme={styles.fontIcon}
                      onClick={this.handleRemoveColumn}/>
                  )
              }
            </div>
          </DragTarget>
        </DragSource>
      </div>
    );
  }
}

const styles = {
  base: {
    display: 'flex',
    width: '100%',
    justifyContent: 'center',
    opacity: 1,
    overflow: 'hidden'
  },
  fontIcon: {
    Container: {
      width: 25,
      height: 25,
      position: 'relative',
      top: 1
    },
    Icon: {
      cursor: 'pointer'
    }
  },
  column: {
    ...FLEX_ROW_START_CENTER,
    height: 26,
    padding: 5,
    cursor: 'pointer',
    ':hover': {
      backgroundColor: PALE_BLUE
    }
  },
  popover: {
    maxHeight: 200,
    marginLeft: 2
  },
  name: {
    marginLeft: 5
  },

  type: {
    'Icon': {
      width: 24,
      height: 20,
      backgroundPosition: 'left center'
    },
    Container: {
      width: 24,
      height: 20,
      top: 0
    }
  },
  largeContent: {
    marginTop: 0,
    borderRadius: 1
  },
  largeColumn: {
    width: '100%',
    marginTop: 0,
    backgroundColor: ACTIVE_DRAG_AREA,
    border: `1px solid ${BORDER_ACTIVE_DRAG_AREA}`
  },
  content: {
    display: 'flex',
    width: '100%',
    borderLeft: '1px solid transparent',
    borderRight: '1px solid transparent',
    borderTop: '1px solid transparent',
    borderBottom: '1px solid transparent',
    alignItems: 'center',
    cursor: 'move'
  },
  simpleColumn: {
    display: 'flex',
    alignItems: 'center',
    width: 179,
    minHeight: 28,
    backgroundColor: '#f3f3f3',
    borderRadius: 1,
    marginLeft: 5,
    overflow: 'hidden'
  },
  columnWrap: {
    display: 'flex',
    flexWrap: 'nowrap',
    alignItems: 'center',
    justifyContent: 'center',
    padding: '2px 0',
    minWidth: 100
  },
  dragStyle: {
    borderBottom: '1px dotted gray',
    borderTop: '1px dotted gray',
    borderLeft: '1px dotted gray',
    borderRight: '1px dotted gray',
    backgroundColor: 'white',
    height: 30
  },
  empty: {
    cursor: 'default'
  },
  opacity: {
    opacity: 0
  },
  searchStyle: {
    padding: '4px 4px 3px 4px'
  },
  searchInput: {
    padding: '2px 10px 2px 10px',
    fontSize: 11
  },
  searchIcon: {
    Container: {
      position: 'absolute',
      right: 5,
      top: 5
    },
    Icon: {
      width: 18,
      height: 18
    }
  }
};

export default DragAreaColumn;
