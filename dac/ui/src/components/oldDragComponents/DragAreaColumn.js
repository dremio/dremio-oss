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
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Radium from 'radium';
import Immutable from 'immutable';
import { Popover } from '@app/components/Popover';

import FontIcon from 'components/Icon/FontIcon';

import { formDefault, formPlaceholder } from 'uiTheme/radium/typography';
import { typeToIconType } from 'constants/DataTypes';
import { PALE_BLUE, HISTORY_ITEM_COLOR, ACTIVE_DRAG_AREA, BORDER_ACTIVE_DRAG_AREA } from 'uiTheme/radium/colors';
import { FLEX_ROW_START_CENTER } from 'uiTheme/radium/flexStyle';
import DragSource from 'components/DragComponents/DragSource';
import DragTarget from 'components/DragComponents/DragTarget';
import {NOT_SUPPORTED_TYPES} from './DragColumnMenu';

const COUNT_ACTION = 'Count (*)';

@pureRender
@Radium
class DragAreaColumn extends Component {
  static propTypes = {
    dragColumntableType: PropTypes.string,
    ownDragColumntableType: PropTypes.string,
    item: PropTypes.instanceOf(Immutable.Map).isRequired,
    dragType: PropTypes.string.isRequired,
    type: PropTypes.string.isRequired,
    onDragStart: PropTypes.func,
    isDragInProgress: PropTypes.bool,
    moveColumn: PropTypes.func,
    removeColumn: PropTypes.func,
    index: PropTypes.number,
    showColumnDropdown: PropTypes.func,
    showSortDropdown: PropTypes.func,
    onDragEnd: PropTypes.func,
    addColumn: PropTypes.func,
    icon: PropTypes.any,
    id: PropTypes.any,
    columns: PropTypes.instanceOf(Immutable.List)
  };

  constructor(props) {
    super(props);
    this.renderContent = this.renderContent.bind(this);
    this.showPopover = this.showPopover.bind(this);
    this.handleRequestClose = this.handleRequestClose.bind(this);
    this.handleDrop = this.handleDrop.bind(this);
    this.moveColumn = this.moveColumn.bind(this);
    this.disableColor = this.disableColor.bind(this);
    this.enableColor = this.enableColor.bind(this);
    this.checkDropPosibility = this.checkDropPosibility.bind(this);
    this.selectItemOnDrag = this.selectItemOnDrag.bind(this);

    this.state = {
      fieldDisabled: false,
      isOpen: false,
      pattern: ''
    };
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.item.get('action') === COUNT_ACTION) {
      this.setState({
        fieldDisabled: true,
        isOpen: false,
        pattern: ''
      });
    } else {
      this.setState({
        fieldDisabled: false
      });
    }
  }

  handleDrop(data) {
    const { type, id, item, index } = this.props;
    const selectedItem = {
      columnName: data.id,
      dragColumnId: id || item.get('name'),
      dragColumnType: type,
      dragColumnIndex: index
    };
    if (this.checkDropPosibility()) {
      this.props.addColumn(selectedItem);
    }
  }

  selectItemOnDrag(selectedItem) {
    const { disabled } = selectedItem;
    if (disabled) {
      return;
    }
    this.handleRequestClose();
    this.props.addColumn(selectedItem);
  }

  moveColumn(...args) {
    if (this.props.moveColumn) {
      this.props.moveColumn(...args);
    }
  }

  enableColor() {
    if (this.checkDropPosibility()) {
      this.setState({ isNotEmptyDragArea: true });
    }
  }

  disableColor() {
    this.setState({ isNotEmptyDragArea: false });
  }

  showPopover(event) {
    if (!this.shouldHideField()) {
      this.setState({
        isOpen: true,
        anchorEl: event.currentTarget
      });
    }
  }

  shouldHideField() {
    return this.props.item.get('action') === COUNT_ACTION;
  }

  checkDropPosibility() {
    const { item, isDragInProgress, dragColumntableType, ownDragColumntableType } = this.props;
    return item.get('empty') && isDragInProgress && dragColumntableType === ownDragColumntableType;
  }

  handleRequestClose() {
    this.setState({ isOpen: false });
  }

  handlePatternChange(e) {
    this.setState({
      pattern: e.target.value
    });
  }

  renderColumns() {
    const { type, id, item, index } = this.props;
    const { pattern } = this.state;
    return this.props.columns.filter(column =>
      column.get('name').toLowerCase().includes(pattern.trim().toLowerCase())
    )
    .sort(a => (NOT_SUPPORTED_TYPES.indexOf(a.get('type')) !== -1))
    .map(column => {
      const columnType = column.get('type');
      const disabled = NOT_SUPPORTED_TYPES.indexOf(columnType) !== -1;
      const disabledStyle = disabled ? {color: HISTORY_ITEM_COLOR} : {};
      const columnName = column.get('name');
      const selectedItem = {
        dragColumnId: id || item.get('name'),
        dragColumnType: type,
        dragColumnIndex: index,
        columnName,
        columnType,
        disabled
      };

      return (
        <div
          disabled={disabled}
          data-qa={columnName}
          style={[styles.column, formDefault]}
          key={columnName}
          onClick={this.selectItemOnDrag.bind(this, selectedItem)}>
          <FontIcon type={typeToIconType[columnType]} theme={styles.type}/>
          <span style={[{marginLeft: 5}, disabledStyle]}>{columnName}</span>
        </div>
      );
    });
  }

  renderContent() {
    const { item, index, type } = this.props;
    const column = item;
    if (this.checkDropPosibility()) {
      return (
        <div
          style={[styles.content, styles.largeContent, styles.empty]}
          key='custom-content'
          onClick={this.showPopover}
          data-qa={`join-search-field-area-${index}-${type}`}
        >
          <span>Drag a field here</span>
        </div>
      );
    } else if (column.get('empty')) {
      return (
        <div
          style={[styles.content, styles.largeContent, styles.empty, {borderWidth: 0}]}
          key='custom-content'
          onClick={this.showPopover}>
          <div style={styles.searchWrap}>
            <input
              style={[styles.search, formPlaceholder, {fontSize: 11}]}
              onChange={this.handlePatternChange.bind(this)}
              placeholder={la('Choose field…')}
            />
            <FontIcon type='Search' theme={styles.icon}/>
          </div>
          <Popover
            useLayerForClickAway={false}
            anchorEl={this.state.isOpen ? this.state.anchorEl : null }
            onClose={this.handleRequestClose}
            listWidthSameAsAnchorEl
          >
            <div style={styles.popover} data-qa='popover'>
              {this.renderColumns()}
            </div>
          </Popover>
        </div>
      );
    }
    return (
      <div style={[styles.content, styles.largeContent]} key='custom-content'>
        <FontIcon type={typeToIconType[item.get('type')]} key='custom-type' theme={styles.type}/>
        <span style={styles.name} key='custom-name'>
          {column.get('name')}
        </span>
      </div>
    );
  }

  render() {
    const { item, type, index } = this.props;
    const column = item;
    const opacity = column.get('isFake')
      ? styles.opacity
      : {};
    const columnStyle = this.shouldHideField() ? {visibility: 'hidden'} : null;
    const color = this.state.isNotEmptyDragArea ? ACTIVE_DRAG_AREA : '#fff';
    const dragStyle = this.checkDropPosibility()
      ? {...styles.dragStyle, backgroundColor: color}
      : {};
    return (
      <div className='inner-join-column' style={[styles.base, opacity]}
        onDragLeave={this.disableColor}
        onDragOver={this.enableColor}>
        <DragSource
          dragType={this.props.dragType}
          onDragStart={this.onDragStart}
          index={this.props.index}
          onDragEnd={this.props.onDragEnd}
          id={column.get('name')}>
          <DragTarget
            onDrop={this.handleDrop}
            dragType={this.props.dragType}
            moveColumn={this.moveColumn}
            index={this.props.index}
            id={column.get('name')}>
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
                      onClick={this.props.removeColumn.bind(this, type, column.get('name'), index)}/>
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
    opacity: 1
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
    marginLeft: 5
  },
  columnWrap: {
    display: 'flex',
    flexWrap: 'nowrap',
    alignItems: 'center',
    justifyContent: 'center',
    marginTop: 5,
    minWidth: 100
  },
  dragStyle: {
    borderBottom: '1px dotted gray',
    borderTop: '1px dotted gray',
    borderLeft: '1px dotted gray',
    borderRight: '1px dotted gray',
    backgroundColor: 'white',
    height: 26
  },
  empty: {
    cursor: 'default'
  },
  opacity: {
    opacity: 0
  },
  searchWrap: {
    width: '100%',
    position: 'relative',
    display: 'flex',
    flexWrap: 'nowrap',
    background: '#f3f3f3',
    padding: '4px 4px 3px 4px'
  },
  search: {
    border: '1px solid rgba(0, 0, 0, 0.1)',
    width: '100%',
    padding: '2px 10px',
    outline: 0
  },
  icon: {
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
