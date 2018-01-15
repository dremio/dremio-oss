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
import { Cell } from 'fixed-data-table-2';
import { Popover, PopoverAnimationVertical } from 'material-ui/Popover';

import Radium from 'radium';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import $ from 'jquery';

import DragSource from 'components/DragComponents/DragSource';
import ColumnActionMenu from 'components/Menus/ExplorePage/ColumnActionMenu';
import ColumnTypeMenu from 'components/Menus/ExplorePage/ColumnTypeMenu';
import FontIcon from 'components/Icon/FontIcon';

import { EXPLORE_HOVER_COLOR } from 'uiTheme/radium/colors';

import { typeToIconType, BINARY } from 'constants/DataTypes';
import Keys from 'constants/Keys.json';

const MAX_COLUMN_NAME_LENTH = 62;
const ACTION_MENU_WIDTH = 24;
const COLUMN_HEIGHT = 24;
const MAX_HEIGHT_FOR_ANIMATION = 370;
const MARGIN_RIGHT = 5;

@Radium
@pureRender
export default class ColumnHeader extends Component {
  static propTypes = {
    pageType: PropTypes.string,
    defaultColumnWidth: PropTypes.number,
    isResizeInProgress: PropTypes.bool,
    column: PropTypes.object.isRequired,
    width: PropTypes.number,
    dragType: PropTypes.string.isRequired,
    pathname: PropTypes.string,
    query: PropTypes.object,
    isDumbTable: PropTypes.bool,
    columnsCount: PropTypes.number,

    updateColumnName: PropTypes.func.isRequired,
    makeTransform: PropTypes.func.isRequired,
    openDetailsWizard: PropTypes.func,
    preconfirmTransform: PropTypes.func.isRequired
  };

  forceFocus = false; // eslint-disable-line react/sort-comp

  static emulateAutoPosition(anchor, target, targetOrigin, anchorOrigin, targetPosition) {
    const move = targetPosition.top + target.bottom - window.innerHeight;
    return {
      top: move > 0 ? targetPosition.top - move - 10 : targetPosition.top,
      bottom: 0,
      left: targetPosition.left
    };
  }

  static getDragData(name) {
    return {
      type: 'columnName',
      data: {name}
    };
  }

  static getAnimationStyle(size) {
    return size <= MAX_HEIGHT_FOR_ANIMATION
      ? {
        transition: `transform 0ms cubic-bezier(0.23, 1, 0.32, 1) 0ms,
                     opacity 450ms cubic-bezier(0.23, 1, 0.32, 1) 0ms,
                     top 0ms cubic-bezier(0.23, 1, 0.32, 1) 0ms`
      }
      : {};
  }

  constructor(props) {
    super(props);

    this.doTypeAction = this.doTypeAction.bind(this);

    this.state = {
      open: false,
      openType: false,
      anchorOrigin: {
        horizontal: 'right',
        vertical: 'bottom'
      },
      targetOrigin: {
        horizontal: 'right',
        vertical: 'top'
      },
      anchorOriginType: {
        horizontal: 'left',
        vertical: 'bottom'
      },
      targetOriginType: {
        horizontal: 'left',
        vertical: 'top'
      }
    };
  }

  doTypeAction(type, e) {
    if (this.isActionsPrevented()) {
      return false;
    }
    const { column } = this.props;
    if (type === 'MIXED') {
      this.props.openDetailsWizard({detailType: 'SINGLE_DATA_TYPE', columnName: column.name});
    } else {
      this.handleTouchTap('openType', e);
    }
  }

  handleRequestClose = () => this.setState({ open: false });
  handleRequestCloseType = () => this.setState({ openType: false });

  handleTouchTap = (type, event) => {
    if (this.isActionsPrevented()) {
      return false;
    }
    const state = type === 'open'
      ? { open: true, anchorEl: event.currentTarget }
      : { openType: true, anchorElType: event.currentTarget };
    this.setState(state);
  };

  isActionsPrevented() {
    return this.props.pageType !== 'default' || this.props.isDumbTable;
  }

  replaceLibraryPositionMethodsWithOwn() {
    // TODO: move this logic to separate place with popover
    if (this.refs.menu) {
      this.refs.menu.applyAutoPositionIfNeeded = ColumnHeader.emulateAutoPosition;
    }
    if (this.refs.menuType) {
      this.refs.menuType.applyAutoPositionIfNeeded = ColumnHeader.emulateAutoPosition;
    }
  }

  handleFocus = () => {
    // WARNING This is a bit tricky
    // When ReactModal opens, it saves and blurs the current input.
    // Then, when it closes, it refocuses the saved input, which will trigger this handler again.
    // To avoid this messy situation, blur the input before starting preconfirm, then refocus it on confirm.

    if (!this.forceFocus) {
      // timeout to let the focus resolve before blurring. This allows it to keep the caret position.
      setTimeout(() => {
        if (this.input) {
          this.input.blur();
        }
        this.props.preconfirmTransform().then(() => {
          this.forceFocus = true;
          if (this.input) {
            this.input.focus();
          }
          //IE fix: timeout so forceFocus will be set to false after next handleFocus call
          setTimeout(() => {
            this.forceFocus = false;
          }, 0);
        });
      }, 0);
    }
  }

  handleUpdateColumnName(name, e) {
    if (e.target.value) {
      this.props.updateColumnName(name, e);
    }
  }

  handleKeyPress(name, e) { // todo: switch to KeyboardEvent.code (w/ polyfill)
    if (e.keyCode === Keys.ENTER) {
      this.handleUpdateColumnName(name, e);
      this.input.blur();
    } else if (e.keyCode === Keys.ESCAPE) {
      this.input.value = name;
      this.input.blur();
      e.preventDefault();
    }
  }

  handleRenameAction = () => {
    if (this.input) {
      this.input.focus();
    }
  }

  renderEditableColumnName(column, label, cellWidth) {
    const style = {
      width: !this.isActionsPrevented()
        ? cellWidth - MAX_COLUMN_NAME_LENTH + MARGIN_RIGHT
        : cellWidth - MAX_COLUMN_NAME_LENTH + ACTION_MENU_WIDTH + MARGIN_RIGHT,
      userSelect: this.props.isResizeInProgress ? 'none' : 'initial',
      ...styles.inputStyle,
      textDecoration: column.status === 'DELETED' ? 'line-through' : 'none'
    };
    return (
      <input
        className='cell'
        ref={(input) => this.input = input}
        type='text'
        disabled={this.isActionsPrevented() || this.props.isResizeInProgress}
        autoComplete='off'
        style={style}
        id={`cell${column.name}`}
        contentEditable
        onFocus={this.handleFocus}
        onKeyDown={this.handleKeyPress.bind(this, column.name)}
        onBlur={this.handleUpdateColumnName.bind(this, column.name)}
        defaultValue={label}/>
    );
  }

  renderColumnIcon(type, label) {
    if (type === '?') {
      return <span
        className='type'
        id={`${label} + type`}
        style={styles.other}>?</span>;
    }

    const {isDumbTable} = this.props;

    const canClick = !isDumbTable && type !== BINARY; // disable binary type conversions pending DX-5159
    return <FontIcon
      type={typeToIconType[type]}
      theme={styles.typeColumn}
      id={`${label} + type`}
      class='type'
      onClick={canClick ? this.doTypeAction.bind(this, type) : () => {}}
      style={canClick ? undefined : {cursor: 'default'}}
    />;
  }

  renderActionMenuIcon(column) {
    const preventHoverStyle = this.isActionsPrevented()
      ? {...styles.arrowDown.Container, ':hover': {}}
      : {};
    const activeStyle = this.state.open
      ? { Container: {...styles.arrowDown.Container, backgroundColor: EXPLORE_HOVER_COLOR} }
      : { Container: {...styles.arrowDown.Container, ...preventHoverStyle} };
    return this.isActionsPrevented()
      ? null
      : (
        <FontIcon
          theme={activeStyle}
          type='Arrow-Down-Small'
          key={column.name}
          onClick={this.handleTouchTap.bind(this, 'open')}/>
      );
  }

  renderActionMenu() {
    const { column } = this.props;
    this.replaceLibraryPositionMethodsWithOwn(); //TODO we should find better way to change autoposition
    const height = $('.fixed-data-table').height();
    return (
      <Popover
        ref='menu'
        animated={false}
        style={{...styles.popoverAnimation, ...ColumnHeader.getAnimationStyle(height)}}
        useLayerForClickAway={false}
        open={this.state.open}
        canAutoPosition
        anchorEl={this.state.anchorEl}
        anchorOrigin={this.state.anchorOrigin}
        targetOrigin={this.state.targetOrigin}
        onRequestClose={this.handleRequestClose}
        animation={PopoverAnimationVertical}>
        <div style={styles.popover}>
          <ColumnActionMenu
            columnType={column.type}
            columnName={column.name}
            hideDropdown={this.handleRequestClose}
            columnsCount={this.props.columnsCount}
            openDetailsWizard={this.props.openDetailsWizard}
            makeTransform={this.props.makeTransform}
            disabledButtons={[]}
            onRename={this.handleRenameAction}
          />
        </div>
      </Popover>
    );
  }

  renderTypeMenu() {
    const height = $('.fixed-data-table').height();
    const { column } = this.props;
    return (
      <Popover
        ref='menuType'
        animated={false}
        style={{...styles.popoverAnimation, ...ColumnHeader.getAnimationStyle(height)}}
        useLayerForClickAway={false}
        canAutoPosition
        open={this.state.openType}
        anchorEl={this.state.anchorElType}
        anchorOrigin={this.state.anchorOriginType}
        targetOrigin={this.state.targetOriginType}
        onRequestClose={this.handleRequestCloseType}
        animation={PopoverAnimationVertical}>
        <div style={styles.popover}>
          <ColumnTypeMenu
            columnType={column.type}
            columnName={column.name}
            hideDropdown={this.handleRequestCloseType}
            openDetailsWizard={this.props.openDetailsWizard}
            makeTransform={this.props.makeTransform}/>
        </div>
      </Popover>
    );
  }

  render() {
    const { column, width } = this.props;
    const label = column.name;
    const type = column.type;
    return (
      <Cell style={styles.themeStyle}>
        <DragSource
          nativeDragData={ColumnHeader.getDragData(label)}
          preventDrag={this.props.isResizeInProgress}
          dragType={this.props.dragType}
          id={label}>
          <div
            data-qa={label}
            key={label}
            style={styles.wrapperColumn}>
            <div style={styles.colWrap}>
              {this.renderColumnIcon(type, label)}
              {this.renderTypeMenu()}
              {this.renderEditableColumnName(column, label, width)}
            </div>
            {this.renderActionMenuIcon(column)}
            {this.renderActionMenu()}
          </div>
        </DragSource>
      </Cell>
    );
  }
}

const styles = {
  inputStyle: {
    height: COLUMN_HEIGHT,
    border: 'none',
    position: 'static',
    alignText: 'center',
    backgroundColor: '#F3F3F3',
    marginRight: 0
  },
  themeStyle: {
    backgroundColor: '#F3F3F3'
  },
  popoverAnimation: {
    // need this because of the case where group by or join button overlays column action menu
    zIndex: 2,
    transition: `transform 450ms cubic-bezier(0.23, 1, 0.32, 1) 0ms,
                 opacity 450ms cubic-bezier(0.23, 1, 0.32, 1) 0ms,
                 top 450ms cubic-bezier(0.23, 1, 0.32, 1) 0ms`
  },
  colWrap: {
    display: 'flex'
  },
  typeMixed: {
    'Icon': {
      height: 18,
      width: 24,
      position: 'relative',
      marginLeft: -1,
      top: 3,
      cursor: 'pointer',
      color: '#FFBB57'
    }
  },
  typeLabel: {
    cursor: 'pointer',
    textTransform: 'capitalize',
    paddingTop: 5
  },
  popover: {
    padding: 0,
    width: 168
  },
  typeColumn: {
    'Icon': {
      height: 18,
      width: 24,
      marginLeft: -2,
      position: 'relative',
      top: 3
    }
  },
  wrapperColumn: {
    position: 'static',
    display: 'flex',
    justifyContent: 'space-between'
  },
  other: {
    position: 'relative',
    top: 3,
    marginLeft: 5,
    marginRight: 3,
    cursor: 'pointer',
    opacity: 0.7
  },
  arrowDown: {
    Container: {
      position: 'static',
      float: 'right',
      height: 25,
      cursor: 'pointer',
      ':hover': {
        backgroundColor: EXPLORE_HOVER_COLOR
      }
    }
  }
};
