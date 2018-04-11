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

import FontIcon from 'components/Icon/FontIcon';
import { unavailable } from 'uiTheme/radium/typography';
import { ACTIVE_DRAG_AREA, BORDER_ACTIVE_DRAG_AREA } from 'uiTheme/radium/colors';
import { typeToIconType } from 'constants/DataTypes';

import DragSource from 'components/DragComponents/DragSource';
import './ColumnMenuItem.less';

@pureRender
@Radium
export default class ColumnMenuItem extends Component {
  static propTypes = {
    item: PropTypes.instanceOf(Immutable.Map).isRequired,
    dragType: PropTypes.string.isRequired,
    handleDragStart: PropTypes.func,
    onDragEnd: PropTypes.func,
    type: PropTypes.string,
    index: PropTypes.number,
    nativeDragData: PropTypes.object
  }

  constructor(props) {
    super(props);
    this.checkThatDragAvailible = this.checkThatDragAvailible.bind(this);
  }

  checkThatDragAvailible(e) {
    if (this.props.item.get('disabled')) {
      e.stopPropagation();
      e.preventDefault();
    }
  }

  renderDraggableIcon(item) {
    return !item.get('disabled') ? <FontIcon theme={theme.Draggable}
      type='Draggable' class='draggable-icon'/> : null;
  }

  render() {
    const { item } = this.props;
    const font = item.get('disabled')
      ? unavailable
      : {};
    const disabledHoverStyle = item.get('disabled')
      ? {
        cursor: 'default',
        ':hover': {
          backgroundColor: 'transparent',
          boxShadow: 'none)',
          borderLeft: '1px solid transparent',
          borderRight: '1px solid transparent',
          borderTop: '1px solid transparent',
          borderBottom: '1px solid transparent'
        }
      }
      : {};
    return (
      <div className='inner-join-left-menu-item'
        style={[styles.base]}
        onMouseDown={this.checkThatDragAvailible}>
        <DragSource
          nativeDragData={this.props.nativeDragData}
          dragType={this.props.dragType}
          type={this.props.type}
          index={this.props.index}
          onDragStart={this.props.handleDragStart}
          onDragEnd={this.props.onDragEnd}
          isFromAnother
          id={item.get('name')}>
          <div style={[styles.content, disabledHoverStyle]} className='draggable-row'>
            <FontIcon type={typeToIconType[item.get('type')]} theme={styles.type}/>
            <span style={[styles.name, font]}>{item.get('name')}</span>
            {this.renderDraggableIcon(item)}
          </div>
        </DragSource>
      </div>
    );
  }
}

const styles = {
  base: {
    display: 'flex',
    width: '100%',
    minHeight: 25
  },
  content: {
    position: 'relative',
    display: 'flex',
    width: '100%',
    borderLeft: '1px solid transparent',
    borderRight: '1px solid transparent',
    borderTop: '1px solid transparent',
    borderBottom: '1px solid transparent',
    marginTop: 2,
    marginBottom: 2,
    cursor: 'move',
    height: 25,
    alignItems: 'center',
    ':hover': {
      backgroundColor: ACTIVE_DRAG_AREA,
      boxShadow: '0px 0px 4px 0px rgba(0,0,0,0.10)',
      borderLeft: `1px solid ${BORDER_ACTIVE_DRAG_AREA}`,
      borderRight: `1px solid ${BORDER_ACTIVE_DRAG_AREA}`,
      borderTop: `1px solid ${BORDER_ACTIVE_DRAG_AREA}`,
      borderBottom: `1px solid ${BORDER_ACTIVE_DRAG_AREA}`
    }
  },
  type: {
    'Icon': {
      width: 24,
      height: 20,
      backgroundPosition: 'left center'
    },
    Container: {
      width: 28,
      height: 20,
      top: 0
    }
  },
  name: {
    marginLeft: 5
  }
};

const theme = {
  Draggable: {
    Icon: {
      width: 10
    }
  }
};
