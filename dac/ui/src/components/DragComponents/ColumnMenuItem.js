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
import pureRender from 'pure-render-decorator';
import Radium from 'radium';
import Immutable from 'immutable';

import EllipsedText from 'components/EllipsedText';
import FontIcon from 'components/Icon/FontIcon';
import { body, unavailable } from 'uiTheme/radium/typography';
import { ACTIVE_DRAG_AREA, BORDER_ACTIVE_DRAG_AREA } from 'uiTheme/radium/colors';
import { typeToIconType } from 'constants/DataTypes';
import { constructFullPath } from 'utils/pathUtils';

import DragSource from './DragSource';
import './ColumnMenuItem.less';

@pureRender
@Radium
export default class ColumnMenuItem extends Component {
  static propTypes = {
    item: PropTypes.instanceOf(Immutable.Map).isRequired,
    disabled: PropTypes.bool,
    fullPath: PropTypes.instanceOf(Immutable.List),
    dragType: PropTypes.string.isRequired,
    handleDragStart: PropTypes.func,
    onDragEnd: PropTypes.func,
    type: PropTypes.string,
    index: PropTypes.number,
    nativeDragData: PropTypes.object,
    preventDrag: PropTypes.bool,
    name: PropTypes.string,
    fieldType: PropTypes.string
  }
  static defaultProps = {
    fullPath: Immutable.List()
  }

  checkThatDragAvailable = (e) => {
    if (this.props.preventDrag || this.props.disabled) {
      e.stopPropagation();
      e.preventDefault();
    }
  }

  renderDraggableIcon() {
    return !this.props.preventDrag && !this.props.disabled ? <FontIcon theme={theme.Draggable}
      type='Draggable' class='draggable-icon'/> : null;
  }

  render() {
    const { item, disabled, preventDrag, fieldType } = this.props;
    const font = disabled
      ? unavailable
      : body;
    const disabledHoverStyle = preventDrag || disabled
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
    const isGroupBy = this.props.dragType === 'groupBy';
    // full paths are not yet supported by dremio in SELECT clauses, so force this to always be the simple name for now
    const idForDrag = true || // eslint-disable-line no-constant-condition
      isGroupBy ? item.get('name') : constructFullPath(this.props.fullPath.concat(item.get('name')));
    return (
      <div
        className='inner-join-left-menu-item'
        style={[styles.base]}
        onMouseDown={this.checkThatDragAvailable}
      >
        <DragSource
          nativeDragData={this.props.nativeDragData}
          dragType={this.props.dragType}
          type={this.props.type}
          index={this.props.index}
          onDragStart={this.props.handleDragStart}
          onDragEnd={this.props.onDragEnd}
          preventDrag={preventDrag}
          isFromAnother
          id={idForDrag}>
          <div
            style={[styles.content, disabledHoverStyle]}
            className='draggable-row'
            data-qa={`inner-join-field-${item.get('name')}-${fieldType}`}
          >
            <FontIcon type={typeToIconType[item.get('type')]} theme={styles.type}/>
            <EllipsedText style={!preventDrag ? {paddingRight: 10} : {} /* leave space for knurling */}
              text={item.get('name')}>
              <span data-qa={item.get('name')} style={[styles.name, font]}>{item.get('name')}</span>
            </EllipsedText>
            {this.renderDraggableIcon()}
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
      top: 0,
      flex: '0 0 auto'
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
