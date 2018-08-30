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
import { findDOMNode } from 'react-dom';
import classNames from 'classnames';
import Immutable  from 'immutable';
import Radium from 'radium';
import PropTypes from 'prop-types';
import { DragSource, DropTarget } from 'react-dnd';
import { injectIntl } from 'react-intl';

import Art from 'components/Art';

import { typeToIconType, typeToFormatMessageId } from 'constants/DataTypes';

import './Column.less';

const COLUMN = 'COLUMN';

const columnSource = {
  beginDrag(props) {
    return {
      id: props.id,
      index: props.index
    };
  },
  canDrag(props) {
    return props.canDrag;
  }
};

const columnTarget = {
  hover(props, monitor, component) {
    const dragIndex = monitor.getItem().index;
    const hoverIndex = props.index;

    // Don't replace items with themselves
    if (dragIndex === hoverIndex) {
      return;
    }

    // Determine rectangle on screen
    const hoverBoundingRect = findDOMNode(component).getBoundingClientRect();

    // Get vertical middle
    const hoverMiddleY = (hoverBoundingRect.bottom - hoverBoundingRect.top) / 2;

    // Determine mouse position
    const clientOffset = monitor.getClientOffset();

    // Get pixels to the top
    const hoverClientY = clientOffset.y - hoverBoundingRect.top;

    // Dragging downwards
    if (dragIndex < hoverIndex && hoverClientY < hoverMiddleY) {
      return;
    }

    // Dragging upwards
    if (dragIndex > hoverIndex && hoverClientY > hoverMiddleY) {
      return;
    }
    // Time to actually perform the action
    props.moveColumn(dragIndex, hoverIndex);

    monitor.getItem().index = hoverIndex;
  }
};

@injectIntl
@Radium
class Column extends Component {
  static propTypes = {
    allowDrag: PropTypes.func.isRequired,
    column: PropTypes.instanceOf(Immutable.Map).isRequired,
    connectDragSource: PropTypes.func.isRequired,
    connectDropTarget: PropTypes.func.isRequired,
    index: PropTypes.number.isRequired,
    isDragging: PropTypes.bool.isRequired,
    id: PropTypes.any.isRequired,
    moveColumn: PropTypes.func.isRequired,
    onToggleVisible: PropTypes.func.isRequired,
    showHover: PropTypes.func,
    updateHover: PropTypes.func,
    hideHover: PropTypes.func,
    intl: PropTypes.object.isRequired
  }

  onToggleVisible = () => {
    this.props.onToggleVisible(this.props.index);
  }

  render() {
    const { column, connectDragSource, connectDropTarget, isDragging, intl } = this.props;
    const classes = classNames('popup-column', {'column-draging': isDragging});
    const icon = column.get('hidden') ? 'VisibilityOff' : 'Visibility';
    const iconAlt = intl.formatMessage({id: `Common.${column.get('hidden') ? 'Hidden' : 'Visible'}`});
    const textStyle = !column.get('hidden') ? style.textVisible : style.textInvisible;

    return connectDragSource(connectDropTarget(
      <div className='column-elem' data-qa={`drag-line.${this.props.index}`}>
        <div className={classes}  style={[style.wrapper, textStyle]}>
          <div className='toogle-visible'
            onMouseUp={this.props.allowDrag}
            onMouseDown={this.onToggleVisible}
          >
            <Art src={`${icon}.svg`} alt={iconAlt} style={style.icon} />
          </div>
          <div
            onMouseMove={this.props.updateHover}
            onMouseEnter={this.props.showHover}
            onMouseLeave={this.props.hideHover}
            className='wrapper-tree'>
            <Art
              src={`types/${typeToIconType[column.get('type')]}.svg`}
              alt={intl.formatMessage({id: `${typeToFormatMessageId[column.get('type')]}`})}
              style={style.container} />
            <div style={style.textElement}>{column.get('name')}</div>
          </div>
        </div>
      </div>
    ));
  }
}

const dropColumn = DropTarget(COLUMN, columnTarget, connect => ({
  connectDropTarget: connect.dropTarget()
}))(Column);

export default DragSource(COLUMN, columnSource, (connect, monitor) => ({
  connectDragSource: connect.dragSource(),
  isDragging: monitor.isDragging()
}))(dropColumn);

const style = {
  icon: {
    fontSize: 18,
    width: 26,
    marginRight: 50,
    height: 26
  },
  container: {
    width: 30,
    height: 30
  },
  wrapper: {
    display: 'flex',
    marginLeft: -10,
    ':hover': {
      backgroundColor: '#E2E2E2',
      zIndex: 100
    }
  },
  typeIcon: {
    width: 24,
    height: 18
  },
  textElement: {
    display: 'inline-block',
    top: -3,
    marginLeft: 5
  },
  textVisible: {
  },
  textInvisible: {
    color: '#999'
  }
};
