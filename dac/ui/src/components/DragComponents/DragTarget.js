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
import { DropTarget } from 'react-dnd';
import { findDOMNode } from 'react-dom';
import classNames from 'classnames';
import { base } from './DragTarget.less';

const DEFAULT_TYPE = 'groupBy';

const target = {
  drop(props, monitor) {
    const hasDroppedOnChild = monitor.didDrop();
    if (props.onDrop && (props.canDropOnChild || !hasDroppedOnChild)) {
      props.onDrop(monitor.getItem(), monitor);
    }
  },
  hover(props, monitor, component) {
    const dragIndex = monitor.getItem().index;
    const hoverIndex = props.index;

    if (dragIndex === hoverIndex) {
      return;
    }
    const hoverBoundingRect = findDOMNode(component).getBoundingClientRect();
    const hoverMiddleY = (hoverBoundingRect.bottom - hoverBoundingRect.top) / 2;
    const clientOffset = monitor.getClientOffset();
    const hoverClientY = clientOffset.y - hoverBoundingRect.top;
    if (dragIndex < hoverIndex && hoverClientY < hoverMiddleY) {
      return;
    }
    if (dragIndex > hoverIndex && hoverClientY > hoverMiddleY) {
      return;
    }
    if (props.moveColumn) {
      props.moveColumn(dragIndex, hoverIndex);
      monitor.getItem().index = hoverIndex;
    }
  }
};

@DropTarget(props => props.dragType, target, (connect, monitor) => {
  return {
    connectDropTarget: connect.dropTarget(),
    isOver: monitor.isOver(),
    canDrop: monitor.canDrop()
  };
})
export default class DragTargetWrap extends Component {
  static propTypes = {
    dragType: PropTypes.string.isRequired,
    onDragOver: PropTypes.func,
    onDrop: PropTypes.func,
    onEnter: PropTypes.func,
    onLeave: PropTypes.func,
    moveColumn: PropTypes.func,
    id: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
    connectDropTarget: PropTypes.func,
    children: PropTypes.node,
    canDropOnChild: PropTypes.bool,
    isOver: PropTypes.bool,
    className: PropTypes.string
  };

  static defaultProps = {
    canDropOnChild: false,
    dragType: DEFAULT_TYPE,
    isOver: false
  };

  componentWillReceiveProps(nextProps) {
    if (this.props.isOver !== nextProps.isOver) {
      if (nextProps.isOver && this.props.onEnter) this.props.onEnter();
      if (!nextProps.isOver && this.props.onLeave) this.props.onLeave();
    }
  }

  render() {
    const {
      className
    } = this.props;
    return this.props.connectDropTarget(
      <div onDragOver={this.props.onDragOver} className={classNames([base, className])}>
        {this.props.children}
      </div>
    );
  }
}
