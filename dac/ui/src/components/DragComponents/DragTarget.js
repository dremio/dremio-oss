/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import { Component } from "react";
import PropTypes from "prop-types";
import { DropTarget } from "react-dnd";
import { findDOMNode } from "react-dom";
import classNames from "clsx";
import { base } from "@app/uiTheme/less/DragComponents/DragTarget.less";

const DEFAULT_TYPE = "groupBy";

const target = {
  drop(props, monitor) {
    const hasDroppedOnChild = monitor.didDrop();
    if (props.canDropOnChild && hasDroppedOnChild) return; //Child onDrop has been called already
    if (props.onDrop) {
      props.onDrop(monitor.getItem(), monitor);
    }
  },
  hover(props, monitor, component) {
    const dragIndex = monitor.getItem().index;
    const hoverIndex = props.index;

    if (props.onHover) {
      props.onHover(monitor.getItem());
    }

    if (dragIndex === hoverIndex) {
      return;
    }
    const hoverBoundingRect = findDOMNode(component).getBoundingClientRect(); // eslint-disable-line react/no-find-dom-node
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
  },
};

@DropTarget((props) => props.dragType, target, (connect, monitor) => {
  return {
    connectDropTarget: connect.dropTarget(),
    isOver: monitor.isOver(),
    canDrop: monitor.canDrop(),
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
    className: PropTypes.string,
    dragTargetHoverCls: PropTypes.string,
  };

  static defaultProps = {
    canDropOnChild: false,
    dragType: DEFAULT_TYPE,
    isOver: false,
  };

  UNSAFE_componentWillReceiveProps(nextProps) {
    if (this.props.isOver !== nextProps.isOver) {
      if (nextProps.isOver && this.props.onEnter) this.props.onEnter();
      if (!nextProps.isOver && this.props.onLeave) this.props.onLeave();
    }
  }

  render() {
    const { className, isOver, dragTargetHoverCls = "" } = this.props;
    const classname = classNames(base, className, {
      [dragTargetHoverCls]: isOver,
    });
    return this.props.connectDropTarget(
      <div onDragOver={this.props.onDragOver} className={classname}>
        {this.props.children}
      </div>
    );
  }
}
