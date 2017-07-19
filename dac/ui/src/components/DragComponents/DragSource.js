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
import { DragSource } from 'react-dnd';

const source = {
  beginDrag(props) {
    if (props.onDragStart) {
      props.onDragStart(props);
    }
    return {
      id: props.id,
      args: props.args,
      isFromAnother: props.isFromAnother,
      index: props.index,
      type: props.type
    };
  },
  endDrag(props) {
    if (props.onDragEnd) {
      props.onDragEnd(props);
    }
    return {
      id: props.id,
      args: props.args,
      isFromAnother: props.isFromAnother,
      index: props.index
    };
  }
};

@DragSource(props => props.dragType, source, (connect, monitor) => ({
  connectDragSource: connect.dragSource(),
  isDragging: monitor.isDragging()
}))
export default class DragSourceWrap extends Component {
  static propTypes = {
    dragType: PropTypes.string.isRequired,
    nativeDragData: PropTypes.object,
    isFromAnother: PropTypes.bool,
    isDragging: PropTypes.bool,
    preventDrag: PropTypes.bool,
    connectDragSource: PropTypes.func,
    index: PropTypes.number,
    args: PropTypes.string,
    id: PropTypes.any,
    children: PropTypes.node
  };

  constructor(props) {
    super(props);
    this.onDragStart = this.onDragStart.bind(this);
  }

  onDragStart(ev) {
    if (this.props.nativeDragData) {
      // dataType must be 'text' for IE
      ev.dataTransfer.setData('text', JSON.stringify(this.props.nativeDragData));
    }
  }

  render() {
    const style = {
      width: '100%',
      userSelect: 'none',
      opacity: this.props.isDragging
        ? 0
        : 1
    };

    const content = (
      <div style={style} onDragStart={this.onDragStart}>
        {this.props.children}
      </div>
    );

    if (this.props.preventDrag) {
      return content;
    }

    return this.props.connectDragSource(
      content
    );
  }
}
