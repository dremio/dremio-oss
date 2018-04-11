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

import ExploreDragArea from 'pages/ExplorePage/components/ExploreDragArea';

import JoinDragAreaColumn from './JoinDragAreaColumn';

const DEFAULT_DRAG_AREA_TEXT = 'Drag and drop field here or click “Add a Join Condition”.';

@pureRender
@Radium
class JoinDragArea extends Component {
  static propTypes = {
    style: PropTypes.object,
    dragColumntableType: PropTypes.string,
    items: PropTypes.instanceOf(Immutable.List),
    leftColumns: PropTypes.instanceOf(Immutable.List).isRequired,
    rightColumns: PropTypes.instanceOf(Immutable.List).isRequired,
    handleDrop: PropTypes.func,
    removeColumn: PropTypes.func,
    moveColumn: PropTypes.func,
    dragType: PropTypes.string,
    columnDragName: PropTypes.string,
    isDragInProgress: PropTypes.bool,
    addColumn: PropTypes.func
  };

  constructor(props) {
    super(props);

    this.handleDrop = this.handleDrop.bind(this);
  }


  handleDrop(data) {
    if (!this.isDragColumnAlreadyAdded()) {
      this.props.handleDrop(data);
    }
  }

  isDragColumnAlreadyAdded() {
    return !!this.props.items.find(column => column.get('name') === this.props.columnDragName);
  }

  renderColumnsForDragArea() {
    return this.props.items.map((item, i) => (
      <JoinDragAreaColumn
        dragColumntableType={this.props.dragColumntableType}
        items={this.props.items}
        isDragInProgress={this.props.isDragInProgress}
        addColumn={this.props.addColumn}
        leftColumns={this.props.leftColumns}
        rightColumns={this.props.rightColumns}
        key={i}
        index={i}
        type='measures'
        removeColumn={this.props.removeColumn}
        moveColumn={this.props.moveColumn}
        dragType={this.props.dragType}
        item={item}
      />
    ));
  }

  render() {
    const isEmpty = !this.props.items.size;
    return (
      <ExploreDragArea
        dragType={this.props.dragType}
        onDrop={this.handleDrop}
        isDragged={this.props.isDragInProgress && !this.isDragColumnAlreadyAdded()}
        emptyDragAreaText={DEFAULT_DRAG_AREA_TEXT}
        dragContentStyle={!isEmpty ? style.dragContent.base : undefined}
      >
        {this.renderColumnsForDragArea()}
      </ExploreDragArea>
    );
  }
}

const style = {
  dragContent: {
    base: {
      borderLeftWidth: '0',
      borderRightWidth: '0',
      borderTopWidth: '1px',
      borderBottomWidth: '1px'
    }
  }
};

export default JoinDragArea;
