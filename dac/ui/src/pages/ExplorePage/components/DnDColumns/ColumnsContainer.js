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
import Immutable from 'immutable';
import pureRender from 'pure-render-decorator';
import Radium from 'radium';

import Column from './Column';

@Radium
@pureRender
class ColumnsContainer extends Component {
  static propTypes = {
    resources: PropTypes.array.isRequired,
    allowDrag: PropTypes.func.isRequired,
    canDrag: PropTypes.bool.isRequired,
    columns: PropTypes.instanceOf(Immutable.List).isRequired,
    moveColumn: PropTypes.func.isRequired,
    onToggleVisible: PropTypes.func.isRequired
  };

  constructor(props) {
    super(props);
    this.updateHover = this.updateHover.bind(this);
    this.showHover = this.showHover.bind(this);
    this.hideHover = this.hideHover.bind(this);
    this.state = {
      opacity: 0,
      top: 0,
      activeTop: 0,
      activeOpacity: 0
    };
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.resources !== this.props.resources) {
      this.setState({
        activeTop: 0,
        activeOpacity: 0
      });
    }
  }

  hideHover() {
    this.setState({
      opacity: 0
    });
  }

  showHover() {
    this.setState({
      opacity: 1
    });
  }

  updateHover(e) {
    let target = e.target;
    if (target.className.indexOf('node-text') !== -1 || target.className.indexOf('arrow') !== -1) {
      target = e.target.parentElement;
    }
    const position = $(target).position();
    this.setState({
      top: position.top + $(this.refs.resource).scrollTop()
    });
  }

  renderColumn(column, index) {
    const resource = [];
    resource.push(this.props.resources[index]);
    return (
      <Column
        key={column.get('index')}
        index={index}
        canDrag={this.props.canDrag}
        onToggleVisible={this.props.onToggleVisible}
        allowDrag={this.props.allowDrag}
        id={column.get('index')}
        column={column}
        moveColumn={this.props.moveColumn}
        resources={resource}
        onMouseMove={this.updateHover}
        onMouseEnter={this.showHover}
        onMouseLeave={this.hideHover}
        settings={this.state}/>
    );
  }

  render() {
    const {columns} = this.props;
    const renderColumns = columns.map((item, index) => {
      return this.renderColumn(item, index);
    });
    return (
      <div
        className='resource-tree'
        ref='resource'
        style={style}>
        <div className='hover' style={{top: this.state.top, opacity: this.state.opacity}}></div>
        {renderColumns}
      </div>
    );
  }
}

const style = {
  maxHeight: 300,
  minHeight: 200,
  marginTop: 10,
  overflow: 'auto'
};

export default ColumnsContainer;
