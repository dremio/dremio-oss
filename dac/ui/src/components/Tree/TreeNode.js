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
import Radium from 'radium';
import PropTypes from 'prop-types';
import Immutable from 'immutable';

@Radium
export default class TreeNode extends Component {
  static propTypes = {
    node: PropTypes.instanceOf(Immutable.Map),
    renderNode: PropTypes.func,
    isNodeExpanded: PropTypes.func,
    selectedNodeId: PropTypes.string
  };

  renderResources() {
    return (this.props.node.get('resources') || Immutable.List()).map((resource, index) => (
      <TreeNode
        node={resource}
        key={index}
        renderNode={this.props.renderNode}
        isNodeExpanded={this.props.isNodeExpanded}
        selectedNodeId={this.props.selectedNodeId}
      />
    ));
  }

  render() {
    const { node, isNodeExpanded } = this.props;
    const styles = node.get('fullPath') && node.get('fullPath').size === 1 ? style.root : style.base;
    return (
      <div style={styles}>
        {this.props.renderNode(node)}
        {isNodeExpanded(node) && this.renderResources()}
      </div>
    );
  }
}

const style = {
  base: {
    padding: '0 0 0 30px',
    width: '100%',
    ':hover': {
      backgroundColor: '#FFF5DC'
    }
  },
  root: {
    padding: 0
  },
  emptySource: {
    paddingLeft: 30,
    fontSize: 12
  },
  failed: {
    color: 'red'
  },
  spinner: {
    position: 'relative',
    display: 'block',
    paddingLeft: 30
  },
  spinnerIconStyle: {
    width: 25,
    height: 25
  }
};
