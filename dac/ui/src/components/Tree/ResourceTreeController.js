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
import { connect }   from 'react-redux';
import PureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Immutable from 'immutable';

import { loadResourceTree } from 'actions/resources/tree';
import { getResourceTree } from 'selectors/tree';

import { constructFullPath, splitFullPath } from 'utils/pathUtils';

import ResourceTree from './ResourceTree';

@PureRender
export class ResourceTreeController extends Component {
  static propTypes = {
    resourceTree: PropTypes.instanceOf(Immutable.List),
    isDatasetsDisabled: PropTypes.bool,
    hideDatasets: PropTypes.bool,
    hideSpaces: PropTypes.bool,
    hideSources: PropTypes.bool,
    hideHomes: PropTypes.bool,
    loadResourceTree: PropTypes.func,
    onChange: PropTypes.func,
    preselectedNodeId: PropTypes.string,
    dragType: PropTypes.string,
    style: PropTypes.object
  };

  static formatIdFromNode = (node) => constructFullPath(node.get('fullPath'), false)

  constructor(props) {
    super(props);

    this.state = {
      selectedNodeId: props.preselectedNodeId || '',
      expandedNodes: Immutable.List()
    };
  }

  componentWillMount() {
    const { selectedNodeId } = this.state;

    if (selectedNodeId && selectedNodeId !== 'tmp') {
      this.loadResourceTree(selectedNodeId, true).then((res) => {
        if (res.error && !this.isTreeloadRetried) {
          this.isTreeloadRetried = true;
          this.loadResourceTree();
        }
        if (this.props.onChange) {
          this.props.onChange(selectedNodeId);
        }
      });

      this.expandPathToSelectedNode(selectedNodeId);
    } else {
      this.loadResourceTree();
    }
  }

  loadResourceTree = (path = '', isExpand) => {
    const showDatasets = !this.props.hideDatasets;
    const showSpaces = !this.props.hideSpaces;
    const showSources = !this.props.hideSources;
    const showHomes = !this.props.hideHomes;
    return this.props.loadResourceTree(path, { showDatasets, showSpaces, showSources, showHomes, isExpand });
  }

  handleSelectedNodeChange = (selectedNodeId, node) => {
    if (this.props.onChange) {
      this.props.onChange(selectedNodeId, node);
    }
    this.setState({ selectedNodeId });
  }

  isNodeExpanded = (node) => {
    const nodeId = ResourceTreeController.formatIdFromNode(node);
    return Boolean(this.state.expandedNodes.find(expNodeId => expNodeId === nodeId));
  }

  expandPathToSelectedNode = (path) => {
    const parentsFullPathList = splitFullPath(path);
    const parents = parentsFullPathList
    .slice(0, parentsFullPathList.length - 1)
    .map((node, index, curArr) => {
      return constructFullPath(curArr.slice(0, index + 1));
    });
    this.setState({
      expandedNodes: this.state.expandedNodes.concat(Immutable.fromJS(parents))
    });
  }

  handleNodeClick = (clickedNode) => {
    const selectedNodeId = ResourceTreeController.formatIdFromNode(clickedNode);

    const shouldExpandNode = !this.isNodeExpanded(clickedNode);

    const nodeIdsToClose = new Set();
    const nodeIdsToOpen = new Set();

    if (!shouldExpandNode) {
      // TODO: DX-6992 ResourceTreeController should maintain child disclosure
      // see notes there for info why this code is here right now
      const findOpenChildren = (node) => {
        const resources = node.get('resources');
        if (!resources) return; // DS leaf OR unloaded part of tree
        nodeIdsToClose.add(ResourceTreeController.formatIdFromNode(node));
        resources.forEach(findOpenChildren);
      };
      findOpenChildren(clickedNode);
    } else {
      this.loadResourceTree(selectedNodeId);
      nodeIdsToOpen.add(selectedNodeId);
    }

    this.setState((state) => ({
      selectedNodeId,
      expandedNodes: state.expandedNodes.filter(nodeId => !nodeIdsToClose.has(nodeId)).push(...nodeIdsToOpen)
    }));
  }

  render() {
    return (
      <ResourceTree
        isDatasetsDisabled={this.props.isDatasetsDisabled}
        style={this.props.style}
        resourceTree={this.props.resourceTree}
        selectedNodeId={this.state.selectedNodeId}
        dragType={this.props.dragType}
        formatIdFromNode={ResourceTreeController.formatIdFromNode}
        isNodeExpanded={this.isNodeExpanded}
        handleSelectedNodeChange={this.handleSelectedNodeChange}
        handleNodeClick={this.handleNodeClick}
      />
    );
  }
}

const mapStateToProps = (state) => ({
  resourceTree: getResourceTree(state)
});

export default connect(mapStateToProps, {
  loadResourceTree
})(ResourceTreeController);
