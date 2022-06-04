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
import { PureComponent } from 'react';
import { connect }   from 'react-redux';
import PropTypes from 'prop-types';
import Immutable from 'immutable';

import { loadResourceTree } from 'actions/resources/tree';
import { getResourceTree } from 'selectors/tree';
import { fetchScripts } from 'actions/resources/scripts';

import { constructFullPath, splitFullPath } from 'utils/pathUtils';

import { getSortedSources } from '@app/selectors/home';
import { getRefQueryParams } from '@app/utils/nessieUtils';
import ResourceTree from './ResourceTree';

export class ResourceTreeController extends PureComponent {
  static propTypes = {
    resourceTree: PropTypes.instanceOf(Immutable.List),
    sources: PropTypes.instanceOf(Immutable.List), //Loaded from parent
    isDatasetsDisabled: PropTypes.bool,
    hideDatasets: PropTypes.bool,
    hideSpaces: PropTypes.bool,
    hideSources: PropTypes.bool,
    hideHomes: PropTypes.bool,
    loadResourceTree: PropTypes.func.isRequired,
    onChange: PropTypes.func,
    insertFullPathAtCursor: PropTypes.func,
    sidebarCollapsed: PropTypes.bool,
    isCollapsable: PropTypes.bool,
    handleSidebarCollapse: PropTypes.func,
    preselectedNodeId: PropTypes.string,
    dragType: PropTypes.string,
    style: PropTypes.object,
    browser: PropTypes.bool,
    isExpandable: PropTypes.bool,
    shouldShowOverlay: PropTypes.bool,
    shouldAllowAdd: PropTypes.bool,
    isSqlEditorTab: PropTypes.bool,
    fetchScripts: PropTypes.func,
    nessie: PropTypes.object
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
    const { isSqlEditorTab } = this.props;

    if (isSqlEditorTab && this.props.fetchScripts) {
      this.props.fetchScripts();
    }

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

  componentDidUpdate(prevProps) {
    const { isSqlEditorTab } = this.props;
    if (isSqlEditorTab && !prevProps.isSqlEditorTab) {
      this.props.fetchScripts();
    }

    if (prevProps.nessie !== this.props.nessie) { //Collapse nodes when nessie state is changed
      this.setState({ expandedNodes: Immutable.List() }); //eslint-disable-line
    }
  }

  loadResourceTree = (path = '', isExpand) => {
    const showDatasets = !this.props.hideDatasets;
    const showSpaces = !this.props.hideSpaces;
    const showSources = !this.props.hideSources;
    const showHomes = !this.props.hideHomes;
    const [sourceName] = path.split('.');
    const params = getRefQueryParams(this.props.nessie, sourceName.replace(/"/g, ''));
    return this.props.loadResourceTree(path, { showDatasets, showSpaces, showSources, showHomes, isExpand, params });
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
      nodeIdsToOpen.add(selectedNodeId);
    }

    this.loadResourceTree(selectedNodeId);

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
        sources={this.props.sources}
        selectedNodeId={this.state.selectedNodeId}
        dragType={this.props.dragType}
        formatIdFromNode={ResourceTreeController.formatIdFromNode}
        isNodeExpanded={this.isNodeExpanded}
        handleSelectedNodeChange={this.handleSelectedNodeChange}
        handleNodeClick={this.handleNodeClick}
        browser={this.props.browser}
        isExpandable={this.props.isExpandable}
        shouldShowOverlay={this.props.shouldShowOverlay}
        shouldAllowAdd={this.props.shouldAllowAdd}
        insertFullPathAtCursor={this.props.insertFullPathAtCursor}
        isSqlEditorTab={this.props.isSqlEditorTab}
        sidebarCollapsed={this.props.sidebarCollapsed}
        handleSidebarCollapse={this.props.handleSidebarCollapse}
        isCollapsable={this.props.isCollapsable}
      />
    );
  }
}

const mapStateToProps = (state) => ({
  resourceTree: getResourceTree(state),
  sources: getSortedSources(state),
  nessie: state.nessie
});

export default connect(mapStateToProps, {
  fetchScripts,
  loadResourceTree
})(ResourceTreeController);
