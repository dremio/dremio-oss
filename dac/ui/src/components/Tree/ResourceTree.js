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
import { Component } from 'react';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import classNames from 'classnames';
import Radium from 'radium';
import { injectIntl } from 'react-intl';

import { CONTAINER_ENTITY_TYPES } from '@app/constants/Constants';

import Art from 'components/Art';
import DragSource from 'components/DragComponents/DragSource';
import DatasetItemLabel from 'components/Dataset/DatasetItemLabel';
import EllipsedText from 'components/EllipsedText';
import { getIconTypeByEntityTypeAndStatus } from 'utils/iconUtils';
import { PureEntityIcon } from '@app/pages/HomePage/components/EntityIcon';
import TreeNodeBranchPicker from './components/TreeNodeBranchPicker/TreeNodeBranchPicker';

import Tree from './Tree';

import './ResourceTree.less';
import TreeBrowser from './TreeBrowser';

@injectIntl
@Radium
export default class ResourceTree extends Component {
  static propTypes = {
    resourceTree: PropTypes.instanceOf(Immutable.List),
    sources: PropTypes.instanceOf(Immutable.List),
    selectedNodeId: PropTypes.string,
    isDatasetsDisabled: PropTypes.bool,
    dragType: PropTypes.string,
    formatIdFromNode: PropTypes.func,
    isNodeExpanded: PropTypes.func,
    handleSelectedNodeChange: PropTypes.func,
    handleNodeClick: PropTypes.func,
    insertFullPathAtCursor: PropTypes.func,
    sidebarCollapsed: PropTypes.bool,
    handleSidebarCollapse: PropTypes.func,
    style: PropTypes.object,
    intl: PropTypes.object.isRequired,
    browser: PropTypes.bool,
    isExpandable: PropTypes.bool,
    shouldShowOverlay: PropTypes.bool,
    shouldAllowAdd: PropTypes.bool,
    isSqlEditorTab: PropTypes.bool,
    isCollapsable: PropTypes.bool
  };

  static isNodeExpandable = (node) => CONTAINER_ENTITY_TYPES.has(node.get('type'))

  handleSelectedNodeChange = (node) => {
    if (ResourceTree.isNodeExpandable(node)) {
      this.props.handleNodeClick(node);
    }
    this.props.handleSelectedNodeChange(this.props.formatIdFromNode(node), node);
  }

  addtoEditor = (id) => {
    this.props.insertFullPathAtCursor(id);
  }

  renderNode = (node, containerEl) => {
    const nodeId = this.props.formatIdFromNode(node);
    const isDisabled = this.props.isDatasetsDisabled && !CONTAINER_ENTITY_TYPES.has(node.get('type'));
    const arrowIconType = this.props.isNodeExpanded(node) ? 'ArrowDown' : 'ArrowRight';
    const arrowAlt = this.props.isNodeExpanded(node) ? 'Undisclosed' : 'Disclosed';

    const arrowIconStyle = styles.arrow;

    const iconForArrow = ResourceTree.isNodeExpandable(node)
      ? <Art src={`${arrowIconType}.svg`} alt={this.props.intl.formatMessage({ id: `Common.${arrowAlt}` })} style={arrowIconStyle} />
      : null;

    const iconForAdding = ResourceTree.isNodeExpandable(node) && this.props.shouldAllowAdd
      ? <Art
        className='resourceTreeNode__add'
        src='CirclePlus.svg'
        alt=''
        onClick={() => this.addtoEditor(node.get('fullPath'))}
        title='Add to SQL editor'
      />
      : null;

    const activeClass = this.props.selectedNodeId === nodeId ? 'active-node' : '';
    const nodeStatus = node.getIn(['state', 'status'], null);
    const iconType = getIconTypeByEntityTypeAndStatus(node.get('type'), nodeStatus);

    const nodeElement = ResourceTree.isNodeExpandable(node) ?
      <div className='resourceTreeNode node'>
        <PureEntityIcon entityType={node.get('type')} sourceStatus={nodeStatus} style={styles.icon} />
        <EllipsedText className='node-text' style={styles.text} text={node.get('name')} />
        <TreeNodeBranchPicker sources={this.props.sources} containerEl={containerEl} node={node} />
      </div>
      :
      <div style={{ display: 'flex', minWidth: 0, flex: 1 }} className='resourceTreeNode node'>
        <DatasetItemLabel
          name={node.get('name')}
          fullPath={node.get('fullPath')}
          dragType={this.props.dragType}
          typeIcon={iconType}
          placement='right'
          isExpandable={this.props.isExpandable}
          shouldShowOverlay={this.props.shouldShowOverlay}
          shouldAllowAdd={this.props.shouldAllowAdd}
          addtoEditor={this.addtoEditor} />
      </div>;

    const nodeWrap = (
      <>
        <div
          data-qa={node.get('name')}
          onClick={this.handleSelectedNodeChange.bind(this, node)}
          onMouseUp={e => e.preventDefault()}
          style={isDisabled ? styles.disabled : {}}
          className={classNames('resourceTreeNodeWrap', activeClass)}
        >
          {iconForArrow}
          {nodeElement}
        </div>

        {iconForAdding}
      </>
    );

    return this.props.dragType
      ? (
        <DragSource
          dragType={this.props.dragType}
          id={node.get('fullPath')}
          key={nodeId}
          className={activeClass}>
          {nodeWrap}
        </DragSource>
      )
      : nodeWrap;
  }

  render() {
    return (
      <div style={{ ...styles.base, ...this.props.style }} className='resourceTree'>
        { this.props.browser ?
          <TreeBrowser
            resourceTree={this.props.resourceTree}
            sources={this.props.sources}
            renderNode={this.renderNode}
            selectedNodeId={this.props.selectedNodeId}
            isNodeExpanded={this.props.isNodeExpanded}
            dragType={this.props.dragType}
            addtoEditor={this.addtoEditor}
            isSqlEditorTab={this.props.isSqlEditorTab}
            handleSidebarCollapse={this.props.handleSidebarCollapse}
            sidebarCollapsed={this.props.sidebarCollapsed}
            isCollapsable={this.props.isCollapsable}
          />
          :
          <Tree
            resourceTree={this.props.resourceTree}
            sources={this.props.sources}
            renderNode={this.renderNode}
            selectedNodeId={this.props.selectedNodeId}
            isNodeExpanded={this.props.isNodeExpanded}
          />
        }
      </div>
    );
  }
}

const styles = {
  disabled: {
    opacity: 0.7,
    pointerEvents: 'none',
    color: 'rgb(153, 153, 153)',
    background: 'rgb(255, 255, 255)'
  },
  node: {
    display: 'inline-flex',
    flexDirection: 'row',
    justifyContent: 'flex-start',
    alignItems: 'center',
    width: '100%',
    cursor: 'pointer'
  },
  arrow: {
    width: 24,
    height: 24
  },
  emptyDiv: {
    height: 24,
    width: 15,
    marginLeft: 5,
    flex: '0 0 auto'
  },
  icon: {
    Container: {
      width: 21,
      height: 21
    },
    Icon: {
      width: 21,
      height: 21
    }
  },
  text: {
    marginLeft: 5,
    lineHeight: '21px'
  }
};
