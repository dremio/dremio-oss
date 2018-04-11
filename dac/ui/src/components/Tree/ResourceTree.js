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
import Immutable from 'immutable';
import classNames from 'classnames';
import Radium from 'radium';
import { injectIntl } from 'react-intl';

import { CONTAINER_ENTITY_TYPES } from 'constants/Constants';

import Art from 'components/Art';
import DragSource from 'components/DragComponents/DragSource';
import DatasetItemLabel from 'components/Dataset/DatasetItemLabel';
import EllipsedText from 'components/EllipsedText';
import { getIconByEntityType, getArtPropsByEntityIconType } from 'utils/iconUtils';

import Tree from './Tree';

import './ResourceTree.less';

@injectIntl
@Radium
export default class ResourceTree extends Component {
  static propTypes = {
    resourceTree: PropTypes.instanceOf(Immutable.List),
    selectedNodeId: PropTypes.string,
    isDatasetsDisabled: PropTypes.bool,
    dragType: PropTypes.string,
    formatIdFromNode: PropTypes.func,
    isNodeExpanded: PropTypes.func,
    handleSelectedNodeChange: PropTypes.func,
    handleNodeClick: PropTypes.func,
    style: PropTypes.object,
    intl: PropTypes.object.isRequired
  };

  static isNodeExpandable = (node) => CONTAINER_ENTITY_TYPES.has(node.get('type'))

  handleSelectedNodeChange = (node) => {
    if (ResourceTree.isNodeExpandable(node)) {
      this.props.handleNodeClick(node);
    }
    this.props.handleSelectedNodeChange(this.props.formatIdFromNode(node), node);
  }

  renderNode = (node) => {
    const nodeId = this.props.formatIdFromNode(node);
    const isDisabled = this.props.isDatasetsDisabled && !CONTAINER_ENTITY_TYPES.has(node.get('type'));
    const arrowIconType = this.props.isNodeExpanded(node) ? 'TriangleDown' : 'TriangleRight';
    const arrowAlt = this.props.isNodeExpanded(node) ? 'Undisclosed' : 'Disclosed';

    const iconForArrow = ResourceTree.isNodeExpandable(node)
      ? <Art src={`${arrowIconType}.svg`} alt={this.props.intl.formatMessage({ id: `Common.${arrowAlt}` })} style={styles.arrow} />
      : <div style={styles.emptyDiv}></div>;

    const classes = classNames('node', {'active-node': this.props.selectedNodeId === nodeId});
    const iconType = getIconByEntityType(node.get('type'));
    const iconForResourceProps = getArtPropsByEntityIconType(iconType);

    const nodeElement = ResourceTree.isNodeExpandable(node) ?
      <div style={{ display: 'flex', minWidth: 0 }}>
        <Art {...iconForResourceProps} style={styles.icon}/>
        <EllipsedText className='node-text' style={styles.text} text={node.get('name')} />
      </div>
      :
      <div style={{ display: 'flex', minWidth: 0 }}>
        <DatasetItemLabel
          name={node.get('name')}
          fullPath={node.get('fullPath')}
          dragType={this.props.dragType}
          typeIcon={iconType}
          placement='right'/>
      </div>;
    const nodeWrap = (
      <div
        data-qa={node.get('name')}
        onClick={this.handleSelectedNodeChange.bind(this, node)}
        onMouseUp={e => e.preventDefault()}
        style={isDisabled ? styles.disabled : {}}
      >
        <div style={{...styles.node}} className={classes}>
          {iconForArrow}
          {nodeElement}
        </div>
      </div>
    );
    return this.props.dragType
      ? (
        <DragSource
          dragType={this.props.dragType}
          id={node.get('fullPath')}
          key={nodeId}>
          {nodeWrap}
        </DragSource>
      )
      : nodeWrap;
  }

  render() {
    return (
      <div style={{ ...styles.base, ...this.props.style }}>
        <Tree
          resourceTree={this.props.resourceTree}
          renderNode={this.renderNode}
          selectedNodeId={this.props.selectedNodeId}
          isNodeExpanded={this.props.isNodeExpanded}
        />
      </div>
    );
  }
}

const styles = {
  base: {
    height: '100%',
    paddingTop: 2,
    overflowY: 'auto',
    maxHeight: 242,
    minHeight: 240,
    border: '1px solid #E0E0E0'
  },
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
    height: 24,
    cursor: 'pointer'
  },
  arrow: {
    width: 28,
    height: 28,
    marginTop: 1,
    marginRight: -8
  },
  emptyDiv: {
    height: 24,
    width: 15,
    marginLeft: 5,
    flex: '0 0 auto'
  },
  icon: {
    width: 21,
    height: 21
  },
  text: {
    marginLeft: 5,
    lineHeight: '21px'
  }
};
