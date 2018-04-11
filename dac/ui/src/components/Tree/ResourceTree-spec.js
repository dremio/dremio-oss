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
import { shallow } from 'enzyme';
import Immutable from 'immutable';
import ResourceTree from './ResourceTree';

describe('ResourceTree', () => {
  let minimalProps;
  let commonProps;
  let homeNode;
  beforeEach(() => {
    minimalProps = {};
    commonProps = {
      ...minimalProps,
      handleSelectedNodeChange: sinon.spy(),
      handleNodeClick: sinon.spy(),
      formatIdFromNode: (node) => node.get('fullPath').join('.'),
      isNodeExpanded: sinon.spy(),
      selectedNodeId: 'foo.bar'
    };
    homeNode = Immutable.fromJS({
      fullPath: ['@dremio'],
      name: 'Home',
      type: 'HOME'
    });
  });
  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<ResourceTree {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });
  it('should render Tree', () => {
    const wrapper = shallow(<ResourceTree {...commonProps}/>);
    expect(wrapper.find('Tree')).to.have.length(1);
  });
  describe('isNodeExpandable', () => {
    it('should return true if node type is expandable', () => {
      expect(ResourceTree.isNodeExpandable(homeNode)).to.be.true;
      let node = homeNode.set('type', 'FOLDER');
      expect(ResourceTree.isNodeExpandable(node)).to.be.true;
      node = node.set('type', 'SPACE');
      expect(ResourceTree.isNodeExpandable(node)).to.be.true;
      node = node.set('type', 'SOURCE');
      expect(ResourceTree.isNodeExpandable(node)).to.be.true;
    });
    it('should return false if node type is not expandable', () => {
      const node = homeNode.set('type', 'VIRTUAL_DATASET');
      expect(ResourceTree.isNodeExpandable(node)).to.be.false;
    });
  });
  describe('renderNode', () => {
    let instance;
    let wrapper;
    beforeEach(() => {
      wrapper = shallow(<ResourceTree {...commonProps}/>);
      instance = wrapper.instance();
    });
    it('should render expand arrow only if node is expandable', () => {
      let renderedNode = shallow(instance.renderNode(homeNode));
      expect(renderedNode.find('Art')).to.have.length(2);

      const node = homeNode.set('type', 'VIRTUAL_DATASET');
      renderedNode = shallow(instance.renderNode(node));
      expect(renderedNode.find('Art')).to.have.length(0);
    });
    it('should render DatasetItemLabel only if node is dataset', () => {
      const node = Immutable.fromJS({
        fullPath: ['foo', 'bar'],
        name: 'bar',
        type: 'VIRTUAL_DATASET'
      });
      let renderedNode = shallow(instance.renderNode(node));
      expect(renderedNode.find('DatasetItemLabel')).to.have.length(1);
      expect(renderedNode.find('Art')).to.have.length(0);

      renderedNode = shallow(instance.renderNode(homeNode));
      expect(renderedNode.find('DatasetItemLabel')).to.have.length(0);
      expect(renderedNode.find('Art')).to.have.length(2);
    });
    it('should render node with .active-node class only if it is selected', () => {
      const node = Immutable.fromJS({
        fullPath: ['foo', 'bar'],
        name: 'bar',
        type: 'VIRTUAL_DATASET'
      });
      let renderNode = shallow(instance.renderNode(node));
      expect(renderNode.find('.active-node')).to.have.length(1);

      renderNode = shallow(instance.renderNode(homeNode));
      expect(renderNode.find('.active-node')).to.have.length(0);
    });
  });

  describe('handleSelectedNodeChange', () => {
    let wrapper;
    let instance;
    beforeEach(() => {
      sinon.spy(commonProps, 'formatIdFromNode');
      wrapper = shallow(<ResourceTree {...commonProps}/>);
      instance = wrapper.instance();
    });
    afterEach(() => {
      commonProps.formatIdFromNode.restore();
    });

    it('should call handleSelectedNodeChange, formatIdFromNode from props', () => {
      instance.handleSelectedNodeChange(homeNode);
      expect(commonProps.handleSelectedNodeChange).to.be.called;
      expect(commonProps.formatIdFromNode).to.be.calledWith(homeNode);
    });
    it('should call handleNodeClick from props when clicked node is expandable', () => {
      instance.handleSelectedNodeChange(homeNode);
      expect(commonProps.handleNodeClick).to.be.calledWith(homeNode);
      expect(commonProps.handleSelectedNodeChange).to.be.called;
      expect(commonProps.formatIdFromNode).to.be.calledWith(homeNode);
    });

    it('should not call handleNodeClick from props when node is not expandable', () => {
      const nonExpandableNode = Immutable.fromJS({ type: 'VIRTUAL_DATASET', fullPath: ['@dremio', 'ds']});
      instance.handleSelectedNodeChange(nonExpandableNode);
      expect(commonProps.handleNodeClick).to.be.not.called;
      expect(commonProps.handleSelectedNodeChange).to.be.called;
      expect(commonProps.formatIdFromNode).to.be.calledWith(nonExpandableNode);
    });
  });
});
