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
import { shallow } from 'enzyme';
import Immutable from 'immutable';
import ResourceTree from './ResourceTree';

describe('ResourceTree', () => {
  let minimalProps;
  let commonProps;
  let homeNode;
  let instance;
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
    let wrapper;
    beforeEach(() => {
      wrapper = shallow(<ResourceTree {...commonProps}/>);
      instance = wrapper.instance();
    });
  });

  describe('handleSelectedNodeChange', () => {
    let wrapper;
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
