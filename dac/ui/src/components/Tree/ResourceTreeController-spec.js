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
import { ResourceTreeController } from './ResourceTreeController';

describe('ResourceTreeController', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      loadResourceTree: sinon.stub().returns({
        then: (callback) => callback({})
      })
    };
    commonProps = {
      ...minimalProps,
      onChange: sinon.spy(),
      resourceTree: Immutable.fromJS([
        {
          type: 'SPACE',
          name: 'DG',
          fullPath: ['DG'],
          resources: [{
            type: 'FOLDER',
            name: 'fold',
            fullPath: ['DG', 'fold'],
            resources: [{
              type: 'VIRTUAL_DATASET',
              name: 'ds',
              fullPath: ['DG', 'fold', 'ds']
            }]
          }]
        },
        {
          type: 'SOURCE',
          name: 'dfs',
          fullPath: ['dfs']
        },
        {
          type: 'HOME',
          name: '@dremio',
          fullPath: ['@dremio']
        }
      ])
    };
  });
  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<ResourceTreeController {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });
  it('should render with common props without exploding', () => {
    const wrapper = shallow(<ResourceTreeController {...commonProps}/>);
    expect(wrapper).to.have.length(1);
  });
  describe('componentWillMount', () => {
    it(`should set selectedNodeId, call onChange, call expandPathToSelectedNode,
      call loadResourceTree if has preselectedNodeId`, () => {
      const props = {
        ...commonProps,
        preselectedNodeId: 'DG'
      };
      const wrapper = shallow(<ResourceTreeController {...props}/>);
      const instance = wrapper.instance();
      sinon.spy(instance, 'expandPathToSelectedNode');
      sinon.spy(instance, 'loadResourceTree');
      instance.componentWillMount();
      expect(wrapper.state('selectedNodeId')).to.eql(props.preselectedNodeId);
      expect(props.onChange.called).to.be.true;
      expect(instance.expandPathToSelectedNode.called).to.be.true;
      expect(instance.expandPathToSelectedNode.calledWith(props.preselectedNodeId)).to.be.true;
      expect(instance.loadResourceTree.called).to.be.true;
      expect(instance.loadResourceTree.calledWith(props.preselectedNodeId, true)).to.be.true;
    });
    it('should call loadResourceTree if preselectedNodeId is empty', () => {
      shallow(<ResourceTreeController {...commonProps}/>);
      expect(commonProps.loadResourceTree.called).to.be.true;
    });
  });

  describe('handleSelectedNodeChange', () => {
    it('should call onChange with selectedNodeId and node and set state with selectedNodeId', () => {
      const wrapper = shallow(<ResourceTreeController {...commonProps}/>);
      const instance = wrapper.instance();
      const node =  {
        type: 'SPACE',
        name: 'DG',
        fullPath: ['DG']
      };
      instance.handleSelectedNodeChange(node.name, node);
      expect(commonProps.onChange.called).to.be.true;
      expect(commonProps.onChange.calledWith(node.name, node)).to.be.true;
      expect(wrapper.state('selectedNodeId')).to.equal(node.name);
    });
  });
  describe('isNodeExpanded', () => {
    it('should return true if node is expanded', () => {
      const wrapper = shallow(<ResourceTreeController {...commonProps}/>);
      const instance = wrapper.instance();
      const node = Immutable.fromJS({
        type: 'SPACE',
        name: 'DG',
        fullPath: ['DG']
      });
      const expandedNodes = Immutable.fromJS([node.get('name')]);

      wrapper.setState({ expandedNodes });
      expect(instance.isNodeExpanded(node)).to.be.true;
    });
    it('should return false if node is not expanded', () => {
      const wrapper = shallow(<ResourceTreeController {...commonProps}/>);
      const instance = wrapper.instance();
      const node = Immutable.fromJS({
        type: 'SPACE',
        name: 'DG',
        fullPath: ['DG']
      });
      expect(instance.isNodeExpanded(node)).to.be.false;
    });
  });
  describe('expandPathToSelectedNode', () => {
    it('should add all parent combinations as dotted paths to state.expandedNodes', () => {
      const wrapper = shallow(<ResourceTreeController {...commonProps}/>);
      const instance = wrapper.instance();
      const path = 'DG.folder.asd';
      instance.expandPathToSelectedNode(path);
      expect(wrapper.state('expandedNodes')).to.be.equal(Immutable.List(['DG', 'DG.folder']));
    });

    it('should add nothing to state.expandedNodes if no parents exist', () => {
      const wrapper = shallow(<ResourceTreeController {...commonProps}/>);
      const instance = wrapper.instance();
      const path = 'asd';
      instance.expandPathToSelectedNode(path);
      expect(wrapper.state('expandedNodes').size).to.be.equal(0);
    });

    it('should correctly handle paths with doublequotes', () => {
      const wrapper = shallow(<ResourceTreeController {...commonProps}/>);
      const instance = wrapper.instance();
      const path = '"@drem.io".folder.another.andanother';
      instance.expandPathToSelectedNode(path);
      expect(wrapper.state('expandedNodes')).to.be.equal(Immutable.List([
        '"@drem.io"',
        '"@drem.io".folder',
        '"@drem.io".folder.another'
      ]));
    });
  });
  describe('handleNodeClick', () => {
    let wrapper;
    let instance;
    let node;
    beforeEach(() => {
      wrapper = shallow(<ResourceTreeController {...commonProps}/>);
      instance = wrapper.instance();
      node = commonProps.resourceTree.get(0);
    });
    it('should set selectedNodeId, if node does not exists in expandedNodes then add it, call loadResourceTree', () => {
      sinon.spy(instance, 'loadResourceTree');
      instance.handleNodeClick(node);
      expect(instance.loadResourceTree.called).to.be.true;
      expect(instance.loadResourceTree.calledWith(node.get('name'))).to.be.true;
      expect(wrapper.state('selectedNodeId')).to.eql(node.get('name'));
      expect(wrapper.state('expandedNodes').includes(node.get('name'))).to.be.true;
    });
    it('should delete node from expandedNodes if it exists, with children', () => {
      sinon.spy(instance, 'loadResourceTree');
      wrapper.setState({ expandedNodes: Immutable.fromJS(['DG', 'DG.fold']) });
      instance.handleNodeClick(node);
      expect(instance.loadResourceTree).to.not.be.called;
      expect(wrapper.state('expandedNodes').size).to.be.equal(0);
    });
  });
});
