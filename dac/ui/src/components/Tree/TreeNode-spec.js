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
import TreeNode from './TreeNode';

describe('TreeNode', () => {
  let minimalProps;
  beforeEach(() => {
    minimalProps = {
      node: Immutable.fromJS({
        fullPath: 'node',
        resources: [
          { fullPath: 'resource1' },
          { fullPath: 'resource2' },
          { fullPath: 'resource3' }
        ]
      }),
      renderNode: sinon.spy(),
      isNodeExpanded: sinon.stub(),
      selectedNodeId: '123'
    };
  });

  it('should render with minimal props without exploding, call renderNode and isNodeExpanded', () => {
    const wrapper = shallow(<TreeNode {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
    expect(minimalProps.renderNode).to.be.calledWith(minimalProps.node);
    expect(minimalProps.isNodeExpanded).to.be.calledWith(minimalProps.node);
  });

  it('should render child TreeNode for each node in props.node.resources if isNodeExpanded returns true', () => {
    minimalProps.isNodeExpanded.returns(true);

    const wrapper = shallow(<TreeNode {...minimalProps}/>);
    expect(wrapper.find('TreeNode')).to.have.length(3);
  });

  it('should pass props to child TreeNode', () => {
    minimalProps.isNodeExpanded.returns(true);
    const wrapper = shallow(<TreeNode {...minimalProps}/>);
    const childNodeProps = wrapper.find('TreeNode').first().props();
    expect(childNodeProps.renderNode).to.equal(minimalProps.renderNode);
    expect(childNodeProps.isNodeExpanded).to.equal(minimalProps.isNodeExpanded);
    expect(childNodeProps.selectedNodeId).to.equal(minimalProps.selectedNodeId);
  });

  it('should not any TreeNode components if isNodeExpanded returns false', () => {
    minimalProps.isNodeExpanded.returns(false);

    const wrapper = shallow(<TreeNode {...minimalProps}/>);
    expect(wrapper.find('TreeNode')).to.have.length(0);
  });
});
