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
import Tree from './Tree';

describe('Tree', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {};
    commonProps = {
      ...minimalProps,
      selectedNodeId: 'selectedNodeId',
      resourceTree: Immutable.fromJS([
        { fullPath: 'node1' },
        { fullPath: 'node2' },
        { fullPath: 'node3' }
      ]),
      renderNode: sinon.spy()
    };
  });
  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<Tree {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });
  it('should render TreeNode component for each node in resourceTree', () => {
    const wrapper = shallow(<Tree {...commonProps}/>);
    expect(wrapper.find('TreeNode')).to.have.length(commonProps.resourceTree.size);

    const nodeProps = wrapper.find('TreeNode').first().props();
    expect(nodeProps.node).to.eql(commonProps.resourceTree.get(0));
    expect(nodeProps.renderNode).to.eql(commonProps.renderNode);
    expect(nodeProps.selectedNodeId).to.eql(commonProps.selectedNodeId);
  });
});
