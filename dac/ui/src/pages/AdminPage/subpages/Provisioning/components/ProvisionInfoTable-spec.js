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
import ProvisionInfoTable from './ProvisionInfoTable';

describe('ProvisionInfoTable', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {};
    commonProps = {
      ...minimalProps,
      provision: Immutable.fromJS({
        containers: {
          runningList: [
            {
              containerPropertyList: [
                { value: 'foo' }
              ]
            },
            {
              containerPropertyList: [
                { value: 'bar' }
              ]
            }
          ],
          disconnectedList: [
            {
              containerPropertyList: [
                { value: 'foo' }
              ]
            },
            {
              containerPropertyList: [
                { value: 'bar' }
              ]
            }
          ]
        }
      })
    };
  });
  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<ProvisionInfoTable {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render StatefulTableViewer with tableData based on runningList', () => {
    const wrapper = shallow(<ProvisionInfoTable {...commonProps}/>);
    const tableViewerWrapper = wrapper.find('TableViewer');
    expect(tableViewerWrapper).to.have.length(1);
    expect(tableViewerWrapper.prop('tableData')).to.have.size(4);
  });
});
