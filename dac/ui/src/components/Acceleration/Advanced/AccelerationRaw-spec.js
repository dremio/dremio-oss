/*
 * Copyright (C) 2017 Dremio Corporation
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

import AccelerationRaw from './AccelerationRaw';

describe('AccelerationRaw', () => {
  let minimalProps;
  let commonProps;
  let wrapper;
  let instance;
  beforeEach(() => {
    minimalProps = {
      acceleration: Immutable.fromJS({
        context: {
          datasetSchema: {
            fieldList: []
          }
        }
      }),
      fields: {
        rawLayouts: {
          enabled: false,
          layoutList: ['col1', 'col2']
        }
      }
    };
    minimalProps.fields.rawLayouts.layoutList.addField = sinon.spy();
    minimalProps.fields.rawLayouts.layoutList.removeField = sinon.spy();
    commonProps = {
      ...minimalProps
    };
    wrapper = shallow(<AccelerationRaw {...commonProps}/>);
    instance = wrapper.instance();
  });

  it('should render with minimal props without exploding', () => {
    const wrap = shallow(<AccelerationRaw {...minimalProps}/>);
    expect(wrap).to.have.length(1);
  });

  it('should add new layout', () => {
    const defaultLayout = {
      name: 'New Reflection',
      details: {
        displayFieldList: [],
        partitionFieldList: [],
        sortFieldList: [],
        partitionDistributionStrategy: 'CONSOLIDATED'
      }
    };
    instance.addNewLayout();
    expect(commonProps.fields.rawLayouts.layoutList.addField).to.have.been.calledWith(defaultLayout);
  });

  describe('removeLayout', () => {
    it('should remove layout', () => {
      instance.removeLayout(1);
      expect(commonProps.fields.rawLayouts.layoutList.removeField).to.have.been.calledWith(1);
    });

    it('should not remove layout if we have only one layout', () => {
      minimalProps.fields.rawLayouts.layoutList.length = 1;
      instance.removeLayout(1);
      expect(instance.props.fields.rawLayouts.layoutList.removeField.called).to.be.false;
    });
  });
});
