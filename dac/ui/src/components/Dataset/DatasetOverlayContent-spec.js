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

import { DatasetOverlayContent } from './DatasetOverlayContent';

describe('DatasetOverlayContent', () => {

  let commonProps;
  let minimalProps;
  beforeEach(() => {
    minimalProps = {
      fullPath: Immutable.List(['Prod-sample', 'ds1']),
      loadSummaryDataset: sinon.spy(),
      typeIcon: 'VirtualDataset',
      summaryDataset: Immutable.fromJS({
        fullPath: ['Prod-sample', 'ds2'],
        jobCount: 0,
        descendants: 1,
        links: {
          jobs: '/jobs/dataset/%22Sales-Sample%22.ds4',
          edit: '/datasets/new_untitled?parentDataset=%22Sales-Sample%22.ds',
          run: '/dataset/%22Sales-Sample%22.ds4/version/010a36aec7d443b3/'
        },
        datasetType: 'PHYSICAL_DATASET',
        fields : [
          {
            name: 'A',
            type: 'INTEGER'
          },
          {
            name: 'B',
            type: 'INTEGER'
          }
        ]
      })
    };
    commonProps = {
      ...minimalProps
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<DatasetOverlayContent {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render dataset-label-overlay, ColumnMenuItem, DatasetItemLabel', () => {
    const wrapper = shallow(<DatasetOverlayContent {...commonProps}/>);
    expect(wrapper.find('.dataset-label-overlay')).to.have.length(1);
    expect(wrapper.find('ColumnMenuItem')).to.have.length(2);
    expect(wrapper.find('DatasetItemLabel')).to.have.length(1);
    expect(wrapper.find('BreadCrumbs')).to.have.length(0);
  });
});
