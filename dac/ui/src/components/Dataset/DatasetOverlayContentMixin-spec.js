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

import { DatasetOverlayContent as DatasetOverlayContentBase } from 'components/Dataset/DatasetOverlayContent';
import DatasetOverlayContentMixin from './DatasetOverlayContentMixin';

@DatasetOverlayContentMixin
class DatasetOverlayContent extends DatasetOverlayContentBase {}

describe('DatasetOverlayContentMixin', () => {

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
        datasetType: 'VIRTUAL_DATASET',
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

  describe.skip('renderPencil', () => {
    it('should render pencil if canEdit', () => {
      const wrapper = shallow(<DatasetOverlayContent {...commonProps}/>);
      expect(wrapper.find('Link')).to.have.length(3);
    });

    it('should not render pencil if not canEdit', () => {
      const wrapper = shallow(<DatasetOverlayContent {...commonProps}/>);
      const dsTypes = 'PHYSICAL_DATASET PHYSICAL_DATASET_SOURCE_FILE PHYSICAL_DATASET_SOURCE_FOLDER PHYSICAL_DATASET_HOME_FILE'.split(' '); // eslint-disable-line max-len
      for (const dsType of dsTypes) {
        wrapper.setProps({
          summaryDataset: commonProps.summaryDataset.set('datasetType', dsType)
        });
        expect(wrapper.find('Link')).to.have.length(2);
      }
    });
  });
});


