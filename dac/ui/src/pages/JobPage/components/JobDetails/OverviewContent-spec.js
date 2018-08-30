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

import OverviewContent from './OverviewContent';

describe('OverviewContent', () => {

  let minimalProps;
  let commonProps;
  let wrapper;
  let context;
  beforeEach(() => {
    minimalProps = {
      jobDetails: Immutable.fromJS({
        jobId: {
          id: 'jobid'
        },
        requestType: 'CREATE_PREPARE',
        state: 'COMPLETED',
        endTime: 123546,
        startTime: 0,
        user: 'dremio',
        queryType: 'UI_RUN',
        stats: {},
        sql: 'SELECT * FROM',
        attemptDetails: [
          {
            reason: 'schema Learning',
            profileUrl: '/profiles/2808817f-f02f-adaf-4911-ae91b1423500'
          },
          {
            reason: 'Insufficient memory',
            profileUrl: '/profiles/2808817f-f02f-adaf-4911-ae91b1423500'
          }
        ],
        parentsList: [
          {
            datasetPathList: ['Prod-Sample', 'ds1'],
            type: 'VIRTUAL_DATASET'
          }
        ]
      })
    };
    commonProps = {
      ...minimalProps
    };
    context = {location: {}};

    wrapper = shallow(<OverviewContent {...commonProps}/>, {context});
  });

  it('should render with minimal props without exploding', () => {
    expect(shallow(<OverviewContent {...minimalProps}/>, {context})).to.have.length(1);
  });

  describe('checkResultOfProfile', () => {

    it('should return undefined if we haven\'t any reason', () => {
      const attemptDetails = Immutable.fromJS([
        {
          reason: '',
          profileUrl: '/profiles/2808817f-f02f-adaf-4911-ae91b1423500'
        },
        {
          reason: '',
          profileUrl: '/profiles/2808817f-f02f-adaf-4911-ae91b1423500'
        }
      ]);
      expect(OverviewContent.checkResultOfProfile(attemptDetails)).to.eql(undefined);
    });

    it('should return array with items that have reason=Schema Learning ', () => {
      expect(
        OverviewContent.checkResultOfProfile(commonProps.jobDetails.get('attemptDetails'), 'schema')
      ).to.eql(1);
    });

    it('should return array with items that have reason=Insufficient Memory ', () => {
      expect(
        OverviewContent.checkResultOfProfile(commonProps.jobDetails.get('attemptDetails'), 'memory')
      ).to.eql(1);
    });

  });

  describe('renderInfoAboutProfile', () => {

    it('should return string with only attempts', () => {
      const props = {
        jobDetails: Immutable.fromJS({
          ...commonProps.jobDetails.toJS(),
          attemptDetails: Immutable.fromJS([
            {
              reason: '',
              profileUrl: '/profiles/2808817f-f02f-adaf-4911-ae91b1423500'
            },
            {
              reason: '',
              profileUrl: '/profiles/2808817f-f02f-adaf-4911-ae91b1423500'
            }
          ])
        })
      };
      wrapper = shallow(<OverviewContent {...props}/>, {context});
      expect(wrapper.instance().renderInfoAboutProfile()).to.eql('This query was attempted 2 times');
    });

    it('should return string about attempts with schema', () => {
      const props = {
        jobDetails: Immutable.fromJS({
          ...commonProps.jobDetails.toJS(),
          attemptDetails: Immutable.fromJS([
            {
              reason: 'schema Learning',
              profileUrl: '/profiles/2808817f-f02f-adaf-4911-ae91b1423500'
            },
            {
              reason: '',
              profileUrl: '/profiles/2808817f-f02f-adaf-4911-ae91b1423500'
            }
          ])
        })
      };
      wrapper = shallow(<OverviewContent {...props}/>, {context});
      expect(
        wrapper.instance().renderInfoAboutProfile()
      ).to.eql('This query was attempted 2 times due to schema learning 1');
    });

    it('should be metadata job', () => {
      const props = {
        jobDetails: Immutable.fromJS({
          ...commonProps.jobDetails.toJS(),
          requestType: 'GET_CATALOGS'
        })
      };
      wrapper = shallow(<OverviewContent {...props}/>, {context});
      expect(
          wrapper.instance().isMetadataJob()
      ).to.eql(true);
    });

    it('should be non-metadata job', () => {
      const props = {
        jobDetails: Immutable.fromJS({
          ...commonProps.jobDetails.toJS(),
          requestType: 'SQL'
        })
      };
      wrapper = shallow(<OverviewContent {...props}/>, {context});
      expect(
          wrapper.instance().isMetadataJob()
      ).to.eql(false);
    });

    it('should be no dataset available', () => {
      const props = {
        jobDetails: Immutable.fromJS({
          ...commonProps.jobDetails.toJS(),
          datasetPathList: undefined
        })
      };
      wrapper = shallow(<OverviewContent {...props}/>, {context});
      expect(
          wrapper.instance().isDatasetAvailable()
      ).to.eql(false);
    });

    it('should be no parents available', () => {
      const props = {
        jobDetails: Immutable.fromJS({
          ...commonProps.jobDetails.toJS(),
          parentsList: undefined
        })
      };
      wrapper = shallow(<OverviewContent {...props}/>, {context});
      expect(
          wrapper.instance().isParentsBlockToBeShown()
      ).to.eql(false);
    });

    it('should be parents available', () => {
      const props = {
        jobDetails: Immutable.fromJS({
          ...commonProps.jobDetails.toJS(),
          parentsList: [
              {datasetPathList: ['a', 'b']}
          ]
        })
      };
      wrapper = shallow(<OverviewContent {...props}/>, {context});
      expect(
          wrapper.instance().isParentsBlockToBeShown()
      ).to.eql(true);
    });


    it('no sql box for metadata job', () => {
      const props = {
        jobDetails: commonProps.jobDetails.set('requestType', 'GET_CATALOGS')
      };
      wrapper = shallow(<OverviewContent {...props}/>, {context});
      expect(wrapper.instance().renderSqlBlock()).to.eql(null);
    });

    it('no sql box for materialization job', () => {
      const props = {
        jobDetails: commonProps.jobDetails.set('materializationFor', new Immutable.Map())
      };
      wrapper = shallow(<OverviewContent {...props}/>, {context});
      expect(wrapper.instance().renderSqlBlock()).to.eql(null);
    });

    it('should return string about attempts with memory', () => {
      const props = {
        jobDetails: Immutable.fromJS({
          ...commonProps.jobDetails.toJS(),
          attemptDetails: Immutable.fromJS([
            {
              reason: '',
              profileUrl: '/profiles/2808817f-f02f-adaf-4911-ae91b1423500'
            },
            {
              reason: 'Insufficient memory',
              profileUrl: '/profiles/2808817f-f02f-adaf-4911-ae91b1423500'
            }
          ])
        })
      };
      wrapper = shallow(<OverviewContent {...props}/>, {context});
      expect(
        wrapper.instance().renderInfoAboutProfile()
      ).to.eql('This query was attempted 2 times due to insufficient memory 1');
    });

    it('should return string about attempts with memory and schema', () => {
      expect(
        wrapper.instance().renderInfoAboutProfile()
      ).to.eql('This query was attempted 2 times due to insufficient memory 1 and schema learning 1');
    });

  });

});
