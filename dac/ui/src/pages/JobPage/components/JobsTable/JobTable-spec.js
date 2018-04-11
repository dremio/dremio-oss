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

import JobTable from './JobTable';

describe('JobsTable-spec', () => {

  let commonProps;
  beforeEach(() => {
    commonProps = {
      jobs: Immutable.fromJS([
        {
          'id' : '2926787a-28fe-998b-19c7-334ce2ab13e4',
          'state' : 'COMPLETED',
          'sql' : 'WITH dremio_limited_preview AS (SELECT *\nFROM "Prod-Sample"."ds2") ' +
            'SELECT * FROM dremio_limited_preview LIMIT 500',
          'jobType' : 'SQL',
          'client' : '',
          'user' : 'anonymous',
          'startTime' : 1457096581513,
          'endTime' : 1457096581769,
          'datasetVersion' : 'a9eee529-dc38-4688-a96b-687bc1cc1388',
          parentsList: [{datasetPathList: ['Tpch-Sample.tpch03', 'Tpch-Sample.tpch04']}],
          datasetPathList: ['Prod-Sample', 'job2'],
          'preview' : true,
          'stats' : {
            'intputBytes' : 0,
            'outputBytes' : 0,
            'intputRecords' : 0,
            'outputRecords' : 0
          }
        }
      ]),
      orderedColumn: Immutable.fromJS({columnName: null, order: 'desc'}),
      width: '50%',
      getJobData: () => {}
    };
  });

  it('render Jobs Table', () => {
    const wrapper = shallow(<JobTable {...commonProps}/>, {context: {location: {query: {}}}});
    expect(wrapper.hasClass('jobs-table')).to.be.true;
  });

  describe('renderHeaderBlock', () => {
    let wrapper;
    let instance;
    beforeEach(() => {
      sinon.stub(JobTable.prototype, 'sortJobsByColumn');
      wrapper = shallow(<JobTable {...commonProps}/>, {context: {location: {query:{}}}});
      instance = wrapper.instance();
    });
    afterEach(() => {
      JobTable.prototype.sortJobsByColumn.restore();
    });
    it('should render header block with all columns', () => {
      expect(wrapper.find('.job-table-header').children()).to.have.length(5);
    });
    it('should render columns with key attribute assigned with column key', () => {
      const renderedKeys = wrapper.find('.job-table-header').children().map(c => c.key());
      const columnKeys = wrapper.state('columnsHeader').keySeq().toJS();
      // react escapes key attribute with '.$' prefix for children components
      expect(renderedKeys).to.be.eql(columnKeys.map(key => `.$${key}`));
    });
    it('should render columns with correct labels', () => {
      const renderedLabels = wrapper.find('.job-table-header').children().map(c => c.text());
      const columnLabels = wrapper.state('columnsHeader').valueSeq().toJS().map(i => i.label);
      expect(renderedLabels).to.be.eql(columnLabels);
    });
    it('should render User column with margin-left 50', () => {
      const userStyles = wrapper.find('.job-table-header > div').at(1).props('style').style;
      expect(userStyles.marginLeft).to.be.eql(50);
    });
    it('should call sortJobsByColumns with column key when clicked', () => {
      wrapper.find('.job-table-header').children().forEach(c => c.simulate('click'));
      const columnKeys = wrapper.state('columnsHeader').keySeq().toJS();
      expect(columnKeys).to.have.length(5);
      columnKeys.forEach((key, calledIndex) => {
        expect(instance.sortJobsByColumn.args[calledIndex]).to.be.eql([key]);
      });
    });
  });

});
