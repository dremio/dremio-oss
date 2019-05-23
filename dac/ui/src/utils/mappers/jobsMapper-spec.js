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
import Immutable from 'immutable';
import jobsMapper from './jobsMapper.js';

describe('Test jobs mapper', () => {

  describe('job detail mapper', () => {
    const json = {
      'id': '407dce9b-8da1-41fd-a972-a0ed7234f555',
      'user': 'test_user',
      'path': 'Marketing.Northwest.Leads',
      'state': 'pending'
    };
    it('simple json', () => {
      const expectedResult = {
        id: '407dce9b-8da1-41fd-a972-a0ed7234f555',
        user: 'test_user',
        path: 'Marketing.Northwest.Leads',
        state: 'pending'
      };
      expect(jobsMapper.jobDetails(json)).to.eql(expectedResult);
    });
  });

  describe('map Table Dataset Profiles', () => {
    const incoming = [
      {
        'pushdownQuery': 'query',
        'datasetProfile' : {
          'datasetPathsList' : ['cp."tpch/supplier.parquet"'],
          'accelerated': true,
          'bytesRead' : 15168,
          'recordsRead' : 100,
          'parallelism' : 1,
          'waitOnSource' : 0
        }
      }
    ];

    it('dataset profiles', () => {
      const expectedResult = [
        {
          'pushdownQuery': 'query',
          'datasetPathsList' : ['cp."tpch/supplier.parquet"'],
          'bytesRead' : 15168,
          'recordsRead' : 100,
          'parallelism' : 1,
          'waitOnSource' : 0,
          'accelerated' : true
        }
      ];


      expect(jobsMapper.mapTableDatasetProfiles(incoming)).to.eql(expectedResult);
    });
  });

  describe('jobs mapper', () => {

    it('handles empty payload', () => {
      expect(jobsMapper.mapJobs(null)).to.be.eql([]);
    });

    it('handles empty jobs list', () => {
      expect(jobsMapper.mapJobs({})).to.be.eql([]);
    });

    it('handles jobs list = null', () => {
      expect(jobsMapper.mapJobs({ jobs: null })).to.be.eql([]);
    });

    it('convert jobs to immutable items', () => {
      const job = {
        a: 1,
        b: 2
      };
      const resultJob = jobsMapper.mapJobs({ jobs: [job] })[0];
      expect(resultJob instanceof Immutable.Map).to.be.true;
      expect(resultJob.toJS()).to.be.eql(job);
    });
  });
});
