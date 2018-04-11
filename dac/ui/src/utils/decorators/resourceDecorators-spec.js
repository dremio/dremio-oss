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
import * as resourceDecorators from './resourceDecorators';

describe('resourceDecorators', function() {

  describe('decorateProvision()', function() {
    const defaultWorkSummary = {
      total: 0,
      active: 0,
      pending: 0,
      decommissioning: 0,
      disconnected: 0,
      totalRAM: 0,
      totalCores: 0
    };

    it('should return decorated provision with full provision data', function() {
      const provision = Immutable.fromJS({
        containers: {
          decommissioningCount: 1,
          pendingCount: 1,
          provisioningCount: 1,
          disconnectedList: [
            {
              containerPropertyList: [
                {
                  key: 'memoryMB',
                  value: '10'
                },
                {
                  key: 'virtualCoreCount',
                  value: '1'
                }
              ]
            }
          ],
          runningList: [
            {
              containerPropertyList: [
                {
                  key: 'memoryMB',
                  value: '10'
                },
                {
                  key: 'virtualCoreCount',
                  value: '1'
                }
              ]
            },
            {
              containerPropertyList: [
                {
                  key: 'memoryMB',
                  value: '20'
                },
                {
                  key: 'virtualCoreCount',
                  value: '2'
                }
              ]
            }
          ]
        },
        dynamicConfig: {
          containerCount: 3
        }
      });
      const result = provision.merge(Immutable.fromJS({
        workersSummary: {
          total: 3,
          active: 1,
          pending: 1,
          decommissioning: 1,
          disconnected: 1,
          totalRAM: 30,
          totalCores: 3
        }
      }));

      expect(resourceDecorators.decorateProvision(provision)).to.be.equal(result);
    });

    it('should set decorateProvision properties like 0 if provision data is empty', () => {
      const provision = Immutable.fromJS({
        containers: {}
      });
      const result = provision.merge(Immutable.fromJS({ workersSummary: defaultWorkSummary }));

      expect(resourceDecorators.decorateProvision(provision)).to.be.equal(result);
    });

    it('should set workersSummary.active equal to runningList.size if decommissioningCount is undefined', () => {
      const provision = Immutable.fromJS({
        containers: {
          runningList: [
            {
              containerPropertyList: []
            },
            {
              containerPropertyList: []
            },
            {
              containerPropertyList: []
            },
            {
              containerPropertyList: []
            }
          ]
        }
      });
      const result = provision.merge(Immutable.fromJS({
        workersSummary: {
          ...defaultWorkSummary,
          active: 4
        }
      }));

      expect(resourceDecorators.decorateProvision(provision)).to.be.equal(result);
    });

    it('should return decorated provision when containerPropertyList hasn\'t some keys', function() {
      const provision = Immutable.fromJS({
        containers: {
          runningList: [
            {
              containerPropertyList: [
                {
                  key: 'memoryMB',
                  value: '10'
                }
              ]
            },
            {
              containerPropertyList: [
                {
                  key: 'virtualCoreCount',
                  value: '2'
                }
              ]
            },
            {
              containerPropertyList: []
            }
          ]
        },
        dynamicConfig: {
          containerCount: 3
        }
      });
      const result = provision.merge(Immutable.fromJS({
        workersSummary: {
          total: 3,
          active: 3,
          pending: 0,
          decommissioning: 0,
          disconnected: 0,
          totalRAM: 10,
          totalCores: 2
        }
      }));

      expect(resourceDecorators.decorateProvision(provision)).to.be.equal(result);
    });
  });
});
