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

import WorkersGrid, { WORKER_STATE_TO_COLOR as gridColors } from './WorkersGrid';

describe('WorkersGrid', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      entity: Immutable.fromJS({
        workersSummary: {
          active: 0,
          pending: 0,
          disconnected: 0,
          decommissioning: 0
        }
      })
    };
    commonProps = {
      ...minimalProps,
      entity: Immutable.fromJS({
        workersSummary: {
          active: 1,
          pending: 2,
          disconnected: 3,
          decommissioning: 4
        }
      })
    };
  });
  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<WorkersGrid {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  describe('getGridItemSize', () => {
    let wrapper;
    let instance;
    beforeEach(() => {
      wrapper = shallow(<WorkersGrid {...minimalProps}/>);
      instance = wrapper.instance();
    });

    it('should return 15 when worker\'s count less then 40', () => {
      const gridItems = [
        { workerState: 'active', value: 39 },
        { workerState: 'pending', value: 1 },
        { workerState: 'provisioning', value: 0 },
        { workerState: 'decommissioning', value: 0 }
      ];
      expect(instance.getGridItemSize(gridItems)).to.be.eql(15);
    });

    it('should return 9 when worker\'s count less more then 40 but less then 175', () => {
      let gridItems = [
        { workerState: 'active', value: 39 },
        { workerState: 'pending', value: 2 },
        { workerState: 'disconnected', value: 0 },
        { workerState: 'decommissioning', value: 0 }
      ];
      expect(instance.getGridItemSize(gridItems)).to.be.eql(9);
      gridItems = [
        { workerState: 'active', value: 174 },
        { workerState: 'pending', value: 1 },
        { workerState: 'disconnected', value: 0 },
        { workerState: 'decommissioning', value: 0 }
      ];
      expect(instance.getGridItemSize(gridItems)).to.be.eql(9);
    });

    it('should return 5 when worker\'s count less more then 175', () => {
      const gridItems = [
        { workerState: 'active', value: 175 },
        { workerState: 'pending', value: 1 },
        { workerState: 'disconnected', value: 0 },
        { workerState: 'decommissioning', value: 0 }
      ];
      expect(instance.getGridItemSize(gridItems)).to.be.eql(5);
    });
  });

  describe('renderWorkersGrid', () => {
    let wrapper;
    beforeEach(() => {
      wrapper = shallow(<WorkersGrid {...minimalProps}/>);
    });

    it('should render no items by default', () => {
      expect(wrapper.find('.grid-list').children()).to.have.length(0);
    });

    it('should render same quantity of squares as total workers size', () => {
      wrapper.setProps(commonProps);
      expect(wrapper.find('.grid-list').children()).to.have.length(10);
    });

    it('should render squares in following order: active, pending, provisioning, decommissioning', () => {
      wrapper.setProps(commonProps);
      const allGridItems = wrapper.find('.grid-list').children();
      const getChildrenInRange = (start, end) => {
        const range = [];
        allGridItems.forEach((item, i) => {
          if (i < start || i >= end) {
            return;
          }
          range.push(item);
        });
        return range;
      };
      const getItemsColor = (children) => {
        return Immutable.Set(children.map((child) => child.props('style').style.background)).toJS();
      };
      const active = getChildrenInRange(0, 1);
      const pending = getChildrenInRange(1, 3);
      const disconnected = getChildrenInRange(3, 6);
      const decommissioning = getChildrenInRange(6, 9);
      expect(getItemsColor(active)).to.be.eql([gridColors.active]);
      expect(getItemsColor(pending)).to.be.eql([gridColors.pending]);
      expect(getItemsColor(disconnected)).to.be.eql([gridColors.disconnected]);
      expect(getItemsColor(decommissioning)).to.be.eql([gridColors.decommissioning]);
    });
  });
});
