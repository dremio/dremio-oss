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

import TimeDot from './TimeDot';

describe('TimeDot', () => {

  let minimalProps;
  let commonProps;
  let wrapper;
  let instance;
  beforeEach(() => {
    minimalProps = {
      location: {},
      tipVersion: '12345',
      activeVersion: '12345',
      datasetPathname: '/space/foo/ds1'
    };
    commonProps = {
      ...minimalProps,
      historyItem: Immutable.fromJS({
        state: 'STARTED',
        finishedAt: 1462293722000,
        createdAt: 1462290000000,
        owner: 'test_user',
        transformDescription: 'the description',
        recordsReturned: 1337,
        datasetVersion: 'abcdef'
      }),
      onClick: sinon.spy(),
      hideDelay: 0,
      location: {
        query: {jobId: 123}
      }
    };
    wrapper = shallow(<TimeDot {...commonProps}/>);
    instance = wrapper.instance();
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<TimeDot {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render main Link', () => {
    expect(wrapper.find('Link')).to.have.length(1);
  });

  it('should not render Link when at current version', () => {
    wrapper.setProps({location: {query: {version: commonProps.historyItem.get('datasetVersion')}}});
    expect(wrapper.find('Link')).to.have.length(0);
  });

  describe('#getLinkLocation', () => {
    it('should have pathname=props.datasetPathname', () => {
      expect(instance.getLinkLocation().pathname).to.equal(commonProps.datasetPathname);
    });

    it('should have query.tipVersion from props.tipVersion and version from historyItem.datasetVersion', () => {
      expect(instance.getLinkLocation().query).to.eql({
        tipVersion: commonProps.tipVersion,
        version: commonProps.historyItem.get('datasetVersion')
      });
    });

    it('should return null if already at this datasetVersion', () => {
      wrapper.setProps({location: {
        ...commonProps.location,
        query: {
          version: commonProps.historyItem.get('datasetVersion')
        }
      }});

      expect(instance.getLinkLocation()).to.be.null;
    });
  });

  describe('popover show/hide', () => {

    let target;
    let popover;

    beforeEach(() => {
      target = wrapper.find('[data-qa="time-dot-target"]');
      popover = wrapper.find('[data-qa="time-dot-popover"]');
    });

    it('should show on mouse enter', () => {
      target.simulate('mouseenter');
      expect(wrapper.find('Overlay').props().show).to.be.true;
      target.simulate('mouseleave');
    });

    it('should show hide after a delay on mouseleave', (done) => {
      target.simulate('mouseenter');
      target.simulate('mouseleave');
      expect(wrapper.find('Overlay').props().show).to.be.true;
      setTimeout(() => {
        wrapper.update();
        expect(wrapper.find('Overlay').props().show).to.be.false;
        done();
      }, 1);
    });

    it('should not hide popover while mouse in popover', (done) => {
      target.simulate('mouseenter');
      target.simulate('mouseleave');
      popover.simulate('mouseenter');

      setTimeout(() => {
        wrapper.update();
        expect(wrapper.find('Overlay').props().show).to.be.true;
        popover.simulate('mouseleave');
        setTimeout(() => {
          wrapper.update();
          expect(wrapper.find('Overlay').props().show).to.be.false;
          done();
        }, 1);
      }, 1);
    });

    it('should still show popover if mouse moves back to target', (done) => {
      target.simulate('mouseenter');
      target.simulate('mouseleave');
      popover.simulate('mouseenter');
      popover.simulate('mouseleave');
      target.simulate('mouseenter');

      setTimeout(() => {
        wrapper.update();
        expect(wrapper.find('Overlay').props().show).to.be.true;
        done();
      }, 1);
    });
  });
});
