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

import { TimeDot } from './TimeDot';

describe('TimeDot', () => {

  let minimalProps;
  let commonProps;
  let wrapper;
  let instance;
  let link;
  beforeEach(() => {
    minimalProps = {
      location: {},
      tipVersion: '12345',
      activeVersion: '12345',
      datasetPathname: '/space/foo/ds1',
      navigateToHistory: sinon.stub()
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
        pathname: 'orignal/path/name',
        query: {jobId: 123}
      }
    };
    wrapper = shallow(<TimeDot {...commonProps}/>);
    instance = wrapper.instance();
    link = wrapper.find('Link');
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<TimeDot {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render main Link', () => {
    expect(link).to.have.length(1);
  });

  it('should not render Link when at current version', () => {
    wrapper.setProps({location: {query: {version: commonProps.historyItem.get('datasetVersion')}}});
    expect(wrapper.find('Link')).to.have.length(0);
  });

  describe('#getLinkLocation', () => {
    it('should take a pathname from location', () => {
      expect(instance.getLinkLocation().pathname).to.equal(commonProps.location.pathname);
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
    const mockEvent = {target: {getBoundingClientRect: () => ({top: 0, height: 0})}};
    /**
     * Returns a wrapper for a popover.
     * This method must be called when popover is shown, after mouseenter
     * event for dot element
     */
    const getPopover = () => wrapper.find('Tooltip');

    it('should show on mouse enter', () => {
      link.simulate('mouseenter', mockEvent);
      wrapper.update();
      expect(wrapper.state('open')).to.be.true;
      link.simulate('mouseleave');
    });

    it('should show hide after a delay on mouseleave', (done) => {
      link.simulate('mouseenter', mockEvent);
      link.simulate('mouseleave');
      expect(wrapper.state('open')).to.be.true;
      setTimeout(() => {
        wrapper.update();
        expect(wrapper.state('open')).to.be.false;
        done();
      }, 1);
    });

    it('should hide popover if cursor is moved to a popover', (done) => {
      link.simulate('mouseenter', mockEvent);
      const popover = getPopover();
      link.simulate('mouseleave');
      popover.simulate('mouseenter', mockEvent);

      setTimeout(() => {
        wrapper.update();
        expect(wrapper.state('open')).to.be.false;
        done();
      }, 1);
    });

    it('should still show popover if mouse moves back to target', (done) => {
      link.simulate('mouseenter', mockEvent);
      link.simulate('mouseleave');
      link.simulate('mouseenter', mockEvent);

      setTimeout(() => {
        wrapper.update();
        expect(wrapper.state('open')).to.be.true;
        done();
      }, 1);
    });
  });
});
