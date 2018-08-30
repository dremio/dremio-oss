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

import { Advanced } from './Advanced';
import { LABELS } from './settingsConfig';

describe('Advanced', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    LABELS.$c = 'C Label';
    LABELS.$zBefore = '1 Before Label';
    LABELS.$aAfter = '2 After Label';

    minimalProps = { // todo: find a way to auto-gen this based on propTypes def
      getAllSettings: sinon.stub().returns(Promise.resolve()),
      resetSetting: sinon.stub().returns(Promise.resolve()),
      addNotification: sinon.stub().returns(Promise.resolve()),

      viewState: new Immutable.Map(),

      settings: new Immutable.Map(),
      setChildDirtyState: sinon.spy()
    };
    commonProps = {
      ...minimalProps,

      settings: Immutable.fromJS({
        '$a': {
          id: '$a',
          value: 1,
          type: 'INTEGER',
          showOutsideWhitelist: true
        },
        '$b': {
          id: '$b',
          value: 'bar',
          type: 'TEXT',
          showOutsideWhitelist: false
        },
        // unlabeled key sort test
        '$dz': {
          id: '$dz',
          value: true,
          type: 'BOOLEAN',
          showOutsideWhitelist: false
        },
        '$d': {
          id: '$d',
          value: true,
          type: 'BOOLEAN',
          showOutsideWhitelist: false
        },
        '$c': {
          id: '$c',
          value: 1.1,
          type: 'FLOAT',
          showOutsideWhitelist: false
        },
        'support.email.addr': { // canary for RESERVED
          id: 'support.email.addr',
          value: true,
          type: 'BOOLEAN',
          showOutsideWhitelist: true
        },
        'exec.queue.enable': { // canary for sections & empty string label
          id: 'exec.queue.enable',
          value: true,
          type: 'BOOLEAN',
          showOutsideWhitelist: true
        },
        // labeled key sort test
        '$aAfter': {
          id: '$aAfter',
          value: 1.1,
          type: 'FLOAT',
          showOutsideWhitelist: false
        },
        '$zBefore': {
          id: '$zBefore',
          value: true,
          type: 'BOOLEAN',
          showOutsideWhitelist: false
        }
      }),
      updateFormDirtyState: sinon.spy()
    };
  });

  afterEach(() => {
    delete LABELS.$c;
    delete LABELS.$z_first;
    delete LABELS.$a_last;
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<Advanced {...minimalProps} />);
    expect(wrapper).to.have.length(1);
  });

  it('should getAllSettings on mount', () => {
    shallow(<Advanced {...commonProps} />);
    expect(commonProps.getAllSettings).to.have.been.calledWith({ viewId: 'ADVANCED_SETTINGS_VIEW_ID' });
  });

  describe('#settingExists', () => {
    it('not loaded', () => {
      const instance = shallow(<Advanced {...minimalProps} />).instance();
      expect(instance.settingExists('$a')).to.be.false;
    });

    it('true', () => {
      const instance = shallow(<Advanced {...commonProps} />).instance();
      expect(instance.settingExists('$a')).to.be.true;
    });

    it('false', () => {
      const instance = shallow(<Advanced {...commonProps} />).instance();
      expect(instance.settingExists('n/a', 'user')).to.be.false;
    });
  });

  describe('#getShownSettings', () => {
    it('not loaded', () => {
      const instance = shallow(<Advanced {...minimalProps} />).instance();
      expect(instance.getShownSettings()).to.eql([]);
    });

    it('should return items for showOutsideWhitelist and tempShown (and not reserved)', () => {
      const instance = shallow(<Advanced {...commonProps} />).instance();
      instance.setState(function(state) {
        return {
          tempShown: state.tempShown.add('$b')
        };
      });

      // no support.email.addr because it's RESERVED
      expect(instance.getShownSettings().map(e => e.id)).to.eql('$a exec.queue.enable $b'.split(' '));
    });

    it('should skip items in sections for includeSections:false', () => {
      const instance = shallow(<Advanced {...commonProps} />).instance();
      instance.setState(function(state) {
        return {
          tempShown: state.tempShown.add('$b')
        };
      });

      // no support.email.addr because it's RESERVED
      // no exec.queue.enable because it's in a section
      expect(instance.getShownSettings({includeSections:false}).map(e => e.id)).to.eql('$a $b'.split(' '));
    });
  });

  describe('#sortSettings', () => {
    it('should sort tempShown, labeled, unlabeled (and alphabetaical within each)', () => {
      const instance = shallow(<Advanced {...minimalProps} />).instance();
      instance.setState({tempShown: Immutable.OrderedSet(['$a', '$b'])});

      const settingsArray = commonProps.settings.toList().toJS();
      instance.sortSettings(settingsArray);

      // $b, $a: tempShown, insertion order
      // exec.queue.enable, $zBefore, $aAfter, $c, support.email.addr: alpha order of labels
      // $d, $dz last: alpha order of ids
      const order = '$b $a exec.queue.enable $zBefore $aAfter $c support.email.addr $d $dz'.split(' ');
      expect(settingsArray.map(e => e.id)).to.eql(order);
    });
  });

  describe('#addAdvanced', () => {
    let evt;
    let input;
    beforeEach(() => {
      input = {value: '$d'};
      evt = {
        preventDefault: sinon.stub(),
        target: {
          children: [input],
          reset: sinon.stub()
        }
      };
    });

    afterEach(() => {
      expect(evt.preventDefault).to.have.been.called;
    });


    it('add', () => {
      const instance = shallow(<Advanced {...commonProps} />).instance();
      instance.addAdvanced(evt);
      expect(Array.from(instance.state.tempShown)).to.eql(['$d']);

      expect(evt.target.reset).to.have.been.called;
      expect(minimalProps.addNotification).to.have.not.been.called;
    });

    it('should do nothing when input value is empty string', () => {
      input.value = '';

      const instance = shallow(<Advanced {...commonProps} />).instance();
      instance.addAdvanced(evt);
      expect(Array.from(instance.state.tempShown)).to.eql([]);

      expect(evt.target.reset).to.not.have.been.called;
      expect(minimalProps.addNotification).to.have.not.been.called;
    });

    it('non-existing', () => {
      input.value = 'n/a';

      const instance = shallow(<Advanced {...commonProps} />).instance();
      instance.addAdvanced(evt);
      expect(Array.from(instance.state.tempShown)).to.eql([]);

      expect(evt.target.reset).to.not.have.been.called;
      expect(minimalProps.addNotification).to.have.been.called;
    });

    it('pre-existing', () => {
      input.value = '$a';

      const instance = shallow(<Advanced {...commonProps} />).instance();
      instance.addAdvanced(evt);
      expect(Array.from(instance.state.tempShown)).to.eql([]);

      expect(evt.target.reset).to.have.been.called;
      expect(minimalProps.addNotification).to.have.been.called;
    });
  });

  describe('#resetSetting', () => {
    it('should call the correct methods)', (done) => {
      const instance = shallow(<Advanced {...minimalProps} />).instance();

      instance.setState(function(state) {
        return {
          tempShown: state.tempShown.add('foo')
        };
      });

      instance.resetSetting('foo').then(() => {
        expect(instance.props.resetSetting).to.have.been.called;
        expect(instance.props.getAllSettings).to.have.been.called;
        expect(Array.from(instance.state.tempShown)).to.eql([]);
        done();
      });
    });
  });
});
