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
import { shallow, mount } from 'enzyme';
import Immutable from 'immutable';

import Message, {RENDER_NO_DETAILS} from './Message';

describe('Message', () => {

  let commonProps;
  beforeEach(() => {
    commonProps = {
      message: Immutable.Map({message: 'foo'}),
      messageType: 'warning',
      messageId: 'id1'
    };
  });

  it('renders <div .message>', () => {
    const wrapper = shallow(<Message {...commonProps}/>);

    expect(wrapper.hasClass('message')).to.be.true;
    expect(wrapper.hasClass(commonProps.messageType)).to.be.true;
    expect(wrapper.find('FontIcon')).to.have.length(2);
    expect(wrapper.find('FontIcon').first().prop('type')).to.be.equal('Warning');
    expect(wrapper.find('.message-content').text()).to.equal(commonProps.message.get('message'));
  });

  it('should not render when dismissed', () => {
    const wrapper = shallow(<Message {...commonProps}/>);
    wrapper.setState({dismissed: true});
    expect(wrapper.find('.message')).to.have.length(0);

    wrapper.setState({dismissed: false});
    expect(wrapper.find('.message')).to.have.length(1);
  });

  it('hides when dismiss is clicked and shows again when it receives a new message', () => {
    const wrapper = shallow(<Message {...commonProps}/>);
    expect(wrapper.find('FontIcon').at(1).prop('onClick')).to.equal(wrapper.instance().onDismiss);
    wrapper.instance().onDismiss();
    expect(wrapper.state('dismissed')).to.be.true;

    wrapper.setProps({message: Immutable.Map({message: 'foo2'}), messageId: 'id2'});
    expect(wrapper.state('dismissed')).to.be.false;
  });

  it('does not hide when dismiss is clicked and props.onDismiss returns false', () => {
    const wrapper = shallow(<Message {...commonProps} onDismiss={() => false}/>);
    wrapper.instance().onDismiss();
    expect(wrapper.state('dismissed')).to.be.false;
  });

  it('throws on unknown messageType', () => {
    expect(() => shallow(<Message {...commonProps} messageType='foo'/>)).to.throw();
  });

  it('should render close icon by default', () => {
    expect(shallow(<Message {...commonProps} />).find('FontIcon[type="XSmall"]')).to.have.length(1);
  });

  it('should not render close icon when message is not dissmisable', () => {
    expect(shallow(<Message isDismissable={false} />).find('FontIcon[type="XSmall"]')).to.have.length(0);
  });

  describe('#componentWillReceiveProps', () => {
    it('should reset for changed messageId', function() {
      const instance = shallow(<Message {...commonProps}/>).instance();
      const spy = sinon.spy(instance, 'setState');

      instance.componentWillReceiveProps({messageId: 'id1'});
      expect(spy).to.have.not.been.called;

      instance.componentWillReceiveProps({messageId: 'id2'});
      expect(spy).to.have.been.calledWith({ dismissed: false, showMore: false });
    });
  });

  describe('#showMoreToggle', function() {
    it('should toggle showMore from true to false', function() {
      const instance = shallow(<Message {...commonProps}/>).instance();
      instance.setState({showMore: true});
      instance.showMoreToggle();
      expect(instance.state.showMore).to.be.false;
    });
  });

  describe('#renderErrorMessageText', function() {
    it('should return message from string', function() {
      const instance = shallow(<Message {...commonProps} message='bar'/>).instance();
      expect(instance.renderErrorMessageText()).to.eql('bar');
    });
    it('should return message from Map', function() {
      const instance = shallow(<Message {...commonProps}/>).instance();
      expect(instance.renderErrorMessageText()).to.eql('foo');
    });
    it('should return legacy message from Map', function() {
      const instance = shallow(<Message {...commonProps} message={Immutable.Map({errorMessage: 'foo'})}/>).instance();
      expect(instance.renderErrorMessageText()).to.eql('foo');
    });
    it('should return renderMessageForCode over #message', function() {
      // use PIPELINE_FAILURE as a canary
      const instance = shallow(<Message
        {...commonProps}
        message={Immutable.Map({message: 'foo', code: 'PIPELINE_FAILURE'})}
      />).instance();
      expect(instance.renderErrorMessageText()).to.eql(
        <span>{la('There was an error in the Reflection pipeline.')}</span>
      );
    });

  });

  describe('#renderDetails', function() {
    it('should not render with string', function() {
      const instance = shallow(<Message {...commonProps} message='foo'/>).instance();
      expect(instance.renderDetails()).to.be.undefined;
    });
    it('should not render without stackTrace, moreInfo, or code details', function() {
      const instance = shallow(<Message {...commonProps}/>).instance();
      expect(instance.renderDetails()).to.be.undefined;
    });
    it('should render with stackTrace as array', function() {
      const instance = shallow(<Message {...commonProps}
        message={Immutable.Map({message: 'foo', stackTrace: ['a', 'b']})}
      />).instance();
      expect(shallow(instance.renderDetails()).text()).to.eql('a\nb');
    });
    it('should render with stackTrace as string', function() {
      const instance = shallow(<Message {...commonProps}
        message={Immutable.Map({message: 'foo', stackTrace: 'foo'})}
      />).instance();
      expect(shallow(instance.renderDetails()).text()).to.eql('foo');
    });
    it('should render with moreInfo', function() {
      const instance = shallow(<Message {...commonProps}
        message={Immutable.Map({message: 'foo', moreInfo:'bar'})}
      />).instance();
      expect(shallow(instance.renderDetails()).text()).to.eql('bar');
    });
    it('should render nothing if code specifies RENDER_NO_DETAILS', function() {
      const instance = shallow(<Message {...commonProps}
        message={Immutable.fromJS({ // using MATERIALIZATION_FAILURE as a canary
          code: 'MATERIALIZATION_FAILURE',
          message: 'foo',
          materializationFailure: {
            jobId: 'job',
            layoutId: 'layout',
            materializationId: 'materialization'
          }
        })}
      />).instance();
      expect(instance.renderDetails()).to.be.undefined;
    });

    // todo: add "should render with code details" once anything does that
  });

  describe('#renderDetailsForCode', function() {
    it('should not render without a code', function() {
      const instance = shallow(<Message {...commonProps}
        message={Immutable.fromJS({
          message: 'foo'
        })}
      />).instance();
      expect(instance.renderDetailsForCode()).to.be.undefined;
    });

    it('should return @@RENDER_NO_DETAILS for MATERIALIZATION_FAILURE', function() {
      const instance = shallow(<Message {...commonProps}
        message={Immutable.fromJS({
          code: 'MATERIALIZATION_FAILURE',
          message: 'foo',
          materializationFailure: {
            jobId: 'job',
            layoutId: 'layout',
            materializationId: 'materialization'
          }
        })}
      />).instance();
      expect(instance.renderDetailsForCode()).to.eql(RENDER_NO_DETAILS);
    });
  });

  describe('#renderMessageForCode', function() {
    it('should not render without a code', function() {
      const instance = shallow(<Message {...commonProps}
        message={Immutable.fromJS({
          message: 'foo'
        })}
      />).instance();
      expect(instance.renderMessageForCode()).to.be.undefined;
    });

    it('should render for PIPELINE_FAILURE', function() {
      const instance = shallow(<Message {...commonProps}
        message={Immutable.fromJS({
          code: 'PIPELINE_FAILURE',
          message: 'foo'
        })}
      />).instance();
      expect(mount(instance.renderMessageForCode()).text()).to.eql(
        'There was an error in the Reflection pipeline.'
      );
    });

    it('should render for MATERIALIZATION_FAILURE', function() {
      const instance = shallow(<Message {...commonProps}
        message={Immutable.fromJS({
          code: 'MATERIALIZATION_FAILURE',
          message: 'foo',
          materializationFailure: {
            jobId: 'job',
            layoutId: 'layout',
            materializationId: 'materialization'
          }
        })}
      />).instance();
      expect(mount(instance.renderMessageForCode()).text()).to.eql(
        'There was an error building a Reflection (show job).'
      );
    });

    it('should render for DROP_FAILURE', function() {
      const instance = shallow(<Message {...commonProps}
        message={Immutable.fromJS({
          code: 'DROP_FAILURE',
          message: 'foo'
        })}
      />).instance();
      expect(mount(instance.renderMessageForCode()).text()).to.eql(
        'There was an error dropping a Reflection.'
      );
    });
  });
});
