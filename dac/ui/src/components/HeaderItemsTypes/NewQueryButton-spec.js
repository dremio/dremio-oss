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

import { NewQueryButton } from './NewQueryButton';

describe('NewQueryButton', () => {

  let wrapper;
  let instance;
  let minimalProps;
  let commonProps;
  let context;
  const clickEvent = {
    preventDefault: () => {}
  };
  beforeEach(() => {
    minimalProps = {
      location: {pathname: '/foo'},
      currentSql: '',
      showConfirmationDialog: sinon.spy(),
      resetNewQuery: sinon.spy()
    };
    commonProps = {
      ...minimalProps
    };
    context = {
      username: 'test_user',
      router: {push: sinon.spy()}
    };
    wrapper = shallow(<NewQueryButton {...commonProps}/>, {context});
    instance = wrapper.instance();
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<NewQueryButton {...minimalProps}/>, {context});
    expect(wrapper).to.have.length(1);
  });

  it('should render icon and NewQuery', () => {
    expect(wrapper.find('FontIcon')).to.have.length(1);
    expect(wrapper.find('FormattedMessage').prop('id')).to.be.equal('NewQuery.NewQuery');
    expect(wrapper.find('a').props().onClick).to.equal(instance.handleClick);
  });

  describe('#handleClick', () => {
    it('should navigate to newQuery if not already there', () => {
      instance.handleClick(clickEvent);
      expect(context.router.push).to.be.calledWith('/new_query?context=%22%40test_user%22');
    });

    it('should not confirm if already at New Query and there is no sql', () => {
      wrapper.setProps({location: {pathname: '/new_query'}, currentSql: ' '});
      instance.handleClick(clickEvent);
      expect(context.router.push).to.not.be.called;
      expect(commonProps.showConfirmationDialog).to.not.be.called;
      expect(commonProps.resetNewQuery).to.be.called;

      wrapper.setProps({currentSql: null});
      instance.handleClick(clickEvent);
      expect(context.router.push).to.not.be.called;
      expect(commonProps.showConfirmationDialog).to.not.be.called;
      expect(commonProps.resetNewQuery).to.be.called;
    });

    it('should confirm then reset if at New Query and there is sql', () => {
      wrapper.setProps({location: {pathname: '/new_query'}, currentSql: 'some sql'});
      instance.handleClick(clickEvent);
      expect(context.router.push).to.not.be.called;
      expect(commonProps.showConfirmationDialog).to.be.called;
      expect(commonProps.resetNewQuery).to.not.be.called;

      commonProps.showConfirmationDialog.args[0][0].confirm();
      expect(commonProps.resetNewQuery).to.be.called;
    });
  });
});
