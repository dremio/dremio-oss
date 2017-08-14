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
import * as sourcesActions from 'actions/resources/sources';
import {LeftTree} from './LeftTree';

describe('LeftTree', () => {

  let minimalProps;
  let commonProps;
  let context;
  beforeEach(() => {
    context = {
      location: {
        pathname: ''
      },
      router: {
        push: sinon.stub()
      },
      loggedInUser: {
        admin: true
      }
    };
    minimalProps = {
      spaces: Immutable.fromJS([{}]),
      sources: Immutable.fromJS([{}]),
      spacesViewState: new Immutable.Map(),
      sourcesViewState: new Immutable.Map(),
      createSampleSource: sinon.stub().resolves({
        payload: Immutable.fromJS({
          entities: {source: { 'new-id': {id: 'new-id', links: {self: '/self'}}}},
          result: 'new-id'
        })
      })

    };
    commonProps = {
      ...minimalProps
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<LeftTree {...minimalProps} />, { context });
    expect(wrapper).to.have.length(1);
  });

  it('should render with common props without exploding', () => {
    const wrapper = shallow(<LeftTree {...commonProps}/>, { context });
    expect(wrapper).to.have.length(1);
  });

  describe('#addSampleSource()', () => {
    it('should navigate to source on success', () => {
      const instance = shallow(<LeftTree {...commonProps} />, { context }).instance();
      const promise = instance.addSampleSource();
      expect(instance.state.isAddingSampleSource).to.be.true;
      return promise.then(() => {
        expect(context.router.push).to.have.been.calledWith('/self');
        expect(instance.state.isAddingSampleSource).to.be.false;
      });
    });
    it('should reset state.isAddingSampleSource on http error', () => {
      commonProps.createSampleSource = sinon.stub().resolves({error: true});
      const instance = shallow(<LeftTree {...commonProps} />, { context }).instance();
      const promise = instance.addSampleSource();
      expect(instance.state.isAddingSampleSource).to.be.true;
      return promise.then(() => {
        expect(context.router.push).to.have.not.been.called;
        expect(instance.state.isAddingSampleSource).to.be.false;
      });
    });
    it('should reset state.isAddingSampleSource on promise reject', () => {
      commonProps.createSampleSource = sinon.stub().rejects();
      const instance = shallow(<LeftTree {...commonProps} />, { context }).instance();
      const promise = instance.addSampleSource();
      expect(instance.state.isAddingSampleSource).to.be.true;
      return promise.then(() => {
        throw new Error('should reject');
      }, () => {
        expect(context.router.push).to.have.not.been.called;
        expect(instance.state.isAddingSampleSource).to.be.false;
      });
    });
  });

  describe('#getInitialSpacesContent()', () => {
    it('have no sources', () => {
      const instance = shallow(<LeftTree {...commonProps} sources={new Immutable.List()} />, { context }).instance();
      expect(instance.getInitialSpacesContent()).to.be.null;
    });
    it('have sources', () => {
      const instance = shallow(<LeftTree {...commonProps} />, { context }).instance();
      expect(instance.getInitialSpacesContent()).to.be.null;
    });
  });

  describe('#getInitialSourcesContent()', () => {
    it("only sample, user can't add: show nothing", () => {
      context.loggedInUser.admin = false;
      sinon.stub(sourcesActions, 'isSampleSource').returns(true);
      const instance = shallow(<LeftTree {...commonProps} />, { context }).instance();
      expect(instance.getInitialSourcesContent()).to.be.null;
      sourcesActions.isSampleSource.restore();
    });
    it('single source (not sample): show nothing', () => {
      const instance = shallow(<LeftTree {...commonProps} />, { context }).instance();
      expect(instance.getInitialSourcesContent()).to.be.null;
    });
    it('multiple sources: show nothing', () => {
      commonProps.sources = Immutable.fromJS([{}, {}]);
      const instance = shallow(<LeftTree {...commonProps} />, { context }).instance();
      expect(instance.getInitialSourcesContent()).to.be.null;
    });
    it('no sources, user can add: show text and both buttons', () => {
      commonProps.sources = Immutable.fromJS([]);
      const instance = shallow(<LeftTree {...commonProps} />, { context }).instance();
      expect(
        shallow(instance.getInitialSourcesContent()).text()
      ).to.be.equal(
        'You do not have any sources.<SimpleButton /><LinkButton />'
      );
    });
    it("no sources, user can't add: show text", () => {
      commonProps.sources = Immutable.fromJS([]);
      context.loggedInUser.admin = false;
      const instance = shallow(<LeftTree {...commonProps} />, { context }).instance();
      expect(
        shallow(instance.getInitialSourcesContent()).text()
      ).to.be.equal(
        'You do not have any sources.'
      );
    });
    it('only sample source, user can add: show text and add button', () => {
      sinon.stub(sourcesActions, 'isSampleSource').returns(true);

      const instance = shallow(<LeftTree {...commonProps} />, { context }).instance();
      expect(
        shallow(instance.getInitialSourcesContent()).text()
      ).to.be.equal(
        'Add your own source:<LinkButton />'
      );

      sourcesActions.isSampleSource.restore();
    });
  });
});
