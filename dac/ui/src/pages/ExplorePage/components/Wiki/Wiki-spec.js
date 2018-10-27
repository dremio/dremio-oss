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
import { WikiView } from './Wiki';

describe('Wiki component', () => {
  const minimalProps =  {
    addHasChangesHook: () => {},
    confirm: () => {},
    confirmUnsavedChanges: () => {}
  };

  describe('initEntity', () => {
    const mockInitEntity = (wrapper, testFn) => {
      const spyFn = sinon.spy();
      const instance = wrapper.instance();
      sinon.stub(instance, 'initEntity').callsFake(spyFn);

      testFn(spyFn);

      // you should revert all mocks here
      instance.initEntity.restore();
    };

    it('should be called if not empty entityId is provided', () => {
      const wrapper = shallow(<WikiView {...minimalProps} />);
      mockInitEntity(wrapper, (spyFn) => {
        wrapper.setProps({
          entityId: 'not empty id'
        });

        expect(spyFn).have.been.called; // initEntity should be called if not empty entity is provided
      });
    });

    it('should be called if entity is changed', () => {
      const entity1 = 'not empty id';
      const entity2 = 'other id';
      const wrapper = shallow(<WikiView {...minimalProps} entityId={entity1} />);
      mockInitEntity(wrapper, (spyFn) => {
        wrapper.setProps({
          entityId: entity2
        });

        expect(spyFn).have.been.called;
      });
    });

    it('should not be called if entity is not changed', () => {
      const entity1 = 'not empty id';
      const wrapper = shallow(<WikiView {...minimalProps} entityId={entity1} />);
      mockInitEntity(wrapper, (spyFn) => {
        wrapper.setProps({
          entityId: entity1
        });

        expect(spyFn).have.not.been.called;
      });
    });

    it('should not be called if empty entity is provided', () => {
      const wrapper = shallow(<WikiView {...minimalProps} entityId='entity id' />);
      mockInitEntity(wrapper, (spyFn) => {
        wrapper.setProps({
          entityId: null
        });

        expect(spyFn).have.not.been.called;
      });
    });
  });
});

