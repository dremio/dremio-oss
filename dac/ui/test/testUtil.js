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
export function minimalFormProps(fields = []) {
  const props = {
    onSubmit: sinon.spy(),
    asyncValidating: false,
    dirty: false,
    invalid: false,
    valid: true,
    pristine: false,
    submitting: false,
    submitFailed: false,
    values: {},
    handleSubmit: sinon.stub().returns(sinon.spy()),
    untouch: sinon.spy(),
    untouchAll: sinon.spy(),
    touch: sinon.spy(),
    touchAll: sinon.spy(),
    initializeForm: sinon.spy(),
    resetForm: sinon.spy(),
    destroyForm: sinon.spy(),
    asyncValidate: sinon.spy(),
    onCancel: sinon.spy(),
    fields: {}
  };
  fields.forEach((field) => {
    props.fields[field] = { // todo: should invest in making this better (see below)
      onChange: sinon.spy()
    };
  });
  return props;
}

export function getResponseForEntity(entityType, entityId, entityData) {
  return {
    payload: Immutable.fromJS({
      entities: {
        [entityType]: {
          [entityId]: entityData
        }
      },
      result: entityId
    })
  };
}

export const findMenuItemLinkByText = (wrapper, text) => {
  return wrapper.find(`MenuItemLink[text="${text}"]`);
};

export const findMenuItemByText = (wrapper, text) => {
  return wrapper.find('MenuItem').children().findWhere(child => child.text() === text);
};


export function formFields(formValue) {
  if (typeof formValue === 'object' && formValue !== null) {
    if (Array.isArray(formValue)) {
      const output = formValue.map(v => formFields(v));
      output.addField = sinon.spy();
      output.removeField = sinon.spy();
      return output;
    }
    const output = {};
    Object.entries(formValue).forEach(([name, value]) => {
      output[name] = formFields(value);
    });
    return output;
  }
  return {
    value: formValue,
    onChange: sinon.spy()
  };
}


export const stubArrayFieldMethods = (field) => Object.assign(field, { // eslint-disable-line no-restricted-properties
  removeField: sinon.spy(),
  addField: sinon.spy()
});

// need this as an error in 'afterEach' skips all the tests. The output will say that only 1 test
// is failed, but there could be multiple test are failed
export const testWithHooks = ({ beforeFn, afterFn }) => (description, testFn) => {
  it(description, () => {
    if (typeof beforeFn === 'function') {
      beforeFn();
    }
    testFn();
    if (typeof afterFn === 'function') {
      afterFn();
    }
  });
};
