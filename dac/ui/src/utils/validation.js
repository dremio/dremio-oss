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
import { assign, set, result } from 'lodash/object';
import { capitalize } from 'lodash';
import Immutable from 'immutable';

export function isEmptyValue(value) {
  return value === '' || value === undefined || value === null;
}

export function isEmptyObject(value) {
  const keys = value && Object.keys(value);
  return !value || keys.length === 0 || !keys.some((k) => value[k]);
}

export function isRequired(key, label = key) {
  return function(values) {
    const value = result(values, key);
    const isEmptyArr = value && (value instanceof Immutable.List && !value.size);
    if (isEmptyValue(value) || (typeof value === 'number' && isNaN(value)) || isEmptyArr) {
      return set({}, key, `${capitalize(label)} is required`);
    }
  };
}

export function confirmPassword(password, confirm) {
  return function(values) {
    if ((values[password] || values[confirm]) && values[password] !== values[confirm]) {
      return set({}, confirm, 'Passwords don\'t match');
    }
  };
}

export function isEmail(key) {
  return function(values) {
    const email = values[key];
    if (!/^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}$/i.test(email)) {
      return set({}, key, 'Not a valid email address');
    }
  };
}

export function notEmptyArray(key, message) {

  return function(values) {
    const value = result(values, key);
    if (!value || value.length === 0) {
      return set({}, key, message || `${key} should not be empty`);
    }
  };
}

export function notEmptyObject(key, message) {
  return function(values) {
    const value = result(values, key);
    if (isEmptyObject(value)) {
      return set({}, key, message || `${key} should not be empty`);
    }
  };
}

export function isRequiredIfAnotherPropertyEqual(key, dependetKey, dependetValue) {

  return function(values) {
    if (!values[key] && values[dependetKey] !== dependetValue) {
      return {[key]: `${key} is required`};
    }
  };
}

export function isNumber(key, label = key) {
  return function(values) {
    const value = result(values, key);
    if (!isEmptyValue(value) && isNaN(Number(value))) {
      return set({}, key, `${label} must be a number`);
    }
  };
}

export function isWholeNumber(key, label = key) {

  return function(values) {
    const value = result(values, key);
    if (!isEmptyValue(value) && isNaN(Number(value)) || value !== undefined && Number(value) < 0) {
      return set({}, key, `${label} must be a number > 0`);
    }
  };
}

export function isInteger(key, label = key) {
  return function(values) {
    const value = result(values, key);
    if (!isEmptyValue(value) && (isNaN(Number(value)) || Number(value) % 1)) {
      return set({}, key, `${label} must be an integer`);
    }
  };
}

export function isPositiveInteger(key, label = key) {
  return function(values) {
    const value = result(values, key);
    if (!isEmptyValue(value) && (isNaN(Number(value)) || Number(value) % 1 || Number(value) <= 0)) {
      return set({}, key, `${label} must be a positive integer`);
    }
  };
}

export function isRegularExpression(key, message) {
  return function(values) {
    const value = result(values, key);
    if (isEmptyValue(value)) {
      return;
    }
    try {
      RegExp(value);
    } catch (err) {
      return set({}, key, message || err.message);
    }
  };
}

export function applyBoundValidator(values, fields) {
  const validations = {};
  for (const key of fields) {
    const value = result(values, key);
    if (isEmptyValue(value)) {
      validations[key] = 'At least one bound should not be None';
    } else {
      return {};
    }
  }
  return validations;
}

export function applyValidators(values, validators) {
  const messages = assign({}, ...validators.map((v) => v(values)));
  return messages;
}

// TODO: add unit test
export function getListErrorsFromNestedReduxFormErrorEntity(errors) {
  const listOfErrors = Object.keys(errors);
  const resultList = [];
  listOfErrors.forEach(error => {
    if (Array.isArray(errors[error])) {
      resultList.push(...errors[error].map(getListErrorsFromNestedReduxFormErrorEntity));
    } else if (typeof errors[error] === 'object') {
      resultList.push(...getListErrorsFromNestedReduxFormErrorEntity(errors[error]));
    } else {
      resultList.push(errors[error]);
    }
  });
  return resultList;
}
