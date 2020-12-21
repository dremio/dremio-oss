/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import invariant from 'invariant';
import { merge, get, set, result } from 'lodash/object';
import { capitalize } from 'lodash';
import Immutable from 'immutable';

// todo: loc

export function isEmptyValue(value) {
  return value === '' || value === undefined || value === null;
}

export function makeLabelFromKey(key) {
  if (!key) return key;
  const keyParts = key.split('.');
  return capitalize(keyParts[keyParts.length - 1]);
}

export function isEmptyObject(value) {
  const keys = value && Object.keys(value);
  return !value || keys.length === 0 || !keys.some((k) => value[k]);
}

export function isRequired(key, label) {
  const finalLabel = label || capitalize(key);
  return function(values) {
    const value = result(values, key);
    const isEmptyArr = value && (value instanceof Immutable.List && !value.size);
    if (isEmptyValue(value) || (typeof value === 'number' && isNaN(value)) || isEmptyArr) {
      // use lodash.set in case key has dotted path
      return set({}, key, `${finalLabel} is required.`);
    }
  };
}

export function confirmPassword(password, confirm) {
  return function(values) {
    if ((values[password] || values[confirm]) && values[password] !== values[confirm]) {
      return set({}, confirm, 'Passwords don\'t match.');
    }
  };
}

export function isEmail(key) {
  return function(values) {
    const email = values[key];
    if (!/^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}$/i.test(email)) {
      return set({}, key, 'Not a valid email address.');
    }
  };
}

export function notEmptyArray(key, message) {

  return function(values) {
    const value = result(values, key);
    if (!value || value.length === 0) {
      return set({}, key, message || `${key} should not be empty.`);
    }
  };
}

export function notEmptyObject(key, message) {
  return function(values) {
    const value = result(values, key);
    if (isEmptyObject(value)) {
      return set({}, key, message || `${key} should not be empty.`);
    }
  };
}

export function isRequiredIfAnotherPropertyEqual(key, dependetKey, dependetValue, label) {
  return function(values) {
    if (!get(values, key) && get(values, dependetKey) === dependetValue) {
      return set({}, key, `${label || makeLabelFromKey(key)} is required.`);
    }
  };
}

export function isNumber(key, label = key) {
  return function(values) {
    const value = result(values, key);
    if (!isEmptyValue(value) && isNaN(Number(value))) {
      return set({}, key, `${label} must be a number.`);
    }
  };
}

export function isWholeNumber(key, label = key) {
  return function(values) {
    const value = result(values, key);
    if (!isEmptyValue(value) && (isNaN(Number(value)) || Number(value) % 1 || Number(value) < 0)) {
      return set({}, key, `${label} must be an integer greater than or equal to zero.`);
    }
  };
}

export function isInteger(key, label = key) {
  return function(values) {
    const value = result(values, key);
    if (!isEmptyValue(value) && (isNaN(Number(value)) || Number(value) % 1)) {
      return set({}, key, `${label} must be an integer.`);
    }
  };
}

export const isIntegerWithLimits = (key, label = key, lowLimit = null, topLimit = null) => {
  const isIntegerChecker = isInteger(key, label);
  if (lowLimit === null && topLimit === null) return isIntegerChecker;
  invariant(lowLimit === null || topLimit === null || lowLimit <= topLimit,
    'lowLimit must be <= topLimit');
  let limitMessage;
  if (topLimit !== null && lowLimit !== null) {
    limitMessage = `${label} must be an integer between ${lowLimit} and ${topLimit}.`;
  } else if (topLimit !== null) {
    limitMessage = `${label} must be an integer less than or equal ${topLimit}.`;
  } else {
    limitMessage = `${label} must be an integer greater than or equal to ${lowLimit}.`;
  }
  return values => {
    const isIntCheckResult = isIntegerChecker(values);
    if (get(isIntCheckResult, key)) {
      return isIntCheckResult;
    }
    const value = result(values, key);
    if (!isEmptyValue(value) && (lowLimit !== null && value < lowLimit) ||
      (topLimit !== null && value > topLimit)) {
      return set({}, key, limitMessage);
    }
  };
};

export const isIntegerKey1LessThenKey2 = (key1, key2, label = key1 + ', ' + key2) => {
  const isKey1Integer = isInteger(key1, `${key1} is not an integer`);
  const isKey2Integer = isInteger(key1, `${key2} is not an integer`);

  return values => {
    const isKey1IntegerResult = isKey1Integer(values, `${key1} is not an integer`);
    if (get(isKey1IntegerResult, key1)) {
      return isKey1IntegerResult;
    }

    const isKey2IntegerResult = isKey2Integer(values, `${key2} is not an integer`);
    if (get(isKey2IntegerResult, key2)) {
      return isKey2IntegerResult;
    }

    const key1Val = result(values, key1);
    const key2Val = result(values, key2);
    if (key1Val < key2Val) {
      return set({}, key1, label);
    }
  };
};

export function isPositiveInteger(key, label = key) {
  return function(values) {
    const value = result(values, key);
    if (!isEmptyValue(value) && (isNaN(Number(value)) || Number(value) % 1 || Number(value) <= 0)) {
      return set({}, key, `${label} must be an integer greater than zero.`);
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
      validations[key] = 'At least one bound should not be None.';
    } else {
      return {};
    }
  }
  return validations;
}

export function noDoubleQuotes(key) {
  return function(values) {
    const value = result(values, key);
    if (value && value.includes('"')) {
      return set({}, key, 'Double quotes are not allowed.');
    }
  };
}

export function noSpaces(key) {
  return function(values) {
    const value = result(values, key);
    if (value && value.includes(' ')) {
      return set({}, key, 'Spaces are not allowed.');
    }
  };
}

export function applyValidators(values, validators) {
  const messages = merge({}, ...validators.map((v) => v(values)));
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
