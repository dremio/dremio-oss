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
import {
  applyValidators,
  isRequired, isNumber, isInteger, isWholeNumber,
  isRegularExpression, isEmail, confirmPassword,
  makeLabelFromKey, isIntegerWithLimits, noDoubleQuotes,
  noSpaces
} from './validation';

describe('validation', () => {
  describe('isRequired', () => {
    it('should fail if null or empty string', () => {
      expect(isRequired('foo')({})).to.eql({foo: 'Foo is required.'});
      expect(isRequired('foo')({foo: ''})).to.eql({foo: 'Foo is required.'});
    });

    itShouldPassForGoodValues(isRequired, ['a', 0]);
    itShouldWorkForNestedKeys(isRequired, 'a', undefined);
    itShouldPutLabelInMessage(isRequired, undefined);
  });

  describe('confirmPassword', () => {
    it('should not fail when both password and confirmation password are empty', () => {
      const values = {password: '', passwordVerify: ''};
      expect(confirmPassword('password', 'passwordVerify')(values)).to.be.undefined;
    });

    it('should not fail when both password and confirmation password matches', () => {
      const values = {password: 'password', passwordVerify: 'password'};
      expect(confirmPassword('password', 'passwordVerify')(values)).to.be.undefined;
    });

    it('should fail when password and confirmation password don\'t match', () => {
      const validator = confirmPassword('password', 'passwordVerify');
      expect(validator({password: 'p', passwordVerify: ''})).to.be.eql({passwordVerify: 'Passwords don\'t match.'});
      expect(validator({password: '', passwordVerify: 'p'})).to.be.eql({passwordVerify: 'Passwords don\'t match.'});
    });
  });

  describe('isNumber', () => {
    itShouldIgnoreMissingValues(isNumber);
    itShouldPassForGoodValues(isNumber, [1, '-1']);
    itShouldFailForBadValues(isNumber, ['a', 'sdf']);
    itShouldWorkForNestedKeys(isNumber, '1', 'asdf');
    itShouldPutLabelInMessage(isNumber, 'asdf');
  });

  describe('isWholeNumber', () => {
    itShouldIgnoreMissingValues(isWholeNumber);
    itShouldPassForGoodValues(isWholeNumber, [1, 0, '2']);
    itShouldFailForBadValues(isWholeNumber, ['a', 'sdf', 1.1, '1.1', -1]);
    itShouldWorkForNestedKeys(isWholeNumber, '1', 'asdf');
    itShouldPutLabelInMessage(isWholeNumber, 'asdf');
  });

  describe('isInteger', () => {
    itShouldIgnoreMissingValues(isInteger);
    itShouldPassForGoodValues(isInteger, [1, '-1']);
    itShouldFailForBadValues(isInteger, ['a', 'sdf', 1.1, '1.1']);
    itShouldWorkForNestedKeys(isInteger, '1', 'asdf');
    itShouldPutLabelInMessage(isInteger, 'asdf');
  });

  describe('isRegularExpression', () => {
    itShouldIgnoreMissingValues(isRegularExpression);
    itShouldPassForGoodValues(isRegularExpression, ['\\d+']);
    itShouldFailForBadValues(isRegularExpression, ['\\', '(\\d']);
    itShouldWorkForNestedKeys(isRegularExpression, '\\d', '(\\d');
  });

  describe('isEmail', () => {
    it('should fail for don\'t correct email', () => {
      expect(isEmail('email')({ email: 'dremio' })).to.eql({email: 'Not a valid email address.'});
      expect(isEmail('email')({ email: 'dremio@' })).to.eql({email: 'Not a valid email address.'});
      expect(isEmail('email')({ email: 'dremio@gmail' })).to.eql({email: 'Not a valid email address.'});
      expect(isEmail('email')({ email: 'dremio@gmail.' })).to.eql({email: 'Not a valid email address.'});
      expect(isEmail('email')({ email: 'dremio@gmail.c' })).to.eql({email: 'Not a valid email address.'});
    });

    it('should success for correct email', () => {
      expect(isEmail('email')({ email: 'dremio@gmail.com' })).to.eql(undefined);
    });
  });

  describe('noDoubleQuotes', () => {
    it('should prevent double quotes', () => {
      expect(noDoubleQuotes('name')({name: 'a"b'})).to.eql({name: 'Double quotes are not allowed.'});
      expect(noDoubleQuotes('name')({name: 'a""b'})).to.eql({name: 'Double quotes are not allowed.'});
      expect(noDoubleQuotes('name')({name: 'a"""b'})).to.eql({name: 'Double quotes are not allowed.'});
    });
    it('should allow name w/o double quotes', () => {
      expect(noDoubleQuotes('name')({name: 'ab'})).to.be.undefined;
    });
  });

  describe('noSpaces', () => {
    it('should prevent spaces', () => {
      expect(noSpaces('name')({name: 'a b'})).to.eql({name: 'Spaces are not allowed.'});
      expect(noSpaces('name')({name: 'a b   c'})).to.eql({name: 'Spaces are not allowed.'});
      expect(noSpaces('name')({name: ' ab'})).to.eql({name: 'Spaces are not allowed.'});
      expect(noSpaces('name')({name: 'ab '})).to.eql({name: 'Spaces are not allowed.'});
    });
    it('should allow name w/o spaces', () => {
      expect(noSpaces('name')({name: 'ab'})).to.be.undefined;
    });
  });

  describe('applyValidators', () => {
    it('should call and merge results of multiple validators', () => {
      expect(applyValidators({}, [
        isRequired('foo.a'),
        isRequired('foo.b')]
      )).to.eql({foo: {a: 'Foo.a is required.', b: 'Foo.b is required.'}});
    });
  });

  describe('makeLabelFromKey', () => {
    it('should return capitalized simple key', () => {
      expect(makeLabelFromKey('abc')).to.equal('Abc');
      expect(makeLabelFromKey('ABC')).to.equal('Abc');
    });
    it('should take last token from complex key', () => {
      expect(makeLabelFromKey('a.b.cde')).to.equal('Cde');
    });
    it('should handle empty key', () => {
      expect(makeLabelFromKey('')).to.equal('');
      expect(makeLabelFromKey(null)).to.equal(null);
    });
  });

  describe('isIntegerWithLimits', () => {
    it('throws exception if inputs are invalid', () => {
      expect(() => isIntegerWithLimits('field', 'field', 5, 4)).to.throw();
    });

    describe('fallbacks to isInteger', () => {
      itShouldFailForBadValues(isIntegerWithLimits, ['a', 'sdf', 1.1, '1.1']);
      itShouldIgnoreMissingValues(isIntegerWithLimits);
    });

    describe('lowLimit only', () => {
      const testCase = (value, limit, isInvalidValue = true) => {
        it(`'${value}' does${isInvalidValue ? ' not' : ''} meet '${limit}' low limit`, () => {
          const fieldName = 'f1';
          const data = {
            [fieldName]: value
          };
          const validator = isIntegerWithLimits(fieldName, fieldName, limit);
          if (isInvalidValue) {
            expect(validator(data)).to.be.eql({
              [fieldName]: `${fieldName} must be an integer greater than or equal to ${limit}.`
            });
          } else {
            expect(validator(data)).to.be.empty;
          }
        });
      };

      // invalid values
      testCase(1, 2);
      testCase(-5, 0);
      testCase(-5, -4);
      // valid values
      testCase(10, 2, false);
      testCase(11, -5, false);
      testCase(3, 3, false);
    });

    describe('topLimit only', () => {
      const testCase = (value, limit, isInvalidValue = true) => {
        it(`'${value}' does${isInvalidValue ? ' not' : ''} meet '${limit}' top limit`, () => {
          const fieldName = 'f1';
          const data = {
            [fieldName]: value
          };
          const validator = isIntegerWithLimits(fieldName, fieldName, null, limit);
          if (isInvalidValue) {
            expect(validator(data)).to.be.eql({
              [fieldName]: `${fieldName} must be an integer less than or equal ${limit}.`
            });
          } else {
            expect(validator(data)).to.be.empty;
          }
        });
      };

      // invalid values
      testCase(2, 1);
      testCase(0, -5);
      testCase(-4, -5);
      // valid values
      testCase(2, 10, false);
      testCase(-5, 11, false);
      testCase(3, 3, false);
    });

    describe('both limits', () => {
      const testCase = (value, lowLimit, topLimit, isInvalidValue = true) => {
        it(`'${value}' does${isInvalidValue ? ' not' : ''} lie between '${lowLimit}' and '${topLimit}'`, () => {
          const fieldName = 'f1';
          const data = {
            [fieldName]: value
          };
          const validator = isIntegerWithLimits(fieldName, fieldName, lowLimit, topLimit);
          if (isInvalidValue) {
            expect(validator(data)).to.be.eql({
              [fieldName]: `${fieldName} must be an integer between ${lowLimit} and ${topLimit}.`
            });
          } else {
            expect(validator(data)).to.be.empty;
          }
        });
      };

      // invalid values
      testCase(0, 1, 5);
      testCase(6, 1, 5);
      // valid values
      testCase(1, 1, 5, false);
      testCase(5, 1, 5, false);
      testCase(3, 1, 5, false);
    });
  });
});


function itShouldWorkForNestedKeys(validator, goodValue, badValue) {
  it('should work for nested keys', () => {
    expect(validator('foo.bar')({foo: {bar: goodValue}})).to.be.undefined;
    expect(validator('foo.bar')({foo: {bar: badValue}}).foo.bar).to.not.be.undefined;
  });
}

function itShouldPutLabelInMessage(validator, badValue) {
  it('should put label in message', () => {
    expect(validator('foo', 'Label')({foo: badValue}).foo).to.startsWith('Label');
  });
}

function itShouldIgnoreMissingValues(validator) {
  it('should pass if null or empty string', () => {
    expect(validator('foo')({})).to.be.undefined;
    expect(validator('foo')({foo: ''})).to.be.undefined;
  });
}

function itShouldPassForGoodValues(validator, goodValues) {
  it('should pass for good values', () => {
    goodValues.forEach((value) => {
      expect(validator('foo')({foo: value})).to.be.undefined;
    });
  });
}

function itShouldFailForBadValues(validator, badValues) {
  it('should fail for bad values', () => {
    badValues.forEach((value) => {
      expect(validator('foo')({foo: value}).foo).to.not.be.undefined;
    });
  });
}
