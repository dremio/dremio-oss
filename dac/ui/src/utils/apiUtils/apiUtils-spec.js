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
import { ApiError } from 'redux-api-middleware/lib/errors';

import apiUtils from './apiUtils';

describe('apiUtils', () => {
  describe('attachFormSubmitHandlers', () => {
    it('throws on error', () => {
      const promise = apiUtils.attachFormSubmitHandlers(Promise.resolve({
        error: true,
        payload: new ApiError(500, 'statusText')
      }));
      return expect(promise).to.be.rejectedWith({_error: 'statusText'});
    });

    it('throws errorMessage if there is one', () => {
      const promise = apiUtils.attachFormSubmitHandlers(Promise.resolve({
        error: true,
        payload: {
          response: {
            errorMessage: 'errorMessage',
            statusText: 'statusText'
          }
        }
      }));
      return expect(promise).to.be.rejected;
    });

    it('throws validationError if present', () => {
      const promise = apiUtils.attachFormSubmitHandlers(Promise.resolve({
        error: true,
        payload: new ApiError(500, 'statusText', {meta: {
          validationError: {
            fieldName: 'validationError'
          }
        }})
      }));

      return expect(promise).to.be.rejected.and.to.eventually.eql({fieldName: 'validationError'});
    });

    it('parse errors to object', () => {
      const json = {
        'errorMessage' : 'Error message',
        'validationErrorMessages' : {
          'fieldErrorMessages' : {
            'externalBucket' : [
              'may not be empty'
            ],
            'property' : [
              'may not be empty'
            ]
          }
        }
      };
      const expectedResult = {
        externalBucket: 'may not be empty',
        property: 'may not be empty'
      };

      expect(apiUtils.parseErrorsToObject(json)).to.eql(expectedResult);
    });

  });

  describe('getErrorMessage', () => {
    let jsonFn;
    beforeEach(() => {
      jsonFn = sinon.stub();
    });

    it('should return prefix if no message provided', () => {
      jsonFn.resolves(null);
      return apiUtils.getErrorMessage('==>', {json: jsonFn}).then((msg) => {
        expect(msg).to.equal('==>.');
      });
    });
    it('should get errorMessage from error response', () => {
      jsonFn.resolves({errorMessage: 'Message'});
      return apiUtils.getErrorMessage('==>', {json: jsonFn}).then((msg) => {
        expect(msg).to.equal('==>: Message');
      });
    });
    it('should get moreInfo from error response', () => {
      jsonFn.resolves({moreInfo: 'Info'});
      return apiUtils.getErrorMessage('==>', {json: jsonFn}).then((msg) => {
        expect(msg).to.equal('==>: Info');
      });
    });
  });

});
