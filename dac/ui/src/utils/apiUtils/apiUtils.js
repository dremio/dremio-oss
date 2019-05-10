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
import uuid from 'uuid';
import Immutable from 'immutable';
import { API_URL_V2, API_URL_V3 } from 'constants/Api';
import { InvalidRSAA, InternalError, RequestError, ApiError } from 'redux-api-middleware/lib/errors';
import localStorageUtils from 'utils/storageUtils/localStorageUtils';

class ApiUtils {
  isApiError(error) {
    return error instanceof InvalidRSAA ||
      error instanceof InternalError ||
      error instanceof RequestError ||
      error instanceof ApiError;
  }

  getEntityFromResponse(entityType, response) {
    return response.payload.getIn(['entities', entityType, response.payload.get('result')]);
  }

  loadDatasetInfo(resourceId, tableId) {
    return $.ajax({
      url: `${API_URL_V2}/dataset/${resourceId}.${tableId}?view=explore`,
      type: 'GET',
      error: (error) => {
        console.error('LOADING ROWS REQUEST FAILED', error);
      }
    });
  }

  createFormAsyncValidate(path, mapper) {
    return function(values) {
      return fetch(`${API_URL_V2}${path}`, {
        method: 'POST',
        body: JSON.stringify(mapper(values))
      }).then((response) => {
        if (response.ok) {
          return response;
        }
        throw {_error: response.statusText};
      }).then(
        (response) => response.json()
      ).then((data) => {
        if (data.validationError) {
          throw data.validationError;
        }
      });
    };
  }

  parseErrorsToObject(response) {
    const errorFields = {};
    if (response.validationErrorMessages && response.validationErrorMessages.fieldErrorMessages) {
      const { fieldErrorMessages } = response.validationErrorMessages;
      for (const key in fieldErrorMessages) {
        errorFields[key] = fieldErrorMessages[key][0];
      }
    }
    return errorFields;
  }

  attachFormSubmitHandlers(promise) {
    // throws reject promise for reduxForm's handleSubmit
    return promise.then((action) => {
      if (action && action.error) {
        const error = action.payload;
        const {response} = error;
        const errorId = uuid.v4();
        if (response) {
          this.handleError(response);
        }
        throw {_error: { message: error.message, id: errorId }};
      }
      return action;
    }).catch(this.handleError);
  }

  handleError = (error) => {
    if (error.errorMessage) {
      const errorFields = this.parseErrorsToObject(error);
      const errors = {
        _error: { message: Immutable.Map(error), id: uuid.v4() },
        ...errorFields
      };
      throw errors;
    }
    if (error.meta && error.meta.validationError) {
      throw error.meta.validationError;
    }
    if (error.statusText) { // chris asks: how would this be possible? (fetch API rejects with TypeError)
      throw {_error: 'Request Error: ' + error.statusText}; // todo: loc
    }
    throw error;
  };

  fetch(endpoint, options = {}, version = 3) {
    const apiVersion = (version === 3) ? API_URL_V3 : API_URL_V2;

    const headers = new Headers({
      'Content-Type': 'application/json',
      ...options.headers,
      'Authorization': localStorageUtils.getAuthToken()
    }); // protect against older chrome browsers

    return fetch(`${apiVersion}/${endpoint}`, { ...options, headers }).then(response => response.ok ? response : Promise.reject(response));
  }

  /**
   * Returns headers that enables writing numbers as strings for job data
   *
   * key should match with {@see WebServer#X_DREMIO_JOB_DATA_NUMBERS_AS_STRINGS} in {@see WebServer.java}
   * @returns headers object
   * @memberof ApiUtils
   */
  getJobDataNumbersAsStringsHeader() {
    return {
      'x-dremio-job-data-number-format': 'number-as-string'
    };
  }

  // error response may contain moreInfo or errorMessage field, that should be used for error message
  async getErrorMessage(prefix, response) {
    const err = await response.json();
    const errText = err && (err.moreInfo || err.errorMessage) || '';
    return errText.length ? `${prefix}: ${errText}` : `${prefix}.`;
  }
}

const apiUtils = new ApiUtils();

export default apiUtils;
