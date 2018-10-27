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
import { AUTO_PREVIEW_DELAY } from 'constants/Constants';

class ActionUtils {
  shouldLoad(resource) {
    if (!resource) {
      return false;
    }
    const { isInProgress, isInvalid } = resource.get ? resource.toObject() : resource;
    return !isInProgress && isInvalid;
  }

  runAutoPreview(submitForm) {
    clearTimeout(this.autoPreviewTimer);
    this.autoPreviewTimer = setTimeout(submitForm, AUTO_PREVIEW_DELAY);
  }

  humanizeNotificationMessage = (errorMessage) => (payload) => {
    const defaultMessage = payload && payload.status === 409
      ? la('The data has been changed since you last accessed it. Please reload the page.')
      : la('Something went wrong.');
    const _errorMessage = errorMessage ||
      payload && payload.errorMessage ||
      payload && payload.response && payload.response.errorMessage ||
      defaultMessage;
    const moreInfo = payload && payload.response && payload.response.moreInfo || defaultMessage;
    const message = _errorMessage === moreInfo
      ? Immutable.Map({ message: _errorMessage })
      : Immutable.Map({ message: _errorMessage, moreInfo });
    return {
      message,
      level: 'error'
    };
  };

  // added 'actionUtils_' in the begining to make it easier to find out source of action types during debugging
  generateRequestActions = prefix => ({
    start: `actionUtils_${prefix}_REQUEST_START`,
    success: `actionUtils_${prefix}_REQUEST_SUCCESS`,
    failure: `actionUtils_${prefix}_REQUEST_FAILURE`
  })
}

const actionUtils = new ActionUtils();

export default actionUtils;
