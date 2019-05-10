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

const disabler = (e) => {
  e.preventDefault();
  e.stopImmediatePropagation();
  return false;
};

const getElOrBody = el => el || document.getElementsByTagName('body')[0];

const cursorHelper = () => {
  let originalCursor = null; // null if setCursor was not called or reset method was called

  return {
    // use this method if you need to change a cursor for whole page. For example, when you start resizing
    setCursor: (cursor) => {
      const body = getElOrBody();
      // if there are several calls of setCursor, we record cursor state only for first call or call after resetCusor call
      if (originalCursor === null) { //record only first ponter
        originalCursor = body.style.cursor;
      }
      body.style.cursor = cursor;
    },

    //resets page's cursor to its original state
    resetCursor: () => {
      getElOrBody().style.cursor = originalCursor;
      originalCursor = null;
    }
  };
};

export const domUtils = {
  disableSelection: el => {
    getElOrBody(el).addEventListener('mousedown', disabler, false);
  },

  enableSelection: (el) => {
    getElOrBody(el).removeEventListener('mousedown', disabler, false);
  },

  // as safari does not support pointer events, we have to use this hook
  captureMouseEvents: (onMouseMove, onMouseUp) => {
    if (typeof onMouseMove !== 'function' ||
      typeof onMouseUp !== 'function') {
      throw new Error('onMouseMove and onMouseUp must be provided');
    }
    const mouseMoveHandler = e => {
      onMouseMove(e);

      return disabler(e);
    };
    const mouseUpHandler = e => {
      onMouseUp(e);
      //clear the listeners to enable default flow
      document.removeEventListener('mousemove', mouseMoveHandler, true);
      document.removeEventListener('mouseup', mouseUpHandler, true);

      return disabler(e);
    };
    document.addEventListener('mousemove', mouseMoveHandler, true);
    document.addEventListener('mouseup', mouseUpHandler, true);
  },

  ...cursorHelper()
};
