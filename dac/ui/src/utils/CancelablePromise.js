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
export class Cancel {
  stack = new Error().stack;
}

export default class CancelablePromise extends Promise {
  constructor(fcn) {
    let _reject;
    super((resolve, reject) => {
      _reject = reject;
      return fcn(resolve, reject);
    });
    this._reject = _reject;
  }

  _cancelObject = null;
  _reject = null;

  // allowed even if already resolved
  // cancel will also always win - any subsequently fired callbacks will be canceled
  // this is different from other Promise resolutions, but matches how people actually need it to work
  cancel() {
    this._cancelObject = this._cancelObject || new Cancel();
    this._reject();
  }

  then() {
    const superThen = super.then;
    const cancelHandler = superThen.call(
      this,
      ((val) => this._cancelObject ? Promise.reject(this._cancelObject) : val),
      ((error) => Promise.reject(this._cancelObject || error))
    );
    return superThen.apply(cancelHandler, arguments);
  }

  catch(onRejected) {
    return this.then(undefined, onRejected);
  }
}

window.CancelablePromise = CancelablePromise;
