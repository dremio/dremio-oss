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
import notification from './notification';

describe('notification reducer', () => {

  const initialState =  {message: 'old', level: 'bad'};

  it('should return unaltered state by default', () => {
    const result = notification(initialState, {type: 'bla'});
    expect(result).to.equal(initialState);
  });

  it('should replace notification if present in meta', () => {
    const result = notification(initialState, {type: 'bla', meta: {notification: {message: 'foo'}}});
    expect(result).to.eql({message: 'foo'});
  });

  it('should set notification if present in meta and type is function', () => {
    const result = notification(initialState, {type: 'bla', meta: {notification: () => ({message: 'foo'})}});
    expect(result).to.eql({message: 'foo'});
  });

  it('should set notification from errorMessage and default moreInfo if error = true and notification = true', () => {
    const defaultMoreInfo = 'Something went wrong.';
    const result = notification(initialState, {
      type: 'bla',
      error: true,
      payload: {errorMessage: 'message'},
      meta: {notification: true}
    });
    expect(result).to.eql({
      message: Immutable.Map({ message: 'message', moreInfo: defaultMoreInfo }),
      level: 'error'
    });
  });

  it('should set notification from payload.response if error = true and notification = true', () => {
    const result = notification(initialState, {
      type: 'bla',
      error: true,
      payload: { response: { errorMessage: 'message', moreInfo: 'more info' } },
      meta: {notification: true}
    });
    expect(result).to.eql({ message: Immutable.Map({ message: 'message', moreInfo: 'more info' }), level: 'error' });
  });

  it('should return unaltered state if notification is true but no error', () => {
    const result = notification(initialState, {type: 'bla', meta: { notification: true }});
    expect(result).to.equal(initialState);
  });

  it('should set notification with default message when error = true and notification = true', () => {
    const defaultMessage = 'Something went wrong.';
    const result = notification(initialState, {
      type: 'bla',
      error: true,
      meta: {notification: true}
    });
    expect(result).to.eql({ message: Immutable.Map({ message: defaultMessage }), level: 'error' });
  });

  it('should set default message to "data has changed" when status = 409', () => {
    const defaultMessage409 = 'The data has been changed since you last accessed it. Please reload the page.';
    const result = notification(initialState, {
      type: 'bla',
      payload: {
        status: 409
      },
      error: true,
      meta: {notification: true}
    });
    expect(result).to.eql({ message: Immutable.Map({ message: defaultMessage409 }), level: 'error' });
  });
});
