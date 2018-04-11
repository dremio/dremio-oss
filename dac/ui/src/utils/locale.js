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
import IntlMessageFormat from 'intl-messageformat';
import enStrings from 'dyn-load/locales/en.json';

export function getLocale() {
  // todo: write code to actually handle multiple options
  let language = (navigator.languages && navigator.languages[0]) || navigator.language;
  language = 'en'; // hardcode to only supported option today
  let localeStrings = enStrings;
  try {
    if (localStorage.getItem('language')) {
      if (localStorage.getItem('language') === 'ids') {
        localeStrings = undefined;
      } else if (localStorage.getItem('language') === 'double') {
        for (const [key, value] of Object.entries(localeStrings)) {
          localeStrings[key] = value + ' ' + value;
        }
      } else {
        language = localStorage.getItem('language');
      }
    }
  } catch (e) {
    console.error(e);
  }
  return { language, localeStrings };
}

export function formatMessage(message, values) {
  const msg = new IntlMessageFormat(enStrings[message], getLocale().language);
  // todo: write code to actually handle multiple options
  return msg.format(values);
}

export function haveLocKey(key) {
  return key in enStrings;
}

