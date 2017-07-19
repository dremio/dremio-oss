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
/*
**Languages for Debian based OS and macOS**
* To select language on compile step run command "language=en npm start" or
"language=fr npm start" or "language=en npm run startprod" etc.
* To select language runtime run command "useRunTimeLanguage=true npm start" or
"useRunTimeLanguage=true npm start" or "useRunTimeLanguage=true npm run startprod" etc.

**Languages for Windows**
* To select language on compile step run command "set language=en npm start" or
" setlanguage=fr npm start" or "set language=en npm run startprod" etc.
* To select language runtime run command "set useRunTimeLanguage=true npm start" or
"set useRunTimeLanguage=true npm start" or "set useRunTimeLanguage=true npm run startprod" etc.

*/


const languages = require('./languages.json');
const language = process.env.language || 'en';

module.exports = function(content) {
  const start = content.indexOf('la(');
  const end = content.indexOf(')', start);
  if (start !== -1 && end !== -1) {
    const key = content.substring(start + 3, end);
    const text = languages[language][key];
    content = content.substring(0, start) + '\'' + text + '\'' + content.substring(end + 1, content.length); // eslint-disable-line no-param-reassign
  }
  return content;
};
