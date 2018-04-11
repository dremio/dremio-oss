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
import config from 'utils/config';

const fileABug = function(error) {
  const data = {...config};
  data.ts = new Date(data.ts);
  data.ua = navigator.userAgent;
  data.location = window.location;
  const pageData = Object.keys(data).map(function(key) {
    return key + ': ' + data[key];
  }).join('\n');
  const errorText = !error ? '' : `\n\nError Message:
{noformat}${truncate(error.message)}{noformat}

Stack Trace:
{noformat}${truncate(error.stack)}{noformat}`;

  const desc = `


Steps to Reproduce:
#

Observed:


${errorText}
`;

  const env = 'Page data:\n{noformat}\n' + pageData + '\n{noformat}';

  const url = 'https://dremio.atlassian.net/secure/CreateIssueDetails!init.jspa?' +
    'priority=10000&pid=10100&issuetype=10004&labels=filed_from_app&description=' +
    encodeURIComponent(desc) + '&environment=' + encodeURIComponent(env);

  window.open(url, '_blank');
};

export default fileABug;

window.fileABug = fileABug; // expose for other tools (e.g. bookmarklet)

// heuristic to avoid Jira showing a blank screen because the URL is too long
function truncate(str = '') {
  const lines = str.split('\n');
  if (lines.length <= 10) return str;
  return lines.slice(0, 10).join('\n') + '\n…';
}
