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

/**
 * Dynamically import current browser locale
 */
async function loadCurrentLocale(locale: string) {
  if (locale === "en") return;
  try {
    await import(`dayjs/locale/${locale}.js`);
  } catch (e) {
    //Locale is en instead of en-US, noop
    if (locale.indexOf("-") === -1) return;
    try {
      const cur = locale.split("-")[0];
      if (cur === "en") return; //DayJS comes with en already
      await import(`dayjs/locale/${locale.split("-")[0]}.js`);
    } catch (e) {
      //Noop
    }
  }
}

export default loadCurrentLocale;
