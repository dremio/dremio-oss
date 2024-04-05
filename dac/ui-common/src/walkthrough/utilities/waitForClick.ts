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
/* eslint-disable */
export async function waitForElement(
  selector: string,
  timeout = 10000,
  refresh = 500,
) {
  const start = Date.now();

  while (Date.now() - start < timeout) {
    const el = document.querySelector(selector);
    if (el) {
      return el;
    }
    await new Promise((resolve) => setTimeout(resolve, refresh));
  }

  return new Promise(() => {});
}

export async function waitForElementQuery(
  query: () => Element | null,
  timeout = 10000,
  refresh = 500,
) {
  const start = Date.now();

  while (Date.now() - start < timeout) {
    try {
      const el = query();
      if (el) {
        return el;
      }
    } catch (e) {
      console.error(e);
    }

    await new Promise((resolve) => setTimeout(resolve, refresh));
  }

  return new Promise(() => {});
}

export const waitForClick = (el: Element | Promise<Element>): Promise<void> =>
  new Promise(async (resolve) => {
    (await el).addEventListener(
      "click",
      () => {
        resolve(undefined);
      },
      { once: true },
    );
  });
