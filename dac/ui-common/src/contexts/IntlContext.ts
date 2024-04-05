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

import IntlMessageFormat from "intl-messageformat";
import { isDev } from "../utilities/isDev";

const messages = new Map<string, string>();

export type IntlContext = {
  addMessages: (messages: Record<string, string>) => void;
  t: (messageKey: string, params?: Record<string, any>) => string;
};

const intlContext: IntlContext = {
  addMessages: (additionalMessages) => {
    for (const key in additionalMessages) {
      messages.set(key, additionalMessages[key]);
    }
  },
  t: (messageKey, params) => {
    const message = messages.get(messageKey);

    if (!message) {
      if (isDev) {
        console.error(`Could not find translation key: ${messageKey}`);
      }
      return messageKey;
    }

    return new IntlMessageFormat(message, window.navigator.language).format(
      params,
    );
  },
};

export const getIntlContext = () => intlContext;
