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
import apiUtils from "@app/utils/apiUtils/apiUtils";
import { handleUnauthorized } from "@app/store/authMiddleware";
import { store } from "@app/store/store";

const NESSIE_V1_PREFIX = "/nessieV1/api/v1/";
const NESSIE_V2_PREFIX = "/nessie/api/v2/";

function getUrl(initialUrl, urlPath) {
  let url = initialUrl;
  if (!url.endsWith("/")) url += "/";
  url += urlPath;
  return url;
}

const getMiddleWare = (externalUrl = "") => {
  return [
    {
      pre(context) {
        const [, urlPath] =
          context.url.indexOf(NESSIE_V2_PREFIX) !== -1
            ? context.url.split(NESSIE_V2_PREFIX)
            : context.url.split(NESSIE_V1_PREFIX);
        const url = getUrl(
          externalUrl ||
            apiUtils.getAPIVersion("NESSIE", { nessieVersion: "v1" }),
          urlPath
        );

        return Promise.resolve({
          url,
          init: {
            ...context.init,
            headers: {
              ...context.init.headers,
              //Always send credentials, even to external URLs
              ...apiUtils.prepareHeaders(),
            },
          },
        });
      },
      //Only handle unauthorized for internal DDP only nessie
      ...(!externalUrl && {
        post({ response }) {
          const resultAction = handleUnauthorized(response, store.dispatch);
          if (resultAction) {
            window.location.replace("/"); //Clear history since project context is cleared on login
          }
          return Promise.resolve(response);
        },
      }),
    },
  ];
};

export default getMiddleWare;
