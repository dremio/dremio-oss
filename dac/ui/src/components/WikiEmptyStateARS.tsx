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

import { FormattedMessage } from "react-intl";

export const WikiEmptyStateARS = () => {
  return (
    <div className="my-1 dremio-prose">
      <dremio-icon
        name="interface/edit-ars-wiki"
        style={{
          width: "100%",
          height: "200px",
        }}
      />
      <h3 className="dremio-typography-bold">
        <FormattedMessage id="Wiki.ARS.Welcome" />
      </h3>
      <p>
        <FormattedMessage id="Wiki.ARS.Line1" />
      </p>
      <p>
        <FormattedMessage id="Wiki.ARS.Line2" />
      </p>
      <p>
        <FormattedMessage
          id="Wiki.ARS.Line3"
          values={{
            a: (chunk: string) => (
              <a
                href="https://docs.dremio.com/cloud/get-started/"
                target="_blank"
                rel="noopener noreferrer"
              >
                {chunk}
              </a>
            ),
          }}
        />
      </p>
      <p
        className="dremio-typography-small dremio-typography-less-important"
        style={{ fontStyle: "italic" }}
      >
        <FormattedMessage id="Wiki.ARS.Line4" />
      </p>
    </div>
  );
};
