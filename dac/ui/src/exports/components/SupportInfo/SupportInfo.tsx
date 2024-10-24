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

import { HttpError } from "../../errors/HttpError";
import { CopyButton } from "dremio-ui-lib/components";
import sentryUtil from "#oss/utils/sentryUtil";

export const HttpErrorSupportInfo = (props: { error: HttpError }) => {
  const { error, ...rest } = props;
  const requestId =
    error.res.headers.get("x-request-id") || sentryUtil.getEventId();
  return (
    <p {...rest} style={{ userSelect: "all" }}>
      <code className="dremio-typography-monospace">
        Request ID: {requestId}
      </code>
      <CopyButton contents={`Request ID: ${requestId}`} />
    </p>
  );
};
