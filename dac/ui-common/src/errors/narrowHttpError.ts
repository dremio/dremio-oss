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

import { HttpError } from "./HttpError";
import {
  GenericServerError,
  GenericServerErrorResponseBody,
} from "./GenericServerError";
import { BadRequestError } from "./BadRequestError";
import { UnauthorizedError } from "./UnauthorizedError";
import { ServiceUnavailableError } from "./ServiceUnavailableError";
import { BadGatewayError } from "./BadGatewayError";
import { GatewayTimeoutError } from "./GatewayTimeoutError";
import { ConflictError } from "./ConflictError";
import { NotFoundError } from "./NotFoundError";
import { ForbiddenError } from "./ForbiddenError";

const extractResponseBody = (res: Response) => {
  if (res.headers.get("content-type")?.includes("application/json")) {
    return res.json();
  }

  return res.text();
};

export const narrowHttpError = async (error: HttpError) => {
  const responseBody = await extractResponseBody(error.res);
  switch (error.res.status) {
    case 400:
      return new BadRequestError(
        error.res,
        responseBody as GenericServerErrorResponseBody,
      );
    case 401:
      return new UnauthorizedError(error.res, responseBody);
    case 403:
      return new ForbiddenError(error.res, responseBody);
    case 404:
      return new NotFoundError(error.res, responseBody);
    case 409:
      return new ConflictError(error.res, responseBody);
    case 502:
      return new BadGatewayError(error.res, responseBody);
    case 503:
      return new ServiceUnavailableError(error.res, responseBody);
    case 504:
      return new GatewayTimeoutError(error.res, responseBody);
    default:
      return new GenericServerError(
        error.res,
        responseBody as GenericServerErrorResponseBody,
      );
  }
};
