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

import {
  DEFAULT_ERROR_MESSAGE,
  extractSqlErrorFromResponse,
} from "./errorUtils";

describe("errorUtils", () => {
  describe("extractSqlErrorFromResponse", () => {
    const globalRange = {
      startLine: 12,
      endLine: 3,
      startColumn: 7,
      endColumn: 90,
    };

    const generalError = "general error";
    const specificError = "specific error";

    const defaultSqlError = {
      message: specificError,
      range: {
        startLine: 24,
        endLine: 37,
        startColumn: 19,
        endColumn: 20,
      },
    };

    const responseWithNoDetails = {
      errorMessage: generalError,
    };

    const fullResponse = {
      errorMessage: generalError,
      details: {
        errors: [defaultSqlError],
      },
    };

    it("should return original range", () => {
      expect(
        extractSqlErrorFromResponse(undefined, globalRange).range
      ).to.deep.equal(globalRange);
      expect(
        extractSqlErrorFromResponse(responseWithNoDetails, globalRange).range
      ).to.deep.equal(globalRange);
      expect(
        extractSqlErrorFromResponse(fullResponse, globalRange).range
      ).to.deep.equal(globalRange);
    });

    it("should return generic error if response is empty", () => {
      expect(
        extractSqlErrorFromResponse(undefined, globalRange).message
      ).to.eql(DEFAULT_ERROR_MESSAGE);
      expect(extractSqlErrorFromResponse({}, globalRange).message).to.eql(
        DEFAULT_ERROR_MESSAGE
      );
    });

    it("should return global error if there are no details", () => {
      expect(
        extractSqlErrorFromResponse(responseWithNoDetails, globalRange).message
      ).to.eql(generalError);

      const responseWithEmptyDetails = {
        ...responseWithNoDetails,
        details: {},
      };

      expect(
        extractSqlErrorFromResponse(responseWithEmptyDetails, globalRange)
          .message
      ).to.eql(generalError);
    });

    it("should return global error if the details have no errors", () => {
      const responseWithNoErrors = {
        ...responseWithNoDetails,
        details: {
          errors: [],
        },
      };

      expect(
        extractSqlErrorFromResponse(responseWithNoErrors, globalRange).message
      ).to.eql(generalError);
    });

    it("should return specific error if response has details", () => {
      expect(
        extractSqlErrorFromResponse(fullResponse, globalRange).message
      ).to.eql(specificError);
    });

    it("should return specific error from the first error in details if there are several", () => {
      const responseWithMultipleErrors = {
        ...fullResponse,
        details: {
          errors: [
            defaultSqlError,
            {
              ...defaultSqlError,
              message: "Other Random Message",
            },
          ],
        },
      };

      expect(
        extractSqlErrorFromResponse(responseWithMultipleErrors, globalRange)
          .message
      ).to.eql(specificError);
    });
  });
});
