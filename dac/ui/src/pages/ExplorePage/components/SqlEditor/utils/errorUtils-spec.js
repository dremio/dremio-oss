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
      endLine: 16,
      startColumn: 7,
      endColumn: 90,
    };

    const globalQueryRange = {
      startLineNumber: globalRange.startLine,
      startColumn: globalRange.startColumn,
      endLineNumber: globalRange.endLine,
      endColumn: globalRange.endColumn,
    };

    const generalError = "general error";
    const specificError = "specific error";

    const defaultSqlError = {
      message: specificError,
      range: {
        startLine: 2,
        endLine: 3,
        startColumn: 2,
        endColumn: 10,
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

    it("should return original range if response doesn't have error details", () => {
      expect(
        extractSqlErrorFromResponse(undefined, globalQueryRange).range,
      ).to.deep.equal(globalQueryRange);
      expect(
        extractSqlErrorFromResponse(responseWithNoDetails, globalQueryRange)
          .range,
      ).to.deep.equal(globalQueryRange);
    });

    it("should return original range if response doesn't have error range", () => {
      expect(
        extractSqlErrorFromResponse(
          {
            errorMessage: generalError,
            details: {
              errors: [
                {
                  errorMessage: specificError,
                },
              ],
            },
          },
          globalQueryRange,
        ).range,
      ).to.deep.equal(globalQueryRange);
      expect(
        extractSqlErrorFromResponse(responseWithNoDetails, globalQueryRange)
          .range,
      ).to.deep.equal(globalQueryRange);
    });

    it("should return end column offset by global range plus one for exclusive index when error is on the first line", () => {
      const firstLineError = {
        ...fullResponse,
        details: {
          errors: [
            {
              message: specificError,
              range: {
                startLine: 1,
                endLine: 1,
                startColumn: 2,
                endColumn: 10,
              },
            },
          ],
        },
      };
      expect(
        extractSqlErrorFromResponse(firstLineError, globalQueryRange).range,
      ).to.deep.equal({
        startLineNumber: 12, // original start & end lines
        endLineNumber: 12,
        startColumn: 8, // 7 original and 2 in the error
        endColumn: 17, // 10th column in the error plus we are making it exclusive so 7 (global) + 9 (relative) + 1 (exclusive) = 17
      });
    });

    it("should return start and end column same as in relative error plus one for exclusive index when error on non-start line for the gloabl range", () => {
      expect(
        extractSqlErrorFromResponse(fullResponse, globalQueryRange).range,
      ).to.deep.equal({
        startLineNumber: 13, // 12 original and 2 in the error
        startColumn: 2, // 2 in the error since we don't start on the original line
        endLineNumber: 14, // 3rd line in the error
        endColumn: 11, // 10th column in the error plus we are making it exclusive so 10 (relative) + 1 (exclusive) = 11
      });
    });

    it("should return generic error if response is empty", () => {
      expect(
        extractSqlErrorFromResponse(undefined, globalQueryRange).message,
      ).to.eql(DEFAULT_ERROR_MESSAGE);
      expect(extractSqlErrorFromResponse({}, globalQueryRange).message).to.eql(
        DEFAULT_ERROR_MESSAGE,
      );
    });

    it("should return global error if there are no details", () => {
      expect(
        extractSqlErrorFromResponse(responseWithNoDetails, globalQueryRange)
          .message,
      ).to.eql(generalError);

      const responseWithEmptyDetails = {
        ...responseWithNoDetails,
        details: {},
      };

      expect(
        extractSqlErrorFromResponse(responseWithEmptyDetails, globalQueryRange)
          .message,
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
        extractSqlErrorFromResponse(responseWithNoErrors, globalQueryRange)
          .message,
      ).to.eql(generalError);
    });

    it("should return specific error if response has details", () => {
      expect(
        extractSqlErrorFromResponse(fullResponse, globalQueryRange).message,
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
        extractSqlErrorFromResponse(
          responseWithMultipleErrors,
          globalQueryRange,
        ).message,
      ).to.eql(specificError);
    });
  });
});
