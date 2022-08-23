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
export type ErrorResponse = {
    readonly errorMessage?: string;
    readonly details?: {
        readonly errors?: SqlError[];
    }
}

export type QueryRange = {
    readonly startLine: number;
    readonly startColumn: number;
    readonly endLine: number;
    readonly endColumn: number;
}

export type SqlError = {
    readonly message: string;
    readonly range: QueryRange;
}

export const DEFAULT_ERROR_MESSAGE = "Error";

/**
 * Error message will be in the only error within the error details. 
 * NOTE: if this behavior changes on the server, this is the place to update.
 */
const getSqlError = (errorResponse: ErrorResponse | undefined): SqlError | undefined => {
    return errorResponse?.details?.errors?.[0];
}

/**
 * 
 * @param errorResponse response from the server which contains either details error or a generic one
 * @param queryRange range within sql editor of where the error happened
 * @returns error message along with a (potentially) narrowed down range of where the error happened within the editor.
 */
export const extractSqlErrorFromResponse = (errorResponse: ErrorResponse | undefined, queryRange: QueryRange): SqlError => {
    const sqlError = getSqlError(errorResponse);
    if (!sqlError) {
        return {
            message: errorResponse?.errorMessage ?? DEFAULT_ERROR_MESSAGE,
            range: queryRange,
        };
    }
    // for now, we return absolute range always because there are issues with how it is computed 
    // and we can't rely on the range in response as the result.
    // Specifically, when we chop multi-sql statements, we trim new line/whitespaces in the expression that is sent to the server.
    // So when the result is back, we can't easily reconsile line/column numbers.
    return {
        message: sqlError.message,
        range: queryRange,
    };
}