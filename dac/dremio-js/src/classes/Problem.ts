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
 * @hidden
 */
export type AdditionalDetails = {
	readonly subject: string;
	readonly contents: string;
};

/**
 * @hidden
 */
export type ProblemLinks = {
	/**
	 * URI pointing to documentation relevant to the error code
	 */
	readonly type?: string;
};

/**
 * @hidden
 */
export type ProblemMeta = {
	readonly additionalDetails?: AdditionalDetails;
};

export interface Problem {
	/**
	 * A short, uniquely identifying error code
	 */
	readonly code: string;

	/**
	 * A longer description of the error intended for human consumption
	 */
	readonly title: string;

	readonly links?: ProblemLinks;
	readonly meta?: ProblemMeta;
}

export interface ValidationProblem extends Problem {
	/**
	 * A short, uniquely identifying error code
	 */
	readonly code: "validation:field_conflict" | "validation:field_not_present";

	/**
	 * A longer description of the error intended for human consumption
	 */
	readonly title: string;

	readonly source: {
		/**
		 * A JSON pointer to the affected field
		 */
		readonly pointer: string;
	};

	readonly links?: ProblemLinks;
	readonly meta?: ProblemMeta;
}
