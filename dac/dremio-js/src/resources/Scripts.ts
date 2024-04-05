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

import type { SonarResourceConfig } from "../internal/Config.js";
import { StoredScript } from "../classes/StoredScript.js";
import { Script } from "../classes/Script.js";
import { Result, Ok, Err } from "ts-results-es";
import { HttpError } from "../classes/HttpError.js";
import type { ValidationProblem } from "../classes/Problem.js";
import { ApiErrorV1 } from "../classes/ApiError.js";
import { type Collection } from "../types/Collection.js";
import { type SharedErrors } from "../internal/SharedErrors.js";
import type { LimitParams } from "../types/Params.js";

const duplicateScriptNameError = {
	code: "validation:field_conflict",
	title: "A script with this name already exists.",
	source: { pointer: "/name" },
} as const satisfies ValidationProblem;

export const Scripts = (config: SonarResourceConfig) => ({
	list(
		params: Partial<LimitParams> = {},
	): Collection<StoredScript, SharedErrors> {
		const search = new URLSearchParams({
			...(params.limit && { maxResults: String(params.limit) }),
		});

		let firstPage: any;

		const getFirstPage = () => {
			if (firstPage) {
				return firstPage;
			}
			firstPage = config
				.sonarV2Request(`scripts?${search.toString()}`)
				.then((res) => res.json());
			return firstPage;
		};

		return {
			async total_count() {
				return getFirstPage().then((result: any) => Ok(result.total));
			},
			async *data() {
				const data = (await getFirstPage()).data.map((script: any) =>
					Ok(StoredScript.fromResource(config, script)),
				) as Ok<StoredScript>[];
				yield* data;
			},
		};
	},

	store(
		script: Script,
	): Promise<
		Result<StoredScript, typeof duplicateScriptNameError | SharedErrors>
	> {
		return config
			.sonarV2Request(`scripts`, {
				method: "POST",
				body: JSON.stringify({
					name: script.name,
					description: script.description,
					content: script.query.sql,
					context: script.query.context,
				}),
				headers: { "Content-Type": "application/json" },
			})
			.then((res) => res.json())
			.then((scriptResource) =>
				Ok(StoredScript.fromResource(config, scriptResource)),
			)
			.catch((e) => {
				if (e instanceof HttpError && e.body instanceof ApiErrorV1) {
					switch (e.body.errorMessage) {
						case "Cannot reuse the same script name within a project. Please try another script name.":
							return Err(duplicateScriptNameError);
					}
				}
				return Err(e);
			});
	},
});
