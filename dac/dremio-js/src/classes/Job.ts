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

import { SonarResourceConfig } from "../internal/Config.js";
import { Observable, share } from "rxjs";
import type { Query } from "./Query.js";
import { Err, Ok, Result } from "ts-results-es";
import { SharedErrors } from "../internal/SharedErrors.js";

/**
 * @hidden
 */
export type JobId = string;

/**
 * @internal
 */
export class Job {
	#sonarResourceConfig: SonarResourceConfig;
	#details: Record<string, any>;
	id: JobId;

	constructor(
		sonarResourceConfig: SonarResourceConfig,
		details: Record<string, any>,
	) {
		this.#sonarResourceConfig = sonarResourceConfig;
		this.#details = details;
		this.id = details.id;
	}

	async #refresh(): Promise<boolean> {
		const job = await this.#sonarResourceConfig
			.sonarV2Request(`jobs-listing/v1.0/${this.id}/jobDetails`)
			.then((res) => res.json());

		let changed = false;
		if (job.jobStatus !== this.#details.jobStatus) {
			changed = true;
			this.#details = job;
		}
		return changed;
	}

	#updates: Observable<any> | null = null;

	get updates() {
		if (!this.#updates) {
			this.#updates = new Observable((subscriber) => {
				if (this.#details.isComplete) {
					subscriber.complete();
					return;
				}
				const update = async () => {
					const changed = await this.#refresh();

					if (changed) {
						subscriber.next();
					}

					if (this.#details.isComplete) {
						subscriber.complete();
					} else {
						setTimeout(() => {
							update();
						}, 2000);
					}
				};
				update();
			}).pipe(share());
		}
		return this.#updates;
	}

	get details() {
		return this.#details;
	}

	static async fromQuery(
		sonarResourceConfig: SonarResourceConfig,
		query: Query,
	): Promise<Result<Job, SharedErrors>> {
		return sonarResourceConfig
			.sonarV2Request(
				`datasets/new_tmp_untitled_sql_and_run?newVersion=000${Math.floor(
					Math.random() * 9999999999999,
				)}`,
				{
					method: "POST",
					headers: { "Content-Type": "application/json" },
					body: JSON.stringify(query),
				},
			)
			.then((res) => res.json())
			.then((job) => Job.fromId(sonarResourceConfig, job.jobId.id))
			.catch((e) => Err(e));
	}

	static async fromId(
		sonarResourceConfig: SonarResourceConfig,
		id: JobId,
	): Promise<Result<Job, SharedErrors>> {
		return sonarResourceConfig
			.sonarV2Request(`jobs-listing/v1.0/${id}/jobDetails`)
			.then((res) => res.json())
			.then((job) => Ok(new Job(sonarResourceConfig, job)))
			.catch((e) => Err(e));
	}
}
