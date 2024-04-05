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

// import type { components } from "../../../apiTypes/scripts";
import { SonarResourceConfig } from "../internal/Config.js";
import { Query } from "./Query.js";
import { Script } from "./Script.js";
// type ScriptProperties = components["schemas"]["Script"];
type ScriptProperties = any;

/**
 * @internal
 */
export class StoredScript implements Script {
	description: Script["description"];
	id: string;
	name: Script["name"];
	query: Query;
	#sonarResourceConfig: SonarResourceConfig;

	constructor(
		sonarResourceConfig: SonarResourceConfig,
		query: Query,
		properties: ScriptProperties,
	) {
		this.#sonarResourceConfig = sonarResourceConfig;
		this.description = properties.description;
		this.id = properties.id;
		this.name = properties.name;
		this.query = query;
	}

	save() {
		return this.#sonarResourceConfig.sonarV2Request(
			`scripts/${this.id}` as const,
			{
				method: "put",
				headers: { "Content-Type": "application/json" },
				body: this.toJSON(),
			},
		);
	}

	toJSON() {
		return JSON.stringify({
			description: this.description,
			id: this.id,
			name: this.name,
			content: this.query.sql,
			context: this.query.context,
		});
	}

	static fromResource(
		sonarResourceConfig: SonarResourceConfig,
		properties: ScriptProperties,
	) {
		return new StoredScript(
			sonarResourceConfig,
			{ context: properties.context, sql: properties.content },
			properties,
		);
	}
}
