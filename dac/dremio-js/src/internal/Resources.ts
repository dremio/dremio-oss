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

import { SonarResourceConfig } from "./Config.js";
import { Jobs } from "../resources/Jobs.js";
import { Scripts } from "../resources/Scripts.js";

/**
 * @internal
 * @hidden
 */
export const Resources = (config: SonarResourceConfig) => ({
	jobs: Jobs(config),
	scripts: Scripts(config),
});
