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

// need DS types listed here for `loadDatasetForDatasetType` action
// see DX-5388 Clicking on settings button in explore of a physical datasets doesn't work as expected
// kill with DX-5161 Use classes (reduce bugs, develop faster)

import physicalDataset from "./physicalDataset";
import dataset from "./dataset";
import folder from "./folder";
import file from "./file";

import home from "./home";
import source from "./source";
import space from "./space";

export { physicalDataset, dataset, folder, file, home, source, space };
