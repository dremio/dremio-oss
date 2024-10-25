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
import { Schema, arrayOf } from "normalizr";
import { ENTITY_TYPES } from "#oss/constants/Constants";

import dataset from "./dataset";
import file from "./file";
import folder from "./folder";
import physicalDataset from "./physicalDataset";

const space = new Schema(ENTITY_TYPES.space);

space.define({
  contents: {
    datasets: arrayOf(dataset),
    files: arrayOf(file),
    folders: arrayOf(folder),
    physicalDatasets: arrayOf(physicalDataset),
  },
});

export default space;
