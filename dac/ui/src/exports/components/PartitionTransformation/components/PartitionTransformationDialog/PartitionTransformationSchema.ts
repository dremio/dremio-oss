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

import { z } from "zod";

export const PartitionTransformationSchema = z.object({
  numberInput: z
    .number({
      invalid_type_error: "Input must be an integer",
    })
    .gte(1, "Input must be between an integer between 1 and 2147483647")
    .lte(
      2147483647,
      "Input must be between an integer between 1 and 2147483647",
    )
    .int(),
});
