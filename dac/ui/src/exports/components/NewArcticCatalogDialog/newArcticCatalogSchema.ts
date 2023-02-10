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

import { isAlphaNumeric } from "@app/exports/utilities/isAlphaNumeric";
import { z } from "zod";

export const newArcticCatalogSchema = z.object({
  name: z
    .string()
    .trim()
    .max(255, "Catalog name must not exceed 255 characters")
    .min(1, "Catalog name is required")
    .refine(
      isAlphaNumeric,
      "Catalog name must contain only 0-9, A-Z, a-z, _ or -."
    ),
});
