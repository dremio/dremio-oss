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

export const validationSchema = z.object({
  schedule: z
    .object({
      scheduleType: z.string(),
      weekInput: z.string().array(),
      dayOfMonth: z.number().positive().gte(1).lte(30),
      hourInput: z.number().positive().gte(1).lte(23),
    })
    .partial()
    .superRefine((data, ctx) => {
      if (
        data.scheduleType === "@week" &&
        data.weekInput &&
        data.weekInput.length < 1
      ) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["weekInput"],
          message: "Must pick a minimum of one day",
        });
      }
    }),
  advancedConfig: z.object({
    maxFileSize: z
      .number()
      .positive()
      .gte(1, { message: "max file size must be over 0" }),
    minFileSize: z
      .number()
      .positive()
      .gte(1, { message: "min file size must be over 0" }),
    minFiles: z
      .number()
      .positive()
      .gte(1, { message: "file size must be over 0" }),
    targetFileSize: z
      .number()
      .positive()
      .gte(1, { message: "target file size must be over 0" }),
  }),
});
