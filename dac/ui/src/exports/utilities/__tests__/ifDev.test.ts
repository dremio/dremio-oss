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

import { ifDev } from "../ifDev";

describe("ifDev", () => {
  const originalEnv = process.env;

  const setEnv = (NODE_ENV: "production" | "development"): void => {
    process.env = {
      ...originalEnv,
      NODE_ENV,
    };
  };

  it("returns the passed item when develop mode is true", () => {
    const obj = {};
    setEnv("development");
    expect(ifDev(obj)).toBe(obj);
  });

  it("returns the undefined when develop mode is false", () => {
    const obj = {};
    setEnv("production");
    expect(ifDev(obj)).toBe(undefined);
  });
});
