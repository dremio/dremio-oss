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

import { load } from "js-yaml";
import path from "path";
import glob from "glob";
import { parse } from "@formatjs/icu-messageformat-parser";
import fs from "fs";

const toFileMetadata = (filePath: string) => {
  const absolutePath = path.resolve(filePath);
  const content = load(fs.readFileSync(absolutePath, "utf8")) as any;
  for (const key in content) {
    content[key] = parse(content[key]);
  }
  return {
    path: absolutePath,
    outputPath: path.join(
      process.cwd(),
      filePath
        .replace("./lang", "./dist-lang")
        .replace(".yaml", ".json")
        .replace("/lang", "")
    ),
    content,
  };
};

const langFiles = glob.sync("./lang/**/*.yaml");
const writeOutput = (fileMetadata: any) => {
  const { dir } = path.parse(fileMetadata.outputPath);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(
    fileMetadata.outputPath,
    JSON.stringify(fileMetadata.content)
  );
};

langFiles.map(toFileMetadata).forEach(writeOutput);
