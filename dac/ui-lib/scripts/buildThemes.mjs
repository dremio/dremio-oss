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
/* eslint-disable */
import fs from "fs";
import fsExtra from "fs-extra";
import glob from "glob";
import path from "path";
import sass from "sass";

const OUT_DIR = "./dist-themes";

/**
 * Rewrite the absolute filesystem path to a relative path for each entry in `sourceMap.sources`
 */
const rewriteSources = (sourceMap) => {
  sourceMap.sources = sourceMap.sources.map((source) => {
    return source.substring(source.indexOf(`${path.sep}themes${path.sep}`));
  });
  return sourceMap;
};

/**
 * Run the Sass compiler on each file matched by a `glob()` and write it to the `OUT_DIR`
 */
const buildFiles = (err, files) => {
  if (err) {
    throw err;
  }

  // Discover the set of output folders that need to be created under `OUT_DIR`
  const outputFolders = files.reduce((accum, file) => {
    const [_themesRoot, theme, ...paths] = file.split(path.sep);
    accum.add(path.join(OUT_DIR, theme, ...paths, "../"));
    return accum;
  }, new Set());

  // Create each output folder if it doesn't already exist
  outputFolders.forEach((file) => {
    fs.mkdirSync(file, { recursive: true });
  });

  // Run the Sass build on each file
  files.forEach((file) => {
    const [_themesRoot, theme, ...paths] = file.split(path.sep);

    const outFilePath = path
      .join(OUT_DIR, theme, ...paths)
      .replace("scss", "css");

    const result = sass.compile(file, {
      sourceMap: true,
      sourceMapIncludeSources: true,
    });

    const sourceMapFilepath = outFilePath + ".map";
    const sourceMapFilename = sourceMapFilepath.split(path.sep).pop();

    fs.writeFileSync(
      outFilePath,
      result.css + `\n/*# sourceMappingURL=${sourceMapFilename} */`
    );

    fs.writeFileSync(
      sourceMapFilepath,
      JSON.stringify(rewriteSources(result.sourceMap))
    );
  });
};

export const copyAssets = (err, files) => {
  files.forEach((file) => {
    const [_themesRoot, theme, ...paths] = file.split(path.sep);
    const outFilePath = path.join(OUT_DIR, theme, ...paths);
    fsExtra.copySync(file, outFilePath);
  });
};

// Bundle all tokens and component styles into one file
glob("themes/*/index.scss", buildFiles);

// Bundle only the component styles into one file
glob("themes/*/components.scss", buildFiles);

// Output styles for each component individually
glob("themes/*/components/*.scss", buildFiles);

// Bundle only the tokens into one file
glob("themes/*/tokens.scss", buildFiles);

glob("themes/*/assets", copyAssets);
