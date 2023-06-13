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

type License = string;

type Package = {
  name: string;
  version: string;
  path: string;
  license: string;
  author: string;
  homepage: string;
};

type LicenseJson = Record<License, Package[]>;

const compatibleLicenses = new Set<string>([
  "Apache-2.0",
  "MIT",
  "ISC",
  "BSD-3-Clause",
  "BSD-2-Clause",
  "CC-BY-4.0",
  "Unlicense",
  "Unknown",
  "CC0-1.0",
  "(MIT OR GPL-2.0)",
  "0BSD",
  "(GPL-2.0 OR MIT)",
]);

function readLicenseJson(): Promise<string> {
  return new Promise((resolve, reject) => {
    let data = "";
    process.stdin.setEncoding("utf8");
    process.stdin.on("data", (chunk) => {
      data += chunk;
    });
    process.stdin.on("end", () => {
      resolve(data);
    });
    process.stdin.on("error", reject);
  });
}

const getPackageName = (pkg: Package): string => pkg.name;

function hasIncompatibleLicense(licenseJson: LicenseJson) {
  return Object.keys(licenseJson).some((license) => {
    const isCompatible = compatibleLicenses.has(license);

    if (!isCompatible) {
      console.log(`OSS license check: license "${license}" is not approved.`);
      console.log(
        `OSS license check: "${license}" license is used by: \n ${licenseJson[
          license
        ]
          .map(getPackageName)
          .join("\n")}`
      );
    }

    return !isCompatible;
  });
}

async function run() {
  const licenseJson: LicenseJson = JSON.parse(await readLicenseJson());
  if (hasIncompatibleLicense(licenseJson)) {
    process.exit(1);
  }
}

run();
