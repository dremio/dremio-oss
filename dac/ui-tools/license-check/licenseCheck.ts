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

const compatibleLicenses = new Set([
  "0BSD",
  "Apache-2.0",
  "BSD-2-Clause",
  "BSD-3-Clause",
  "CC0-1.0",
  "ISC",
  "MIT",
  "Unlicense",
  "(GPL-2.0 OR MIT)",
  "(MIT OR GPL-2.0)",
]);

const ignoredDependencies = new Set(["caniuse-lite", "options"]);

const readLicenseJson = (): Promise<string> =>
  new Promise((resolve, reject) => {
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

const getPackageName = (pkg: Package): string => pkg.name;

const onlyIncompatibleLicenses = (licenseJson: LicenseJson): LicenseJson =>
  Object.keys(licenseJson).reduce((acc, license) => {
    if (!compatibleLicenses.has(license) && licenseJson[license].length) {
      acc[license] = licenseJson[license];
    }
    return acc;
  }, {} as LicenseJson);

const filterIgnoredPackages = (licenseJson: LicenseJson): LicenseJson => {
  const next = { ...licenseJson };
  Object.keys(next).forEach((license) => {
    next[license] = next[license].filter(
      (pkg) => !ignoredDependencies.has(pkg.name)
    );
  });
  return next;
};

const printIncompatibleLicenses = (incompatibleLicenses: LicenseJson): void => {
  Object.keys(incompatibleLicenses).forEach((license) => {
    console.log(
      `Error: OSS license check: license "${license}" is not approved. ${license} is used by:`
    );
    incompatibleLicenses[license].forEach((pkg) => {
      console.log("  ", pkg.name);
    });
  });
};

async function run() {
  const licenseJson: LicenseJson = JSON.parse(await readLicenseJson());

  const incompatibleLicenses = onlyIncompatibleLicenses(
    filterIgnoredPackages(licenseJson)
  );

  if (Object.keys(incompatibleLicenses).length) {
    printIncompatibleLicenses(incompatibleLicenses);
    process.exit(1);
  }
}

run();
