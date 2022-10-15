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

import * as PATHS from "./paths";

const catalogId = "1234";
const sourceId = "arctic-source-test";
const branch = "main";
const namespace = "folder1/folder2";
const commit = "a1s2d3f4";

describe("arctic catalog routes", () => {
  it("returns the base arctic catalog url", () => {
    const result = PATHS.arcticCatalogBase({ arcticCatalogId: catalogId });
    expect(result).to.equal(`/arctic/${catalogId}`);
  });

  it("returns the arctic catalog url using base", () => {
    const result = PATHS.arcticCatalog({ arcticCatalogId: catalogId });
    expect(result).to.equal(`/arctic/${catalogId}`);
  });

  it("returns the arctic catalog url for data tab", () => {
    const result = PATHS.arcticCatalogData({
      arcticCatalogId: catalogId,
      branchId: branch,
    });
    expect(result).to.equal(`/arctic/${catalogId}/data/${branch}`);
  });

  it("returns the arctic catalog url for data tab, using current namespace", () => {
    const result = PATHS.arcticCatalogData({
      arcticCatalogId: catalogId,
      branchId: branch,
      namespace: namespace,
    });
    expect(result).to.equal(`/arctic/${catalogId}/data/${branch}/${namespace}`);
  });

  it("returns the arctic catalog url for commits tab", () => {
    const result = PATHS.arcticCatalogCommits({
      arcticCatalogId: catalogId,
      branchId: branch,
    });
    expect(result).to.equal(`/arctic/${catalogId}/commits/${branch}`);
  });

  it("returns the arctic catalog url for commits tab, using current namespace", () => {
    const result = PATHS.arcticCatalogCommits({
      arcticCatalogId: catalogId,
      branchId: branch,
      namespace: namespace,
    });
    expect(result).to.equal(
      `/arctic/${catalogId}/commits/${branch}/${namespace}`
    );
  });

  it("returns the arctic catalog url for commit details", () => {
    const result = PATHS.arcticCatalogCommit({
      arcticCatalogId: catalogId,
      branchId: branch,
      commitId: commit,
    });
    expect(result).to.equal(`/arctic/${catalogId}/commit/${branch}/${commit}`);
  });

  it("returns the arctic catalog url for tags tab", () => {
    const result = PATHS.arcticCatalogTags({ arcticCatalogId: catalogId });
    expect(result).to.equal(`/arctic/${catalogId}/tags`);
  });

  it("returns the arctic catalog url for branches tab", () => {
    const result = PATHS.arcticCatalogBranches({ arcticCatalogId: catalogId });
    expect(result).to.equal(`/arctic/${catalogId}/branches`);
  });

  it("returns the arctic catalog url for settings", () => {
    const result = PATHS.arcticCatalogSettings({ arcticCatalogId: catalogId });
    expect(result).to.equal(`/arctic/${catalogId}/settings`);
  });
});

describe("arctic source routes", () => {
  it("returns the base url for an arctic source", () => {
    const result = PATHS.arcticSourceBase({ sourceId: sourceId });
    expect(result).to.equal(`/sources/arctic/${sourceId}`);
  });

  it("returns the arctic source url for data page", () => {
    const result = PATHS.arcticSourceCommitsBase({ sourceId: sourceId });
    expect(result).to.equal(`/sources/arctic/${sourceId}/commits`);
  });

  it("returns the arctic source url for data page", () => {
    const result = PATHS.arcticSourceCommitsBase({ sourceId: sourceId });
    expect(result).to.equal(`/sources/arctic/${sourceId}/commits`);
  });

  it("returns the arctic source url for commit (without base)", () => {
    const result = PATHS.arcticSourceCommitsNonBase();
    expect(result).to.equal(`commits`);
  });

  it("returns the arctic source url for commits (without base)", () => {
    const result = PATHS.arcticSourceCommits({ branchId: branch });
    expect(result).to.equal(`commits/${branch}`);
  });

  it("returns the arctic source url for commits (without base), using namespace", () => {
    const result = PATHS.arcticSourceCommits({
      branchId: branch,
      namespace: namespace,
    });
    expect(result).to.equal(`commits/${branch}/${namespace}`);
  });

  it("returns the arctic source url for commit details (without base)", () => {
    const result = PATHS.arcticSourceCommit({
      branchId: branch,
      commitId: commit,
    });
    expect(result).to.equal(`commit/${branch}/${commit}`);
  });

  it("returns the arctic catalog url for tags tab", () => {
    const result = PATHS.arcticSourceTagsNonBase();
    expect(result).to.equal("tags");
  });

  it("returns the arctic catalog url for branches tab", () => {
    const result = PATHS.arcticSourceBranchesNonBase();
    expect(result).to.equal(`branches`);
  });
});
