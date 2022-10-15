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

import {
  ArcticUrlProps,
  constructArcticUrl,
  convertCatalogToSourceSpace,
} from "./arctic-catalog-utils";
import * as PATHS from "../../paths";
// @ts-ignore
import sinon from "sinon";

describe("Arcitc Catalogs - Constructing URLs for navigating across tabs", () => {
  const params: ArcticUrlProps = {
    type: "catalog",
    baseUrl: "/arctic/1234",
    tab: "data",
    namespace: "",
    hash: "",
    commitId: "",
  };

  const arcticCatalogDataBaseSpy = sinon.spy(PATHS, "arcticCatalogDataBase");
  const arcticCatalogDataSpy = sinon.spy(PATHS, "arcticCatalogData");
  const arcticCatalogCommitsBaseSpy = sinon.spy(
    PATHS,
    "arcticCatalogCommitsBase"
  );
  const arcticCatalogCommitsSpy = sinon.spy(PATHS, "arcticCatalogCommits");
  const arcticCatalogCommitSpy = sinon.spy(PATHS, "arcticCatalogCommit");
  const arcticCatalogBranchesSpy = sinon.spy(PATHS, "arcticCatalogBranches");
  const arcticCatalogTagsSpy = sinon.spy(PATHS, "arcticCatalogTags");

  it("returns the default concatenated url", () => {
    const defaultParams: ArcticUrlProps = {
      type: "catalog",
      baseUrl: "",
      tab: "data",
      namespace: "main/folder1/folder2",
      hash: "?hash=a1s2d3f4",
    };
    const result = constructArcticUrl(defaultParams);
    expect(result).to.equal(
      `${defaultParams.baseUrl}/${defaultParams.tab}/${defaultParams.namespace}${defaultParams.hash}`
    );
  });

  it("returns the arctic catalog url for 'data' without branch name", () => {
    const result = constructArcticUrl({
      ...params,
      namespace: "",
    });
    expect(result).to.equal(`${params.baseUrl}/data`);
    expect(arcticCatalogDataBaseSpy).to.have.been.called;
  });

  it("returns the arctic catalog url for 'data' with branch name", () => {
    const namespace = "main";
    const result = constructArcticUrl({
      ...params,
      namespace: namespace,
    });
    expect(result).to.equal(`${params.baseUrl}/data/${namespace}`);
    expect(arcticCatalogDataSpy).to.have.been.called;
  });

  it("returns the arctic catalog url for 'data' with branch name and namespace", () => {
    const namespace = "main/folder1/folder2";
    const result = constructArcticUrl({
      ...params,
      namespace: namespace,
    });
    expect(result).to.equal(`${params.baseUrl}/data/${namespace}`);
    expect(arcticCatalogDataSpy).to.have.been.called;
  });

  it("returns the arctic catalog url for 'data' with branch name, namespace, and hash", () => {
    const namespace = "main/folder1/folder2";
    const hash = "?hash=a1s2d3f4";
    const result = constructArcticUrl({
      ...params,
      namespace: namespace,
      hash: hash,
    });
    expect(result).to.equal(`${params.baseUrl}/data/${namespace}${hash}`);
    expect(arcticCatalogDataSpy).to.have.been.called;
  });

  it("returns the arctic catalog url for 'commits' without branch name", () => {
    const result = constructArcticUrl({
      ...params,
      namespace: "",
      tab: "commits",
    });
    expect(result).to.equal(`${params.baseUrl}/commits`);
    expect(arcticCatalogCommitsBaseSpy).to.have.been.called;
  });

  it("returns the arctic catalog url for 'commits' with branch name", () => {
    const namespace = "main";
    const result = constructArcticUrl({
      ...params,
      namespace: namespace,
      tab: "commits",
    });
    expect(result).to.equal(`${params.baseUrl}/commits/${namespace}`);
    expect(arcticCatalogCommitsSpy).to.have.been.called;
  });

  it("returns the arctic catalog url for 'commits' with branch name and namespace", () => {
    const namespace = "main/folder1/folder2";
    const result = constructArcticUrl({
      ...params,
      namespace: namespace,
      tab: "commits",
    });
    expect(result).to.equal(`${params.baseUrl}/commits/${namespace}`);
    expect(arcticCatalogCommitsSpy).to.have.been.called;
  });

  it("returns the arctic catalog url for 'commits' with branch name, namespace, and hash", () => {
    const namespace = "main/folder1/folder2";
    const hash = "?hash=a1s2d3f4";
    const result = constructArcticUrl({
      ...params,
      namespace: namespace,
      tab: "commits",
      hash: hash,
    });
    expect(result).to.equal(`${params.baseUrl}/commits/${namespace}${hash}`);
    expect(arcticCatalogCommitsSpy).to.have.been.called;
  });

  it("returns the arctic catalog url for 'commit', looking at commit details", () => {
    const commitId = "a1s2d3f4";
    const namespace = "main";
    const result = constructArcticUrl({
      ...params,
      tab: "commit",
      namespace: namespace,
      commitId: commitId,
    });
    expect(result).to.equal(
      `${params.baseUrl}/commit/${namespace}/${commitId}`
    );
    expect(arcticCatalogCommitSpy).to.have.been.called;
  });

  it("returns the arctic catalog url for 'branches'", () => {
    const result = constructArcticUrl({ ...params, tab: "branches" });
    expect(result).to.equal(`${params.baseUrl}/branches`);
    expect(arcticCatalogBranchesSpy).to.have.been.called;
  });

  it("returns the arctic catalog url for 'tags'", () => {
    const result = constructArcticUrl({ ...params, tab: "tags" });
    expect(result).to.equal(`${params.baseUrl}/tags`);
    expect(arcticCatalogTagsSpy).to.have.been.called;
  });
});

describe("Arcitc Sources - Constructing URLs for navigating across tabs and homepage", () => {
  const sourceId = "test_arctic_source";
  const params: ArcticUrlProps = {
    type: "source",
    baseUrl: `/sources/arctic/${sourceId}`,
    tab: "data",
    namespace: "",
    hash: "",
    commitId: "",
  };

  const arcticSourceCommitsNonBaseSpy = sinon.spy(
    PATHS,
    "arcticSourceCommitsNonBase"
  );
  const arcticSourceCommitsSpy = sinon.spy(PATHS, "arcticSourceCommits");
  const arcticSourceCommitSpy = sinon.spy(PATHS, "arcticSourceCommit");
  const arcticSourceTagsSpy = sinon.spy(PATHS, "arcticSourceTagsNonBase");
  const arcticSourceBranchesSpy = sinon.spy(
    PATHS,
    "arcticSourceBranchesNonBase"
  );

  it("returns the default concatenated url", () => {
    const defaultParams: ArcticUrlProps = {
      type: "source",
      baseUrl: "",
      tab: "commits",
      namespace: "main/folder1/folder2",
      hash: "?hash=a1s2d3f4",
    };
    const result = constructArcticUrl(defaultParams);
    expect(result).to.equal(
      `${defaultParams.baseUrl}/${defaultParams.tab}/${defaultParams.namespace}${defaultParams.hash}`
    );
  });

  it("returns the homepage source url for 'data' without branch/namespace", () => {
    const result = constructArcticUrl(params);
    expect(result).to.equal(
      `/source/${sourceId}${convertCatalogToSourceSpace(params.namespace)}`
    );
  });

  it("returns the homepage source url for 'data' with only branch", () => {
    const namespace = "main";
    const result = constructArcticUrl({
      ...params,
      namespace: namespace,
    });
    expect(result).to.equal(
      `/source/${sourceId}${convertCatalogToSourceSpace(namespace)}`
    );
  });

  it("returns the homepage source url for 'data' with branch and namespace", () => {
    const namespace = "main/folder1/folder2";
    const result = constructArcticUrl({
      ...params,
      namespace: namespace,
    });
    expect(result).to.equal(
      `/source/${sourceId}${convertCatalogToSourceSpace(namespace)}`
    );
  });

  it("returns the arctic source url for 'commits' without branch name", () => {
    const namespace = "";
    const tab = "commits";
    const result = constructArcticUrl({
      ...params,
      tab: tab,
      namespace: namespace,
    });
    expect(result).to.equal(`${params.baseUrl}/${tab}`);
    expect(arcticSourceCommitsNonBaseSpy).to.have.been.called;
  });

  it("returns the arctic source url for 'commits' with branch name", () => {
    const namespace = "main";
    const tab = "commits";
    const result = constructArcticUrl({
      ...params,
      tab: tab,
      namespace: namespace,
    });
    expect(result).to.equal(`${params.baseUrl}/${tab}/${namespace}`);
    expect(arcticSourceCommitsSpy).to.have.been.called;
  });

  it("returns the arctic source url for 'commits' with branch name and namespace", () => {
    const namespace = "main/folder1/folder2";
    const tab = "commits";
    const result = constructArcticUrl({
      ...params,
      tab: tab,
      namespace: namespace,
    });
    expect(result).to.equal(`${params.baseUrl}/${tab}/${namespace}`);
    expect(arcticSourceCommitsSpy).to.have.been.called;
  });

  it("returns the arctic source url for 'commits' with branch name, namespace, and hash", () => {
    const namespace = "main/folder1/folder2";
    const tab = "commits";
    const hash = "?hash=a1s2d3f4";
    const result = constructArcticUrl({
      ...params,
      tab: tab,
      namespace: namespace,
      hash: hash,
    });
    expect(result).to.equal(`${params.baseUrl}/${tab}/${namespace}${hash}`);
    expect(arcticSourceCommitsSpy).to.have.been.called;
  });

  it("returns the arctic source url for 'commit', looking at commit details", () => {
    const commitId = "a1s2d3f4";
    const namespace = "main";
    const result = constructArcticUrl({
      ...params,
      tab: "commit",
      namespace: namespace,
      commitId: commitId,
    });
    expect(result).to.equal(
      `${params.baseUrl}/commit/${namespace}/${commitId}`
    );
    expect(arcticSourceCommitSpy).to.have.been.called;
  });

  it("returns the arctic source url for 'branches'", () => {
    const result = constructArcticUrl({ ...params, tab: "branches" });
    expect(result).to.equal(`${params.baseUrl}/branches`);
    expect(arcticSourceBranchesSpy).to.have.been.called;
  });

  it("returns the arctic source url for 'tags'", () => {
    const result = constructArcticUrl({ ...params, tab: "tags" });
    expect(result).to.equal(`${params.baseUrl}/tags`);
    expect(arcticSourceTagsSpy).to.have.been.called;
  });
});
