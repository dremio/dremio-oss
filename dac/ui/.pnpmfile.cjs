// Remove this file when Cheerio fixes issue in rc.11:
// https://stackoverflow.com/questions/72346115/i-am-get-an-export-error-when-build-test/72350383
function readPackage(pkg, context) {
  if (pkg.name === "enzyme") {
    pkg.dependencies = {
      ...pkg.dependencies,
      cheerio: "1.0.0-rc.10",
    };
  }
  return pkg;
}

module.exports = {
  hooks: {
    readPackage,
  },
};
