# nessie-rest-javax

This module contains copies of REST related nessie classes using javax annotations.
They were copied out of the PR that removed them in OSS nessie:
https://github.com/projectnessie/nessie/pull/7837

The more recent jakarta-annotation based implementations can be found here:
https://github.com/projectnessie/nessie/tree/9b8999c9c99d8efcfc71820aaa50617d9520be78/servers/rest-common/src/main/java/org/projectnessie/services/rest

In future nessie upgrades, we might need to keep the logic in sync until we can
use jakarta annotations in the Dremio build as well.
