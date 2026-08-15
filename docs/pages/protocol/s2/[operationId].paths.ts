import { usePaths } from 'vitepress-openapi';
import { s2Spec } from '../../../.vitepress/specs';

export default {
  paths() {
    return usePaths({ spec: s2Spec })
      .getPathsByVerbs()
      .map(({ operationId, summary }) => ({
        params: {
          operationId,
          pageTitle: summary,
        },
      }));
  },
};
