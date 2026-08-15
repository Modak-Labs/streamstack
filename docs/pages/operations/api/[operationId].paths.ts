import { usePaths } from 'vitepress-openapi';
import { adminSpec } from '../../../.vitepress/specs';

export default {
  paths() {
    return usePaths({ spec: adminSpec })
      .getPathsByVerbs()
      .map(({ operationId, summary }) => ({
        params: {
          operationId,
          pageTitle: summary,
        },
      }));
  },
};
