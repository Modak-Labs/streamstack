import { usePaths } from 'vitepress-openapi';
import { dsSpec } from '../../../.vitepress/specs';

export default {
  paths() {
    return usePaths({ spec: dsSpec })
      .getPathsByVerbs()
      .map(({ operationId, summary }) => ({
        params: {
          operationId,
          pageTitle: summary,
        },
      }));
  },
};
