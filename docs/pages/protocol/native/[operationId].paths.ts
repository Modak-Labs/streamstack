import { usePaths } from 'vitepress-openapi';
import { nativeSpec } from '../../../.vitepress/specs';

export default {
  paths() {
    return usePaths({ spec: nativeSpec })
      .getPathsByVerbs()
      .map(({ operationId, summary }) => ({
        params: {
          operationId,
          pageTitle: summary,
        },
      }));
  },
};
