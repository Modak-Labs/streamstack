---
aside: false
outline: false
---

<script setup>
import { useRoute } from 'vitepress'
import spec from '../../../../frontend/s2/openapi.yml?raw'

const operationId = useRoute().data.params.operationId
</script>

<OAOperation :operation-id="operationId" :spec="spec" hide-branding />
