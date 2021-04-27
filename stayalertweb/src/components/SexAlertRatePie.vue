<template>
  <ve-pie :data="chartData" :settings="chartSettings"
          height="350px"
          align="center"
          :extend="chartExtend"></ve-pie>
</template>

<script>
import VePie from 'v-charts/lib/pie.common'

export default {
  name: "SexAlertRatePie",
  data() {
    return {
      chartSettings: {roseType: 'radius'},
      chartExtend: {
        series: {
          center: ['50%', '40%'],
        }
      }
    }
  },
  props: ['data'],
  computed: {
    chartData() {
      return {
        columns: ['性别', '警惕率'],
        rows: (this.data || [])
      }
    }
  },
  components: {VePie}
}
</script>

<style scoped>
</style>