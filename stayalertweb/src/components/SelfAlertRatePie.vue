<template>
  <ve-pie :data="chartData" :settings="chartSettings"
          height="220px"
          :extend="chartExtend"></ve-pie>
</template>

<script>
import VePie from 'v-charts/lib/pie.common'

export default {
  name: "SelfAlertRatePie",
  data() {
    return {
      chartSettings: {roseType: 'radius'},
      chartExtend: {
        series: {
          radius: "75%",
          center: ['50%', '55%'],
          ///柱状图颜色顺序，需要手动设置顺序
          itemStyle: {
            normal: {
              color: (params) => {
                return this.colorList[params.dataIndex % this.colorList.length]
              }
            }
          },
        }
      },
      colorList: ["#82e077", "#ff001d", "#61a0a8", "#d48265", "#A8E6CE"]
    }
  },
  props: ['data'],
  computed: {
    chartData() {
      return {
        columns: ['状态', '时间'],
        rows: (this.data || [])
      }
    }
  },
  components: {VePie}
}
</script>

<style scoped>
</style>