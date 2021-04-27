<template>
  <ve-histogram :data="chartData"
                style="width: 100%; height: 250px"
                height="300px"
                :extend="chartExtend"
                :settings="chartSettings"
  ></ve-histogram>
</template>

<script>
import VeHistogram from 'v-charts/lib/histogram.common'

export default {
  name: "PeriodAlertRateHistogram",
  data() {
    return {
      chartExtend: {
        series: {
          center: ['50%', '50%'],
          ///柱状图颜色顺序，需要手动设置顺序
          itemStyle: {
            normal: {
              color: (params) => {
                return this.colorList[params.dataIndex % this.colorList.length]
              }
            }
          },
          // barWidth : 20,//柱图 -- 条宽度
        }
      },
      chartSettings: {
        dimension: ['时间段'],
        metrics: ['警惕率'],
        // showLine: ["警惕率"]
      },
      colorList: ["#69D2E7", "#AAB3AB", "#61a0a8", "#d48265", "#A8E6CE"],
    }
  },
  props: ['data'],
  computed: {
    chartData() {
      return {
        columns: ['时间段', '警惕率'],
        rows: (this.data || [])
      }
    }
  },
  components: {VeHistogram}
}
</script>

<style scoped>

</style>