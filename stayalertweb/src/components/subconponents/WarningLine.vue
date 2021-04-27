<template>
  <ve-line :data="chartData"
           :settings="chartSettings"
           height="250px"
           :colors="colorList"
           :extend="chartExtend"></ve-line>
</template>

<script>
import VeLine from "v-charts/lib/line.common";

export default {
  name: "WarningLine",
  components: {VeLine},
  props: ["data"],
  data() {
    return {
      chartSettings: {
        xAxisType: "time",
        yAxisType: ['0.[00]a', '0.[00]%'],
        yAxisName: ['数值轴', '比率轴'],
        area: true,
        axisSite: {
          right: ['警觉']
        }
      },
      chartExtend: {
        series: {
          // color: (params) => {
          //   return this.colorList[params.seriesIndex % this.colorList.length]
          // },
          // ///柱状图颜色顺序，需要手动设置顺序
          // itemStyle: {
          //   normal: {
          //     color: (params) => {
          //       return this.colorList[params.seriesIndex % this.colorList.length]
          //     }
          //   },
          //   lineStyle: {
          //     color: (params) => {
          //       return this.colorList[params.seriesIndex % this.colorList.length]
          //     }
          //   }
          // },
        }
      },
      colorList: ["#ff001d", "#2e65ff", "#61a0a8", "#d48265", "#A8E6CE"]
    }
  },
  computed: {
    chartData() {
      return {
        columns: ['时间', '警觉', "速度"],
        rows: (this.data || [])
      }
    }
  }
}
</script>

<style scoped>

</style>