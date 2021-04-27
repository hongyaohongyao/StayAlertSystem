<template>
  <ve-bar :data="chartData"
          style="width: 100%;"
          :extend="chartExtend"
          height="300px"
          :settings="chartSettings"
  ></ve-bar>
</template>

<script>
import VeBar from 'v-charts/lib/bar.common'

export default {
  name: "AgeGroupAlertRateBar",
  data() {
    return {
      chartSettings: {
        dimension: ['年龄段'],
        metrics: ['警惕率'],
      },
      chartExtend: {
        series: {
          center: ['50%', '50%'],
          height: 10,
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
      colorList: ["#d48265", "#69D2E7", "#AAB3AB", "#61a0a8", "#A8E6CE"]
    }
  },
  props: ['data'],
  computed: {
    chartData() {
      return {
        columns: ['年龄段', '警惕率'],
        rows: (this.data || [])
      }
    }
  },
  components: {VeBar}
}
</script>

<style scoped>

</style>