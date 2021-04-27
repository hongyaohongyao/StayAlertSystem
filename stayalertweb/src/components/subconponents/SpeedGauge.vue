<template>
    <ve-gauge :data="chartData"
              :settings="chartSettings"
              height="300px"></ve-gauge>
</template>

<script>
    import VeGauge from "v-charts/lib/gauge.common"

    export default {
        name: "SpeedGauge",
        components: {VeGauge},
        props: ["speed"],
        data() {
            return {
                chartSettings: {
                    dataName: {
                        '速度': 'km/h',
                    },
                    seriesMap: {
                        '速度': {
                            min: 0,
                            max: 220,
                            splitNumber: 11,
                            axisLine: {
                                lineStyle: {
                                    color: [[0.09, 'lime'], [0.82, '#1e90ff'], [1, '#ff4500']],
                                    width: 10,
                                    shadowColor: "#D3D3D3",
                                    shadowBlur: 10
                                }
                            }
                        }
                    },
                    dimension: 'type',
                    metrics: 'value'
                }
            }
        },
        computed: {
            chartData() {
                return {
                    columns: ['a', 'b', 'type', 'value'],
                    rows: [
                        {type: '速度', value: this.speed, a: 1, b: 2},
                    ]
                }
            }
        }
    }
</script>

<style scoped>

</style>