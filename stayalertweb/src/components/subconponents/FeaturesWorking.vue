<template>
    <ve-radar :data="chartData"
              :settings="chartSettings"
              height="350px">
    </ve-radar>
</template>

<script>
    import VeRadar from "v-charts/lib/radar.common";

    export default {
        name: "FeaturesWorking",
        components: {VeRadar},
        props: ["dataColumns", "dataValue"],
        data() {
            return {}
        },
        computed: {
            chartData() {
                return {
                    columns: (this.dataColumns || []),
                    rows: [(this.dataValue || {}), this.maxData, this.minData]
                }
            },
            maxData() {
                if (this.dataColumns.length <= 1)
                    return {}
                let df = {"特征类型": "最大值"}
                for (let i = 1; i < this.dataColumns.length; i++) {
                    df[this.dataColumns[i]] = 1
                }
                return df
            },
            minData() {
                if (this.dataColumns.length <= 1)
                    return {}
                let df = {"特征类型": "最小值"}
                for (let i = 1; i < this.dataColumns.length; i++) {
                    df[this.dataColumns[i]] = 0
                }
                return df
            },
            chartSettings() {
                return {
                    dataType: this.dataType
                }
            }
        }
    }
</script>

<style scoped>

</style>