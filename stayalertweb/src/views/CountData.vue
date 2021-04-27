<template xmlns:el-col="http://www.w3.org/1999/html">
  <div>
    <div v-if="hasData">
      <el-row>
        <el-col :span="6">
          <el-card class="date-card">
            <div slot="header" class="clearfix">
              <span style="font-weight: bold">日期: {{ day }}</span>
              <!--        <el-button style="float: right; padding: 3px 0" type="text">操作按钮</el-button>-->
            </div>
            <div>
              <div class="real-time-style">{{ realTime }}</div>
              <div class="data-time-style">{{ dataTime }}</div>
            </div>
          </el-card>
          <el-card class="sex-alert-rate-card">
            <sex-alert-rate-pie :data="sexAlertRate"/>
          </el-card>
        </el-col>
        <el-col :span="18">
          <el-card class="age-group-alert-rate-card">
            <age-group-alert-rate-bar :data="ageGroupAlertRate"/>
          </el-card>
          <el-card class="period-alert-rate-card" style="padding-top: 10px">
            <period-alert-rate-histogram :data="periodAlertRate"/>
          </el-card>
        </el-col>
      </el-row>
    </div>
    <div v-else>
      <h1>无今日数据</h1>
    </div>
  </div>
</template>

<script>
import SexAlertRatePie from "@/components/SexAlertRatePie";
import AgeGroupAlertRateBar from "@/components/AgeGroupAlertRateBar";
import PeriodAlertRateHistogram from "@/components/periodAlertRateHistogram";
import SockJS from "sockjs-client";
import Stomp from "stomp-websocket";
import {constants} from "@/js/consts"

export default {
  name: "CountData",
  components: {PeriodAlertRateHistogram, AgeGroupAlertRateBar, SexAlertRatePie},
  data() {
    return {
      hasData: true,
      day: "",
      realTime: "",
      dataTime: "",
      sexAlertRate: [],
      ageGroupAlertRate: [],
      periodAlertRate: []
    }
  },
  methods: {
    connectSystem() {
      const _this = this
      //测试数据
      // let data = {
      //   "timestamp": 1617112661883,
      //   "sexMaleAlertRate": {"usageTime": 2.0, "alertTime": 1.0},
      //   "sexFemaleAlertRate": {"usageTime": 3.0, "alertTime": 0.5},
      //   "ageGroupAlertRate": {
      //     "50": {"usageTime": 2.0, "alertTime": 1.0},
      //     "20": {"usageTime": 3.0, "alertTime": 1.0}
      //   },
      //   "periodAlertRate": {
      //     "17": {"usageTime": 2.0, "alertTime": 1.0},
      //     "18": {"usageTime": 12.5, "alertTime": 9.0},
      //     "19": {"usageTime": 13.5, "alertTime": 10.0}
      //   }
      // }
      // _this.setCountData(data)

      this.$axios.get("api/count-data", {}).then(msg => {
        if (!!msg.data) {
          _this.setCountData(msg.data)
          _this.createSocket()
        } else {
          _this.hasData = false
        }
      }).catch(err => {
        _this.$message.warning(JSON.stringify(err))
      })
    },
    createSocket() {
      const socket = new SockJS(constants.API_ADDR + '/ws');
      const _this = this
      const stompClient = Stomp.over(socket);
      stompClient.connect({}, () => {
        stompClient.subscribe('/topic/count-data', (msg) => {
          // console.log(msg)
          _this.setCountData(JSON.parse(msg.body))
        });
      })
    },
    setCountData(data) {
      //设置年龄
      {
        this.dataTime = this.getTimeString(new Date(data.timestamp))
        let alertRate0 = data.sexMaleAlertRate
        let alertRate1 = data.sexFemaleAlertRate
        this.sexAlertRate = [
          {"性别": "男", "警惕率": (alertRate0.alertTime / alertRate0.usageTime).toFixed(2)},
          {"性别": "女", "警惕率": (alertRate1.alertTime / alertRate1.usageTime).toFixed(2)}
        ]
      }
      //设置年龄段警惕率
      {
        this.ageGroupAlertRate = []
        let keys = Object.keys(data.ageGroupAlertRate)
        keys.sort((x, y) => parseInt(x) - parseInt(y)) //排序
        for (let key of keys) {
          let alertRate = data.ageGroupAlertRate[key]
          this.ageGroupAlertRate.push({
            "年龄段": key + "-" + (key - 1 + 10) + "岁",
            "警惕率": (alertRate.alertTime / alertRate.usageTime).toFixed(2)
          })
        }
        this.ageGroupAlertRate = this.ageGroupAlertRate
      }
      //设置不同时间段警惕率
      {
        this.periodAlertRate = []
        let keys = Object.keys(data.periodAlertRate)
        keys.sort((x, y) => parseInt(x) - parseInt(y)) //排序
        for (let key of keys) {
          let alertRate = data.periodAlertRate[key]
          this.periodAlertRate.push({
            "时间段": key + "时",
            "警惕率": (alertRate.alertTime / alertRate.usageTime).toFixed(2)
          })
        }
        this.periodAlertRate = this.periodAlertRate
      }
    },
    getTimeString(date) {
      const D = ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09']
      let h = date.getHours()
      h = h < 10 ? D[h] : h
      let m = date.getMinutes()
      m = m < 10 ? D[m] : m
      let s = date.getSeconds()
      s = s < 10 ? D[s] : s
      return h + ":" + m + ":" + s
    }
  },
  created() {
    const _this = this
    //显示日期
    setInterval(() => {
      const now = new Date();
      _this.day = now.getFullYear() + "-" + (now.getMonth() + 1) + "-" + now.getDate()
      _this.realTime = _this.getTimeString(now)
    }, 1000)
    //连接系统
    this.connectSystem()
  }
}
</script>

<style scoped>
.date-card {
  width: 300px;
  height: 225px;
}

.sex-alert-rate-card {
  width: 300px;
  height: 300px;
}

.real-time-style {
  font-weight: bolder;
  font-size: xxx-large;
}

.data-time-style {
  margin-top: 10px;
  color: #f88f96;
  font-weight: bolder;
  font-size: xx-large;
}

.age-group-alert-rate-card {
  width: 100%;
  height: 300px;
}

.period-alert-rate-card {
  width: 100%;
  height: 300px;
}
</style>