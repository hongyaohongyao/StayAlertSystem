<template>
  <div>
    <div v-if="socketSupport">
      <el-card class="box-card">
        <div slot="header" class="clearfix">
          <span style="float: left;font-weight: bolder;font-size: large">{{ userId }}</span>
          <el-button style="float: right"
                     type="danger"
                     icon="el-icon-close"
                     circle @click="closeBoard"/>
        </div>
        <el-row>
          <el-col :span="5">
            <el-card style="text-align: left">
              <div>
                <span style="font-weight: bolder; ">用户名: </span>
                <span>{{ userId }}</span>
              </div>
              <div>
                <span style="font-weight: bolder; padding-left: 12px">性 别: </span>
                <span>{{ userSex }}</span>
              </div>
              <div>
                <span style="font-weight: bolder; padding-left: 12px">年 龄: </span>
                <span>{{ userAge }}</span>
              </div>
            </el-card>
            <el-card style="text-align: left;margin-top: 10px">
              <div>
                <span style="font-weight: bolder; ">使用距离: </span>
                <span>{{ usageDistance }}</span>
              </div>
            </el-card>
          </el-col>
          <el-col :span="7">
            <self-alert-rate-pie :data="selfAlertRate"/>
          </el-col>
          <el-col :span="12">
            <warning-line :data="warningLineData"></warning-line>
          </el-col>
        </el-row>
      </el-card>
    </div>
    <div v-else>
      浏览器不支持socket
    </div>
  </div>
</template>

<script>
import VeLine from 'v-charts/lib/line.common'
import SpeedGauge from "./subconponents/SpeedGauge";
import WarningLine from "./subconponents/WarningLine";
import FeaturesWorking from "./subconponents/FeaturesWorking";
import SelfAlertRatePie from "@/components/SelfAlertRatePie";
import {FormatDateTime} from '@/js/utils'
import SockJS from "sockjs-client";
import {constants} from "@/js/consts";
import Stomp from "stomp-websocket";

export default {
  name: "UserDataBoard",
  components: {SelfAlertRatePie, VeLine, WarningLine, SpeedGauge, FeaturesWorking},
  data() {
    return {
      userSex: "",
      userAge: "",
      usageDistance: "",
      selfAlertRate: [],
      warningLineData: [],
      smoothAlert: 0,
      socketSupport: !!WebSocket
    }
  },
  props: ["userId", "viewerKey"],
  methods: {
    connectSystem() {
      const _this = this
      //测试数据
      // const data = {
      //   "id": "user1",
      //   "sex": "男",
      //   "birthday": "1964-01-20",
      //   "age": 57,
      //   "usageDistance": 210.88194444444417,
      //   "usageTime": 96.0,
      //   "alertTime": 71.0,
      //   "speed": 100,
      //   "isAlert": true,
      //   "timestamp": 1617179654817
      // }
      // _this.setUserData(data)
      this.$axios.get("api/user-data", {params: {userId: this.userId}}).then(msg => {
        if (!!msg.data) {
          _this.setUserData(msg.data)
          _this.createSocket()
        } else {
          _this.$message.warning("无该用户数据")
          _this.closeBoard()
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
        stompClient.subscribe('/topic/user-data/' + this.userId, (msg) => {
          _this.setUserData(JSON.parse(msg.body))
        });
      })
    },
    setUserData(data) {
      if(!!data.userState){
        data =  Object.assign(data, data.userState)
      }
      if(!!data.userInfo){
        data =  Object.assign(data, data.userInfo)
      }
      this.userSex = data.sex;
      this.userAge = data.age;
      this.selfAlertRate = [{
        "状态": "警惕",
        "时间": data.alertTime
      }, {
        "状态": "未警惕",
        "时间": data.usageTime - data.alertTime
      }]
      //设置使用距离
      this.usageDistance = data.usageDistance.toFixed(2) + " M"
      //设置警觉度和速度
      if ("undefined" != typeof data.speed &&
          "undefined" != typeof data.isAlert &&
          "undefined" != typeof data.timestamp) {

        while (this.warningLineData.length >= 15) {
          this.warningLineData.shift()
        }
        this.smoothAlert = (this.smoothAlert + (data.isAlert ? 1 : 0)) / 2
        this.warningLineData.push({
          "时间": FormatDateTime(data.timestamp),
          "警觉": this.smoothAlert,
          "速度": data.speed
        })
        this.warningLineData = this.warningLineData
      }
    },
    closeBoard() {
      this.removeBoard()
    },
    removeBoard() {
      this.$emit("removeBoard", this.viewerKey)
    }
  },
  created() {
    this.connectSystem()
  }
}
</script>

<style scoped>
.box-card {
  width: 98%;
}
</style>