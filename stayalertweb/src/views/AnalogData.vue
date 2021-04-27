<template>
  <div>
    <el-button v-if="showStart" type="success" @click="createStompClients">Start</el-button>
    <el-button v-else type="primary" @click="enableSendMsg=!enableSendMsg">
      {{ enableSendMsg ? "Stop" : "Restart" }}
    </el-button>
    <h5>{{ testMsg }}</h5>
    <h5>{{ testMsg0 }}</h5>
    <div v-for="item in clients" :key="item.key">
      {{ item.id }} : {{ item.msg }}
    </div>
  </div>
</template>

<script>
import SockJS from "sockjs-client";
import Stomp from "stomp-websocket";
import {constants} from "@/js/consts";

export default {
  name: "AnalogData",
  data() {
    return {
      socket_addr: constants.API_ADDR + '/ws',
      // candidate_id: ['hyhy', 'haha', 'hoho', 'huohuo'],
      candidate_id: ['user1', 'user2', 'user3', 'user4', 'user5', 'user6'],
      enableSendMsg: true,
      clients: [],
      testMsg: "1",
      testMsg0: "1",
      showStart: true,
      analog_data: null
    }
  },
  methods: {
    createStompClients() {
      for (let i = 0; i < this.candidate_id.length; i++) {
        this.createClient(this.candidate_id[i], this.analog_data[i % this.analog_data.length])
      }
      this.showStart = false
      this.afterStart()
    },
    afterStart() {
      {
        const socket = new SockJS(this.socket_addr);
        const this_ = this
        const stompClient = Stomp.over(socket);
        stompClient.connect({}, () => {
          stompClient.subscribe('/topic/user-data/user1', (msg) => {
            this_.testMsg = msg.body
          });
        })
      }
      {
        const socket = new SockJS(this.socket_addr);
        const this_ = this
        const stompClient = Stomp.over(socket);
        stompClient.connect({}, () => {
          stompClient.subscribe('/topic/count-data', (msg) => {
            this_.testMsg0 = msg.body
          });
        })
      }
    },
    createClient(id, ana_data) {
      ana_data = JSON.parse(JSON.stringify(ana_data))
      const socket = new SockJS(this.socket_addr);
      const this_ = this
      const stompClient = Stomp.over(socket);
      stompClient.connect({}, () => {
        const socket_msg = {
          client: stompClient,
          msg: "",
          id: id,
          key: id + new Date().toUTCString() + Math.round(Math.random() * 100000)
        }
        this_.clients.push(socket_msg)
        this_.clients = this_.clients
        let loop = 0;
        setInterval(() => {
          if (this_.enableSendMsg) {
            let data = ana_data[loop]
            data["id"] = id
            data["timestamp"] = new Date().getTime()
            let msg = JSON.stringify(data)
            socket_msg.msg = msg
            stompClient.send("/api/send-device-data", {}, msg)
            loop = (loop + 1) % ana_data.length
          }
        }, 500)
      })
    }
  },
  created() {
    const _this = this
    this.$axios.get(constants.LOCALHOST_ADDR + "/analog_data.json", {}).then(msg => {
      _this.analog_data = msg.data
    }).catch(err => {
      _this.$message.warning(JSON.stringify(err))
    })
  }
}
</script>

<style scoped>

</style>