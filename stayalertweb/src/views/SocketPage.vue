<template>
  <div>
    <el-button type="success" icon="el-icon-plus" circle @click="sendMsg"></el-button>
  </div>
</template>

<script>
import SockJS from 'sockjs-client';
import Stomp from 'stomp-websocket'

export default {
  name: "SocketPage",
  data() {
    return {
      socket: null,
      stompClient: null
    }
  },
  methods: {
    sendMsg() {
      this.stompClient.send('/api/send-device-data', {}, "hello")
    },
    createStompClient() {
      const socket = new SockJS('http://localhost:8081/ws');
      // const this_ = this
      this.stompClient = Stomp.over(socket);
      this.stompClient.connect({}, () => {
      })
    }
  },
  created() {
    this.createStompClient()
  }
}
</script>

<style scoped>

</style>