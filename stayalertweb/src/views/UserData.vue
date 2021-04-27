<template>
  <div>
    <board-header @addBoard="addBoard"></board-header>
    <el-main ref="mainBoard">
      <div v-for="viewer in boardList" :key="viewer.key">
        <user-data-board :user-id="viewer.id"
                         :viewer-key="viewer.key"
                         @removeBoard="removeBoard">
        </user-data-board>
      </div>
    </el-main>
  </div>
</template>

<script>
import BoardHeader from "../components/BoardHeader";
import UserDataBoard from "@/components/UserDataBoard";

export default {
  name: "UserData",
  data() {
    return {
      boardList: [],
    }
  },
  methods: {
    addBoard(driverId) {
      this.boardList.push({
        id: driverId,
        key: new Date().toUTCString() + Math.round(Math.random() * 100000)
      })
    },
    removeBoard(viewerKey) {
      this.boardList = this.boardList.filter(v => v.key !== viewerKey)
    },
  },
  mounted() {
    const _this = this;
    window.onresize = function () { // 定义窗口大小变更通知事件
      _this.$refs.mainBoard.$el.style.height = window.innerHeight - 200 + 'px';
    };
    _this.$refs.mainBoard.$el.style.height = window.innerHeight - 200 + 'px';
  },
  components: {UserDataBoard, BoardHeader}
}
</script>

<style scoped>
.el-main {
  overflow-y: scroll;
  height: inherit;
}
</style>