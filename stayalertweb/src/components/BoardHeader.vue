<template>
  <el-header height="60px">
    <input v-model="userId" placeHolder="驾驶员id"></input>
    <span style="padding-right: 10px"></span>
    <el-button type="success" icon="el-icon-plus" circle @click="addViewer"></el-button>
  </el-header>
</template>

<script>

export default {
  name: "Head",
  data() {
    return {
      userId: ""
    }
  },
  props: ['uid', 'processing_info'],
  methods: {
    addViewer() {
      if (/^[A-Za-z0-9]+$/.test(this.userId)) {
        this.$emit("addBoard", this.userId)
      } else {
        this.$message({
          dangerouslyUseHTMLString: true,
          message: "请输入合法的用户id<br/> id输入为字母或数字的组合",
          type: 'warning'
        })
      }
    }
  }
}
</script>

<style scoped>
.el-header {
  background-color: #2F4F4F;
  color: #FFFFFF;
  text-align: left;
  line-height: 55px;
  font-size: 20px;
  width: 100%;
}
</style>