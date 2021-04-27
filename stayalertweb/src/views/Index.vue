<template>
  <div style="height: 100%">
    <Header></Header>
    <el-container>
      <el-aside width="15%">
        <el-row>
          <el-col :span="12" style="width: 100%;">
            <el-menu
                default-active="1"
                class="el-menu-vertical-demo"
                @change=""
                :collapse="isCollapse">
              <el-submenu index="1">
                <template slot="title">
                  <span>统计数据</span>
                </template>
                <el-menu-item index="1-1" @click="toCountData">实时统计数据</el-menu-item>
                <el-menu-item index="1-2" @click="toUserData">用户统计数据</el-menu-item>
              </el-submenu>
            </el-menu>
          </el-col>
        </el-row>
      </el-aside>
      <el-container>
        <el-header class="index-subpage-header" height="35px">
          {{ this.$router.currentRoute.meta.pathName || '' }}
        </el-header>
        <el-main>
          <router-view></router-view>
        </el-main>
      </el-container>
    </el-container>
  </div>
</template>

<script>
import Header from "../components/Header";

export default {
  components: {
    Header
  },
  name: "Index",
  data() {
    return {
      isCollapse: false,
      subPageName: ''
    }
  },
  methods: {
    toCountData() {
      this.$router.push("/index/count-data").catch(err=>err)
    },
    toUserData() {
      this.$router.push("/index/user-data").catch(err=>err)
    }
  }

}
</script>

<style scoped>
.el-main {
  background-color: #FFFFFF;
  color: #333;
  text-align: center;
}

.el-container {
  height: 100%;
}

.el-aside {
  height: 100%;
}

.el-row {
  height: 100%;
}

.index-subpage-header {
  background-color: #436443;
  color: #FFFFFF;
  text-align: left;
  font-size: 25px;
}

</style>