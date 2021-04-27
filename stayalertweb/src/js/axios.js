import axios from 'axios'
import ElementUI from 'element-ui'
import {constants} from "./consts"

let host = window.location.host
let reg = /^localhost+/;
// if (reg.test(host)) {
//     //若本地项目调试使用
axios.defaults.baseURL = constants.API_ADDR;
// } else {
//动态请求地址             协议               主机
// axios.defaults.baseURL = "http://hadoophost:8081";
// }

axios.interceptors.response.use(response => {
    if (response.status === 200) {
        return response
    } else {
        ElementUI.Message.error(response.statusText)
        return Promise.reject(response.statusText)
    }
})