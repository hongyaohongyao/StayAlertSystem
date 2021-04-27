import Vue from 'vue'
import VueRouter from 'vue-router'
import AnalogData from "@/views/AnalogData";
import Index from "../views/Index"
import UserData from "@/views/UserData";
import CountData from "@/views/CountData";

Vue.use(VueRouter)

const routes = [
    {
        path: '/',
        name: 'Root',
        redirect: {name: 'Index'}
    },
    {
        path: '/analog-data',
        name: 'AnalogData',
        component: AnalogData
    },
    {
        path: '/index',
        name: 'Index',
        component: Index,
        children: [
            {
                path: 'user-data',
                name: 'UserData',
                component: UserData,
                meta: {
                    pathName: '实时用户统计数据'
                }
            },
            {
                path: 'count-data',
                name: 'CountData',
                component: CountData,
                meta: {
                    pathName: '实时统计数据'
                }
            }
        ]
    },
    {
        path: '/about',
        name: 'About',
        // route level code-splitting
        // this generates a separate chunk (about.[hash].js) for this route
        // which is lazy-loaded when the route is visited.
        component: () => import(/* webpackChunkName: "about" */ '../views/About.vue')
    }
]

const router = new VueRouter({
    mode: 'history',
    base: process.env.BASE_URL,
    routes
})

export default router
