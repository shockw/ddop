import request from '@/utils/request'

// 查询学生信息列表
export function listStuduent(query) {
  return request({
    url: '/train/studuent/list',
    method: 'get',
    params: query
  })
}

// 查询学生信息详细
export function getStuduent(studentId) {
  return request({
    url: '/train/studuent/' + studentId,
    method: 'get'
  })
}

// 新增学生信息
export function addStuduent(data) {
  return request({
    url: '/train/studuent',
    method: 'post',
    data: data
  })
}

// 修改学生信息
export function updateStuduent(data) {
  return request({
    url: '/train/studuent',
    method: 'put',
    data: data
  })
}

// 删除学生信息
export function delStuduent(studentId) {
  return request({
    url: '/train/studuent/' + studentId,
    method: 'delete'
  })
}
