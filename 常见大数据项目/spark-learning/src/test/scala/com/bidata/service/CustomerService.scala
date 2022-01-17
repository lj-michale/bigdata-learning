package com.bidata.service

import com.bidata.bean.Customer

import scala.collection.mutable.ArrayBuffer
import util.control.Breaks._

class CustomerService {

  // 客户编号
  var customerNum = 1
  val customers = ArrayBuffer(new Customer(1, "tom", '男', 10, "110", "tom@sohu.com"))

  def list(): ArrayBuffer[Customer] = {
    this.customers
  }

  def add(customer: Customer):Boolean = {
    customerNum += 1
    //    val index: Int = customers.map(_.id).max
    customer.id = customerNum
    this.customers.append(customer)
    true
  }

  /**
   * @param id 用户id
   * @return
   */
  def del(id: Int):Boolean = {
    val index = findIndexById(id)
    if (index != -1) {
      customers.remove(index)
      return true
    }
    false
  }

  def findIndexById(id: Int) = {
    var index = -1
    // 遍历costumers
    breakable {
      for (i <- 0 until customers.length) {
        if (customers(i).id == id) {
          index = i
          break()
        }
      }
    }
    index
  }

}