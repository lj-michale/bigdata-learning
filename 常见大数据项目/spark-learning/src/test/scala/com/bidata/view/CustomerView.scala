package com.bidata.view

import com.bidata.bean.Customer
import com.bidata.service.CustomerService

import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn
import util.control.Breaks._

class CustomerView {

  // 定义一个循环变量控制是否退出while
  var loop = true
  // 定义一个key，用于接收用户输入
  var key = ' '

  val customerService = new CustomerService()

  def mainMenu():Unit = {
    do {
      println("------客户信息管理软件------")
      println("      1 添 加 客 户       ")
      println("      2 修 改 客 户       ")
      println("      3 删 除 客 户       ")
      println("      4 客 户 列 表       ")
      println("      5 退       出       ")
      println("------请选择（1-5）：-------")

      key = StdIn.readChar()
      key match {
        case '1' => this.add()
        case '2' => println("修 改 客 户")
        case '3' => this.del()
        case '4' => this.list()
        case '5' => this.loop = false
      }
    } while (loop)
    println("欢迎下次使用。。。")
  }

  def list(): Unit = {

    println()
    println("-------------客户列表-------------")
    println("编号\t\t姓名\t\t性别\t\t年龄\t\t电话\t\t邮箱")
    val customers: ArrayBuffer[Customer] = customerService.list()
    customers.foreach(println)
    println("-----------客户列表end-----------")
    println()

  }

  def add():Unit = {
    println()
    println("-------------添加客户-------------")
    println("姓名：")
    val name = StdIn.readLine()
    println("性别：")
    val gender = StdIn.readChar()
    println("年龄：")
    val age = StdIn.readShort()
    println("电话：")
    val tel = StdIn.readLine()
    println("邮箱：")
    val email = StdIn.readLine()
    // 构建对象
    val customer = new Customer(name, gender, age, tel, email )
    customerService.add(customer)
    println("-------------添加客户成功-------------")
    println()
  }

  def del():Unit = {
    println()
    println("-------------删除客户-------------")
    println("请选择待删除客户编号（-1退出）")
    val id = StdIn.readInt()
    if (id == -1) {
      println("-------退出删除客户-------")
      return
    }
    println("确认是否删除（Y/N）：")

    // 要求用户在退出时提示 "确认是否删除（Y/N）："，用户必须输入y/n否则循环提示
    var choice = ' '
    breakable {
      do {
        choice = StdIn.readChar().toLower
        if (choice == 'y' || choice  == 'n') {
          break()
        }
        println("确认是否删除（Y/N）：")
      } while (true)
    }

    if (choice == 'y') {
      val bool: Boolean = customerService.del(id)
      if (bool) {
        println("-------删除完成-------")
        return
      }
      println("-------删除没有完成-------")
    }

    println()
    println("-------------删除结束-------------")
  }


}
