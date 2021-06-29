package com.bidata.example.hudi.example

import com.alibaba.fastjson.JSON

case class User(id :Integer
                , birthday : String
                , name :String
                , createTime :String
                , position :String)

object User {

  def apply(json: String) = {
    val jsonObject = JSON.parseObject(json)

    val id = jsonObject.getInteger("id")
    val birthday = jsonObject.getString("birthday");
    val name = jsonObject.getString("name")

    val createTime = jsonObject.getString("createTime");
    val position = jsonObject.getString("position")

    new User(id
      , birthday
      , name
      , createTime
      , position)
  }

}