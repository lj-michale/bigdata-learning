package com.aurora.cdc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

@Data
public class User {

    @JSONField(name = "password")
    private String password;

    @JSONField(name = "create_time")
    private long createTime;

    @JSONField(name = "phone")
    private String phone;

    @JSONField(name = "id_card")
    private String idCard;

    @JSONField(name = "id")
    private int id;

    @JSONField(name = "first_name")
    private String firstName;

    @JSONField(name = "email")
    private String email;

    @JSONField(name = "username")
    private String username;

    @JSONField(name = "status")
    private int status;

    public static User of(String json) {
        if (StringUtils.isEmpty(json)) {
            return new User();
        }
        return JSON.parseObject(json, User.class);
    }

    @Override
    public String toString() {
        return
                "User{" +
                        "password = '" + password + '\'' +
                        ",create_time = '" + createTime + '\'' +
                        ",phone = '" + phone + '\'' +
                        ",id_card = '" + idCard + '\'' +
                        ",id = '" + id + '\'' +
                        ",first_name = '" + firstName + '\'' +
                        ",email = '" + email + '\'' +
                        ",username = '" + username + '\'' +
                        ",status = '" + status + '\'' +
                        "}";
    }
}
