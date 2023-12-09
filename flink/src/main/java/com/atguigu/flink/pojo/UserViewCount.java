package com.atguigu.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserViewCount {

    private String user;
    private Long count;
    private Long windowStart;
    private Long windowEnd;

    @Override
    public String toString() {
        return "UserViewCount{" +
                "user='" + user + '\'' +
                ", count=" + count +
                ", windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                '}';
    }
}
