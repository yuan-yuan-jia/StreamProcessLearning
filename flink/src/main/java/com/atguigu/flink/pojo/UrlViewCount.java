package com.atguigu.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UrlViewCount {

    private String url;
    private Long count;
    private Long windowStart;
    private Long windowEnd;
}
