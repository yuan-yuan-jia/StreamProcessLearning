package com.atguigu.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WaterSensor {

    // 传感器id
    private String id;
    // 水位记录
    private Integer vc;
    // 记录时间
    private Long ts;

}
