package com.atguigu.flink.pojo;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ThirdPartyEvent {

    private String orderId;
    private String eventType; // 事件类型

    private String thirdPartyName; // 第三方名称

    private Long ts;

}
