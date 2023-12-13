package com.atguigu.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AppEvent {
   private String orderId; // 订单id
   private String eventType; //事件类型
   private Long ts;
}
