package com.atguigu.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderDetailEvent {

    private String detailId;
    private String orderId;
    private Long ts;

}
