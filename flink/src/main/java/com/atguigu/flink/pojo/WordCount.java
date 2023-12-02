package com.atguigu.flink.pojo;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class WordCount {
    private String word;
    private Integer count;

    @Override
    public String toString() {
        return "(" +
                "word='" + word  +
                ", count=" + count +
                ')';
    }
}
