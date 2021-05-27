package com.luoj.task.learn.operator.aggregate.example001;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-27
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class ProductViewData {

    private String productId;

    private String userId;

    private Long operationType;

    private Long timestamp;

}
