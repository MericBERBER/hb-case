package com.mericberber.hbcase.rest.model;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class ProductViewedByUser {

    private String userId;
    private List<ViewedProduct> products;

    @Getter
    @Setter
    public static class ViewedProduct {

        private String productId;
        private long viewCount;
    }
}
