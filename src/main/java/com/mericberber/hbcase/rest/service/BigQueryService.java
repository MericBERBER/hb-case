package com.mericberber.hbcase.rest.service;

import com.mericberber.hbcase.rest.model.ProductViewCount;
import com.mericberber.hbcase.rest.model.ProductViewedByUser;

import java.util.List;


public interface BigQueryService {


    List<ProductViewCount> getMostViewedProductsInLastHour() throws InterruptedException;
    ProductViewedByUser getLastFiveProductsViewedByUser(String userid) throws InterruptedException;

    void insertTest();
}
