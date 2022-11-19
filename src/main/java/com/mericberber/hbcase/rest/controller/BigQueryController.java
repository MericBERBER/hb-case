package com.mericberber.hbcase.rest.controller;

import com.mericberber.hbcase.rest.model.ProductViewCount;
import com.mericberber.hbcase.rest.model.ProductViewedByUser;
import com.mericberber.hbcase.rest.service.BigQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class BigQueryController {

    @Autowired
    private BigQueryService bqService;

    @GetMapping("/getMostViewedProductsInLastHour")
    public List<ProductViewCount> getMostViewedProductsInLastHour() throws InterruptedException {

        return bqService.getMostViewedProductsInLastHour();
    }

    @GetMapping("/getLastFiveProductsViewedByUser/{userId}")
    public ProductViewedByUser getLastFiveProductsViewedByUser(@PathVariable String userId) throws InterruptedException {

        return bqService.getLastFiveProductsViewedByUser(userId);
    }

    @GetMapping("/testInsert")
    public void testInsert(){
        bqService.insertTest();
    }
}
