package com.example.batch.config;

import com.example.batch.entity.Customer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CustomerProcessor implements ItemProcessor<Customer, Customer> {    //read input as Customer and write into Customer

    @Override
    public Customer process(Customer customer) throws Exception {
        return customer;
    }

   /* @Override
    public Customer process(Customer customer) throws Exception {
        if(customer.getCountry().equals("United States"))
            return customer;
        else
            return null;
    }*/

}
