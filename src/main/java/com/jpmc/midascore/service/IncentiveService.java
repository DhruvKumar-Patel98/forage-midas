package com.jpmc.midascore.service;

import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.dto.Incentive;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class IncentiveService {

    private final RestTemplate restTemplate = new RestTemplate();
    private final String INCENTIVE_API_URL = "http://localhost:8080/incentive";

    public float getIncentive(Transaction transaction) {
        try {
            Incentive incentive = restTemplate.postForObject(INCENTIVE_API_URL, transaction, Incentive.class);
            return incentive != null ? incentive.getAmount() : 0f;
        } catch (Exception e) {
            e.printStackTrace();
            return 0f; // fallback if API fails
        }
    }
}