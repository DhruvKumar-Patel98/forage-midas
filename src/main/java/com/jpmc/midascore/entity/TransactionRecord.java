package com.jpmc.midascore.entity;

import jakarta.persistence.*;

@Entity
public class TransactionRecord {

    @Id
    @GeneratedValue
    private Long id;

    private float amount;

    @Column(nullable = false)
    private float incentive;

    @ManyToOne
    private UserRecord sender;

    @ManyToOne
    private UserRecord recipient;

    protected TransactionRecord() {}

    public TransactionRecord(float amount, UserRecord sender, UserRecord recipient, float incentive) {
        this.amount = amount;
        this.sender = sender;
        this.recipient = recipient;
        this.incentive = incentive;
    }

    public Long getId() { return id; }
    public float getAmount() { return amount; }
    public UserRecord getSender() { return sender; }
    public UserRecord getRecipient() { return recipient; }
    public float getIncentive() { return incentive; }
    public void setIncentive(float incentive) { this.incentive = incentive; }
}