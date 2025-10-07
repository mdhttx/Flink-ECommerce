/*
package Dto;

import lombok.Data;

import java.sql.Timestamp;
@Data
public class Transaction {

    private String transactionId;
    private String productId;
    private String productName;
    private String productCategory;
    private double productPrice;
    private int productQuantity;
    private String productBrand;
    private double totalAmount;
    private String currency;
    private String customerId;
    private Timestamp transactionDate;
    private String paymentMethod;

}

*/

package Dto;

import lombok.Data;
import java.sql.Timestamp;

@Data
public class Transaction {
    // Existing fields
    private String transactionId;
    private String productId;
    private String productName;
    private String productCategory;
    private double productPrice;
    private int productQuantity;
    private String productBrand;
    private double totalAmount;
    private String currency;
    private String customerId;
    private Timestamp transactionDate;
    private String paymentMethod;

    // Missing fields from Python
    private String customerEmail;
    private ShippingAddress shippingAddress;  // Nested object
    private double shippingCost;
    private double taxAmount;
    private double taxRate;
    private int discountPercentage;
    private double discountAmount;
    private double finalAmount;
    private String orderStatus;
    private String shippingMethod;
    private String discountCode;
    private Boolean isGift;
    private String giftMessage;
    private String customerIpAddress;
    private String deviceType;
    private String browser;
    private Boolean returnEligible;
    private int warrantyMonths;
}