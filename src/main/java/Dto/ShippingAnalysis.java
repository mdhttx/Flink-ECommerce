package Dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import java.sql.Date;

@Data
@AllArgsConstructor
public class ShippingAnalysis {
    private Date transactionDate;
    private String shippingMethod;
    private Double totalShippingRevenue;
    private Integer shipmentsCount;
}