package Dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import java.sql.Date;

@Data
@AllArgsConstructor
public class OrderStatusMetrics {
    private Date transactionDate;
    private String orderStatus;
    private Integer orderCount;
    private Double totalValue;
}