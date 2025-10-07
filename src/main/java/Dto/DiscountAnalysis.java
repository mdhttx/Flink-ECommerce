package Dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import java.sql.Date;

@Data
@AllArgsConstructor
public class DiscountAnalysis {
    private Date transactionDate;
    private Integer discountPercentage;
    private Double totalDiscounts;
    private Integer usageCount;
}