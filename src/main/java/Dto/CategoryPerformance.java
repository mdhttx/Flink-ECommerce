package Dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import java.sql.Date;

@Data
@AllArgsConstructor
public class CategoryPerformance {
    private Date transactionDate;
    private String category;
    private Double totalRevenue;
    private Integer itemsSold;
    private Double avgOrderValue;
}