package Dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import java.sql.Date;

@Data
@AllArgsConstructor
public class SalesPerDevice {
    private Date transactionDate;
    private String deviceType;
    private Double totalSales;
    private Integer transactionCount;
}