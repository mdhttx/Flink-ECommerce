package Dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import java.sql.Date;

@Data
@AllArgsConstructor
public class GiftOrderMetrics {
    private Date transactionDate;
    private Integer giftOrdersCount;
    private Integer regularOrdersCount;
    private Double giftOrdersValue;
}