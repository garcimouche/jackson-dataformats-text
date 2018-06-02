package perf;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * POJO representing data from Sales
 */
public class SalesEntry
{
    @JsonProperty("Region") public String region;
    @JsonProperty("Country") public String country;
    @JsonProperty("Item Type") public String itemType;
    @JsonProperty("Sales Channel") public String salesChannel;
    @JsonProperty("Order Priority") public String orderPriority;
    @JsonProperty("Order Date") public String orderDate;
    @JsonProperty("Order ID") public String orderId;
    @JsonProperty("Ship Date") public String shipDate;
    @JsonProperty("Units Sold") public int unitsSold;
    @JsonProperty("Unit Price") public BigDecimal unitPrice;
    @JsonProperty("Unit Cost") public BigDecimal unitCose;
    @JsonProperty("Total Revenue") public BigDecimal totalRevenue;
    @JsonProperty("Total Cost") public BigDecimal totalCost;
    @JsonProperty("Total Profit") public BigDecimal totalProfit;
    @Override
    public String toString() {
        return "SalesEntry [region=" + region + ", country=" + country + ", itemType=" + itemType + ", salesChannel=" + salesChannel
                + ", orderPriority=" + orderPriority + ", orderDate=" + orderDate + ", orderId=" + orderId + ", shipDate="
                + shipDate + ", unitsSold=" + unitsSold + ", unitPrice=" + unitPrice + ", unitCose=" + unitCose + ", totalRevenue="
                + totalRevenue + ", totalCost=" + totalCost + ", totalProfit=" + totalProfit + "]";
    }
    
    
    
    
    
}