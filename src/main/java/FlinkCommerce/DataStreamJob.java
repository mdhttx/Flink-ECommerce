/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package FlinkCommerce;

import Deserializer.JSONValueDeserializationSchema;
import Dto.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.HttpHost;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.index.IndexRequest;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.Requests;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.common.xcontent.XContentType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.jdbc.JdbcSink;
import java.sql.Date;

import static utils.JsonUtil.convertTransactionToJson;

public class DataStreamJob {
    private static final String jdbcUrl = "jdbc:postgresql://localhost:6543/postgres";
    private static final String username = "postgres";
    private static final String password = "postgres";


	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.

        String topic = "financial_transactions";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.enableCheckpointing(5000);

        KafkaSource<Transaction> source = KafkaSource.<Transaction>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics(topic)
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JSONValueDeserializationSchema())
                .build();

        DataStream<Transaction> transactionStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka source");

        transactionStream.print();

        JdbcExecutionOptions execOptions = new JdbcExecutionOptions.Builder().withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build();

        JdbcConnectionOptions connOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName("org.postgresql.Driver")
                .withUsername(username)
                .withPassword(password)
                .build();
        /*
        //create transactions table
        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS transactions (" +
                        "transaction_id VARCHAR(255) PRIMARY KEY, " +
                        "product_id VARCHAR(255), " +
                        "product_name VARCHAR(255), " +
                        "product_category VARCHAR(255), " +
                        "product_price DOUBLE PRECISION, " +
                        "product_quantity INTEGER, " +
                        "product_brand VARCHAR(255), " +
                        "total_amount DOUBLE PRECISION, " +
                        "currency VARCHAR(255), " +
                        "customer_id VARCHAR(255), " +
                        "transaction_date TIMESTAMP, " +
                        "payment_method VARCHAR(255) " +
                        ")",
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {

                },
                execOptions,
                connOptions
        )).name("Create Transactions Table Sink");

        //create sales_per_category table sink
        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS sales_per_category (" +
                        "transaction_date DATE, " +
                        "category VARCHAR(255), " +
                        "total_sales DOUBLE PRECISION, " +
                        "PRIMARY KEY (transaction_date, category)" +
                        ")",
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {

                },
                execOptions,
                connOptions
        )).name("Create Sales Per Category Table");

        //create sales_per_day table sink
        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS sales_per_day (" +
                        "transaction_date DATE PRIMARY KEY, " +
                        "total_sales DOUBLE PRECISION " +
                        ")",
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {

                },
                execOptions,
                connOptions
        )).name("Create Sales Per Day Table");

        //create sales_per_month table sink
        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS sales_per_month (" +
                        "year INTEGER, " +
                        "month INTEGER, " +
                        "total_sales DOUBLE PRECISION, " +
                        "PRIMARY KEY (year, month)" +
                        ")",
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {

                },
                execOptions,
                connOptions
        )).name("Create Sales Per Month Table");


        // INSERT sink
        transactionStream.addSink(JdbcSink.sink(
                "INSERT INTO transactions(transaction_id, product_id, product_name, product_category, product_price, " +
                        "product_quantity, product_brand, total_amount, currency, customer_id, transaction_date, payment_method) " +
                        "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +  // 12 placeholders
                        "ON CONFLICT (transaction_id) DO UPDATE SET " +
                        "product_id = EXCLUDED.product_id, " +
                        "product_name = EXCLUDED.product_name, " +
                        "product_category = EXCLUDED.product_category, " +
                        "product_price = EXCLUDED.product_price, " +
                        "product_quantity = EXCLUDED.product_quantity, " +  // ADDED
                        "product_brand = EXCLUDED.product_brand, " +
                        "total_amount = EXCLUDED.total_amount, " +
                        "currency = EXCLUDED.currency, " +
                        "customer_id = EXCLUDED.customer_id, " +
                        "transaction_date = EXCLUDED.transaction_date, " +
                        "payment_method = EXCLUDED.payment_method " +
                        "WHERE transactions.transaction_id = EXCLUDED.transaction_id" ,
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {
                    preparedStatement.setString(1, transaction.getTransactionId());
                    preparedStatement.setString(2, transaction.getProductId());
                    preparedStatement.setString(3, transaction.getProductName());
                    preparedStatement.setString(4, transaction.getProductCategory());
                    preparedStatement.setDouble(5, transaction.getProductPrice());
                    preparedStatement.setInt(6, transaction.getProductQuantity());
                    preparedStatement.setString(7, transaction.getProductBrand());
                    preparedStatement.setDouble(8, transaction.getTotalAmount());
                    preparedStatement.setString(9, transaction.getCurrency());
                    preparedStatement.setString(10, transaction.getCustomerId());
                    preparedStatement.setTimestamp(11, transaction.getTransactionDate());
                    preparedStatement.setString(12, transaction.getPaymentMethod());
                },
                execOptions,
                connOptions
        )).name("Insert into transactions table sink");

        //map, aggregate, reduce then insert into sales_per_category_table
        transactionStream.map(
                        transaction -> {
                            Date transactionDate = new Date(System.currentTimeMillis());
                            String category = transaction.getProductCategory();
                            double totalSales = transaction.getTotalAmount();
                            return new SalesPerCategory(transactionDate, category, totalSales);
                        }
                ).keyBy(SalesPerCategory::getCategory)
                .reduce((salesPerCategory, t1) -> {
                    salesPerCategory.setTotalSales(salesPerCategory.getTotalSales() + t1.getTotalSales());
                    return salesPerCategory;
                }).addSink(JdbcSink.sink(
                        "INSERT INTO sales_per_category(transaction_date, category, total_sales) " +
                                "VALUES (?, ?, ?) " +
                                "ON CONFLICT (transaction_date, category) DO UPDATE SET " +
                                "total_sales = EXCLUDED.total_sales " +
                                "WHERE sales_per_category.category = EXCLUDED.category " +
                                "AND sales_per_category.transaction_date = EXCLUDED.transaction_date",
                        (JdbcStatementBuilder<SalesPerCategory>) (preparedStatement, salesPerCategory) -> {
                            preparedStatement.setDate(1, new Date(System.currentTimeMillis()));
                            preparedStatement.setString(2, salesPerCategory.getCategory());
                            preparedStatement.setDouble(3, salesPerCategory.getTotalSales());
                        },
                        execOptions,
                        connOptions
                )).name("Insert into sales per category table");
        //map, aggregate, reduce then insert into sales_per_day table
        transactionStream.map(
                        transaction -> {
                            Date transactionDate = new Date(System.currentTimeMillis());
                            double totalSales = transaction.getTotalAmount();
                            return new SalesPerDay(transactionDate, totalSales);
                        }
                ).keyBy(SalesPerDay::getTransactionDate)
                .reduce((salesPerDay, t1) -> {
                    salesPerDay.setTotalSales(salesPerDay.getTotalSales() + t1.getTotalSales());
                    return salesPerDay;
                }).addSink(JdbcSink.sink(
                        "INSERT INTO sales_per_day(transaction_date, total_sales) " +
                                "VALUES (?,?) " +
                                "ON CONFLICT (transaction_date) DO UPDATE SET " +
                                "total_sales = EXCLUDED.total_sales " +
                                "WHERE sales_per_day.transaction_date = EXCLUDED.transaction_date",
                        (JdbcStatementBuilder<SalesPerDay>) (preparedStatement, salesPerDay) -> {
                            preparedStatement.setDate(1, new Date(System.currentTimeMillis()));
                            preparedStatement.setDouble(2, salesPerDay.getTotalSales());
                        },
                        execOptions,
                        connOptions
                )).name("Insert into sales per day table");
        //map, aggregate, reduce then insert into sales_per_month table
        transactionStream.map(
                        transaction -> {
                            Date transactionDate = new Date(System.currentTimeMillis());
                            int year = transactionDate.toLocalDate().getYear();
                            int month = transactionDate.toLocalDate().getMonth().getValue();
                            double totalSales = transaction.getTotalAmount();
                            return new SalesPerMonth(year, month, totalSales);
                        }
                ).keyBy(SalesPerMonth::getMonth)
                .reduce((salesPerMonth, t1) -> {
                    salesPerMonth.setTotalSales(salesPerMonth.getTotalSales() + t1.getTotalSales());
                    return salesPerMonth;
                }).addSink(JdbcSink.sink(
                        "INSERT INTO sales_per_month(year, month, total_sales) " +
                                "VALUES (?,?,?) " +
                                "ON CONFLICT (year, month) DO UPDATE SET " +
                                "total_sales = EXCLUDED.total_sales " +
                                "WHERE sales_per_month.year = EXCLUDED.year " +
                                "AND sales_per_month.month = EXCLUDED.month ",
                        (JdbcStatementBuilder<SalesPerMonth>) (preparedStatement, salesPerMonth) -> {
                            preparedStatement.setInt(1, salesPerMonth.getYear());
                            preparedStatement.setInt(2, salesPerMonth.getMonth());
                            preparedStatement.setDouble(3, salesPerMonth.getTotalSales());
                        },
                        execOptions,
                        connOptions
                )).name("Insert into sales per month table");

       transactionStream.sinkTo(
                new Elasticsearch7SinkBuilder<Transaction>()
                        .setHosts(new HttpHost("localhost" , 9200 ,"http"))
                        .setBulkFlushInterval(10000) //ref
                        .setEmitter((transaction, runtimeContext, requestIndexer) -> {
                            String json = convertTransactionToJson(transaction);

                            IndexRequest indexRequest = Requests.indexRequest()
                                    .index("transactions")
                                    .id(transaction.getTransactionId())
                                    .source(json, XContentType.JSON);
                            requestIndexer.add(indexRequest);
                        }).build()
        ).name("Elasticsearch");


            */
        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS transactions (" +
                        "transaction_id VARCHAR(255) PRIMARY KEY, " +
                        "product_id VARCHAR(255), " +
                        "product_name VARCHAR(255), " +
                        "product_category VARCHAR(255), " +
                        "product_price DOUBLE PRECISION, " +
                        "product_quantity INTEGER, " +
                        "product_brand VARCHAR(255), " +
                        "total_amount DOUBLE PRECISION, " +
                        "currency VARCHAR(255), " +
                        "customer_id VARCHAR(255), " +
                        "transaction_date TIMESTAMP, " +
                        "payment_method VARCHAR(255), " +
                        "customer_email VARCHAR(255), " +
                        "shipping_street VARCHAR(255), " +
                        "shipping_city VARCHAR(255), " +
                        "shipping_state VARCHAR(255), " +
                        "shipping_zipcode VARCHAR(50), " +
                        "shipping_country VARCHAR(100), " +
                        "shipping_cost DOUBLE PRECISION, " +
                        "tax_amount DOUBLE PRECISION, " +
                        "tax_rate DOUBLE PRECISION, " +
                        "discount_percentage INTEGER, " +
                        "discount_amount DOUBLE PRECISION, " +
                        "final_amount DOUBLE PRECISION, " +
                        "order_status VARCHAR(50), " +
                        "shipping_method VARCHAR(50), " +
                        "discount_code VARCHAR(50), " +
                        "is_gift BOOLEAN, " +
                        "gift_message TEXT, " +
                        "customer_ip_address VARCHAR(50), " +
                        "device_type VARCHAR(50), " +
                        "browser VARCHAR(50), " +
                        "return_eligible BOOLEAN, " +
                        "warranty_months INTEGER" +
                        ")",
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {},
                execOptions,
                connOptions
        )).name("Create Transactions Table Sink");

        //create sales_per_category table sink
        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS sales_per_category (" +
                        "transaction_date DATE, " +
                        "category VARCHAR(255), " +
                        "total_sales DOUBLE PRECISION, " +
                        "PRIMARY KEY (transaction_date, category)" +
                        ")",
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {

                },
                execOptions,
                connOptions
        )).name("Create Sales Per Category Table");

        //create sales_per_day table sink
        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS sales_per_day (" +
                        "transaction_date DATE PRIMARY KEY, " +
                        "total_sales DOUBLE PRECISION " +
                        ")",
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {

                },
                execOptions,
                connOptions
        )).name("Create Sales Per Day Table");

        //create sales_per_month table sink
        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS sales_per_month (" +
                        "year INTEGER, " +
                        "month INTEGER, " +
                        "total_sales DOUBLE PRECISION, " +
                        "PRIMARY KEY (year, month)" +
                        ")",
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {

                },
                execOptions,
                connOptions
        )).name("Create Sales Per Month Table");

        // Create sales_per_device table
        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS sales_per_device (" +
                        "transaction_date DATE, " +
                        "device_type VARCHAR(50), " +
                        "total_sales DOUBLE PRECISION, " +
                        "transaction_count INTEGER, " +
                        "PRIMARY KEY (transaction_date, device_type)" +
                        ")",
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {},
                execOptions,
                connOptions
        )).name("Create Sales Per Device Table");

// Create discount_analysis table
        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS discount_analysis (" +
                        "transaction_date DATE, " +
                        "discount_percentage INTEGER, " +
                        "total_discounts DOUBLE PRECISION, " +
                        "usage_count INTEGER, " +
                        "PRIMARY KEY (transaction_date, discount_percentage)" +
                        ")",
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {},
                execOptions,
                connOptions
        )).name("Create Discount Analysis Table");

// Create shipping_analysis table
        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS shipping_analysis (" +
                        "transaction_date DATE, " +
                        "shipping_method VARCHAR(50), " +
                        "total_shipping_revenue DOUBLE PRECISION, " +
                        "shipments_count INTEGER, " +
                        "PRIMARY KEY (transaction_date, shipping_method)" +
                        ")",
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {},
                execOptions,
                connOptions
        )).name("Create Shipping Analysis Table");

// Create order_status_metrics table
        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS order_status_metrics (" +
                        "transaction_date DATE, " +
                        "order_status VARCHAR(50), " +
                        "order_count INTEGER, " +
                        "total_value DOUBLE PRECISION, " +
                        "PRIMARY KEY (transaction_date, order_status)" +
                        ")",
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {},
                execOptions,
                connOptions
        )).name("Create Order Status Metrics Table");

// Create gift_order_metrics table
        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS gift_order_metrics (" +
                        "transaction_date DATE PRIMARY KEY, " +
                        "gift_orders_count INTEGER, " +
                        "regular_orders_count INTEGER, " +
                        "gift_orders_value DOUBLE PRECISION" +
                        ")",
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {},
                execOptions,
                connOptions
        )).name("Create Gift Order Metrics Table");

// Create category_performance table
        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS category_performance (" +
                        "transaction_date DATE, " +
                        "category VARCHAR(255), " +
                        "total_revenue DOUBLE PRECISION, " +
                        "items_sold INTEGER, " +
                        "avg_order_value DOUBLE PRECISION, " +
                        "PRIMARY KEY (transaction_date, category)" +
                        ")",
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {},
                execOptions,
                connOptions
        )).name("Create Category Performance Table");

        // Insert into table sink
        transactionStream.addSink(JdbcSink.sink(
                "INSERT INTO transactions(transaction_id, product_id, product_name, product_category, product_price, " +
                        "product_quantity, product_brand, total_amount, currency, customer_id, transaction_date, payment_method, " +
                        "customer_email, shipping_street, shipping_city, shipping_state, shipping_zipcode, shipping_country, " +
                        "shipping_cost, tax_amount, tax_rate, discount_percentage, discount_amount, final_amount, " +
                        "order_status, shipping_method, discount_code, is_gift, gift_message, customer_ip_address, " +
                        "device_type, browser, return_eligible, warranty_months) " +
                        "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (transaction_id) DO UPDATE SET " +
                        "product_id = EXCLUDED.product_id, " +
                        "product_name = EXCLUDED.product_name, " +
                        "product_category = EXCLUDED.product_category, " +
                        "product_price = EXCLUDED.product_price, " +
                        "product_quantity = EXCLUDED.product_quantity, " +
                        "product_brand = EXCLUDED.product_brand, " +
                        "total_amount = EXCLUDED.total_amount, " +
                        "currency = EXCLUDED.currency, " +
                        "customer_id = EXCLUDED.customer_id, " +
                        "transaction_date = EXCLUDED.transaction_date, " +
                        "payment_method = EXCLUDED.payment_method, " +
                        "customer_email = EXCLUDED.customer_email, " +
                        "shipping_street = EXCLUDED.shipping_street, " +
                        "shipping_city = EXCLUDED.shipping_city, " +
                        "shipping_state = EXCLUDED.shipping_state, " +
                        "shipping_zipcode = EXCLUDED.shipping_zipcode, " +
                        "shipping_country = EXCLUDED.shipping_country, " +
                        "shipping_cost = EXCLUDED.shipping_cost, " +
                        "tax_amount = EXCLUDED.tax_amount, " +
                        "tax_rate = EXCLUDED.tax_rate, " +
                        "discount_percentage = EXCLUDED.discount_percentage, " +
                        "discount_amount = EXCLUDED.discount_amount, " +
                        "final_amount = EXCLUDED.final_amount, " +
                        "order_status = EXCLUDED.order_status, " +
                        "shipping_method = EXCLUDED.shipping_method, " +
                        "discount_code = EXCLUDED.discount_code, " +
                        "is_gift = EXCLUDED.is_gift, " +
                        "gift_message = EXCLUDED.gift_message, " +
                        "customer_ip_address = EXCLUDED.customer_ip_address, " +
                        "device_type = EXCLUDED.device_type, " +
                        "browser = EXCLUDED.browser, " +
                        "return_eligible = EXCLUDED.return_eligible, " +
                        "warranty_months = EXCLUDED.warranty_months",
                (JdbcStatementBuilder<Transaction>) (preparedStatement, t) -> {
                    preparedStatement.setString(1, t.getTransactionId());
                    preparedStatement.setString(2, t.getProductId());
                    preparedStatement.setString(3, t.getProductName());
                    preparedStatement.setString(4, t.getProductCategory());
                    preparedStatement.setDouble(5, t.getProductPrice());
                    preparedStatement.setInt(6, t.getProductQuantity());
                    preparedStatement.setString(7, t.getProductBrand());
                    preparedStatement.setDouble(8, t.getTotalAmount());
                    preparedStatement.setString(9, t.getCurrency());
                    preparedStatement.setString(10, t.getCustomerId());
                    preparedStatement.setTimestamp(11, t.getTransactionDate());
                    preparedStatement.setString(12, t.getPaymentMethod());
                    preparedStatement.setString(13, t.getCustomerEmail());
                    preparedStatement.setString(14, t.getShippingAddress() != null ? t.getShippingAddress().getStreet() : null);
                    preparedStatement.setString(15, t.getShippingAddress() != null ? t.getShippingAddress().getCity() : null);
                    preparedStatement.setString(16, t.getShippingAddress() != null ? t.getShippingAddress().getState() : null);
                    preparedStatement.setString(17, t.getShippingAddress() != null ? t.getShippingAddress().getZipCode() : null);
                    preparedStatement.setString(18, t.getShippingAddress() != null ? t.getShippingAddress().getCountry() : null);
                    preparedStatement.setDouble(19, t.getShippingCost());
                    preparedStatement.setDouble(20, t.getTaxAmount());
                    preparedStatement.setDouble(21, t.getTaxRate());
                    preparedStatement.setInt(22, t.getDiscountPercentage());
                    preparedStatement.setDouble(23, t.getDiscountAmount());
                    preparedStatement.setDouble(24, t.getFinalAmount());
                    preparedStatement.setString(25, t.getOrderStatus());
                    preparedStatement.setString(26, t.getShippingMethod());
                    preparedStatement.setString(27, t.getDiscountCode());
                    preparedStatement.setBoolean(28, t.getIsGift() != null ? t.getIsGift() : false);
                    preparedStatement.setString(29, t.getGiftMessage());
                    preparedStatement.setString(30, t.getCustomerIpAddress());
                    preparedStatement.setString(31, t.getDeviceType());
                    preparedStatement.setString(32, t.getBrowser());
                    preparedStatement.setBoolean(33, t.getReturnEligible() != null ? t.getReturnEligible() : false);
                    preparedStatement.setInt(34, t.getWarrantyMonths());
                },
                execOptions,
                connOptions
        )).name("Insert into transactions table sink");


        // Aggregate: Sales per Device
        transactionStream.map(transaction -> {
                    Date transactionDate = new Date(System.currentTimeMillis());
                    String deviceType = transaction.getDeviceType();
                    double totalSales = transaction.getFinalAmount();
                    return new SalesPerDevice(transactionDate, deviceType, totalSales, 1);
                }).keyBy(SalesPerDevice::getDeviceType)
                .reduce((current, incoming) -> {
                    current.setTotalSales(current.getTotalSales() + incoming.getTotalSales());
                    current.setTransactionCount(current.getTransactionCount() + incoming.getTransactionCount());
                    return current;
                }).addSink(JdbcSink.sink(
                        "INSERT INTO sales_per_device(transaction_date, device_type, total_sales, transaction_count) " +
                                "VALUES (?, ?, ?, ?) " +
                                "ON CONFLICT (transaction_date, device_type) DO UPDATE SET " +
                                "total_sales = EXCLUDED.total_sales, " +
                                "transaction_count = EXCLUDED.transaction_count",
                        (JdbcStatementBuilder<SalesPerDevice>) (preparedStatement, sales) -> {
                            preparedStatement.setDate(1, sales.getTransactionDate());
                            preparedStatement.setString(2, sales.getDeviceType());
                            preparedStatement.setDouble(3, sales.getTotalSales());
                            preparedStatement.setInt(4, sales.getTransactionCount());
                        },
                        execOptions,
                        connOptions
                )).name("Insert into sales per device table");

// Aggregate: Discount Analysis
        transactionStream
                .filter(transaction -> transaction.getDiscountPercentage() > 0)
                .map(transaction -> {
                    Date transactionDate = new Date(System.currentTimeMillis());
                    int discountPercentage = transaction.getDiscountPercentage();
                    double discountAmount = transaction.getDiscountAmount();
                    return new DiscountAnalysis(transactionDate, discountPercentage, discountAmount, 1);
                }).keyBy(DiscountAnalysis::getDiscountPercentage)
                .reduce((current, incoming) -> {
                    current.setTotalDiscounts(current.getTotalDiscounts() + incoming.getTotalDiscounts());
                    current.setUsageCount(current.getUsageCount() + incoming.getUsageCount());
                    return current;
                }).addSink(JdbcSink.sink(
                        "INSERT INTO discount_analysis(transaction_date, discount_percentage, total_discounts, usage_count) " +
                                "VALUES (?, ?, ?, ?) " +
                                "ON CONFLICT (transaction_date, discount_percentage) DO UPDATE SET " +
                                "total_discounts = EXCLUDED.total_discounts, " +
                                "usage_count = EXCLUDED.usage_count",
                        (JdbcStatementBuilder<DiscountAnalysis>) (preparedStatement, analysis) -> {
                            preparedStatement.setDate(1, analysis.getTransactionDate());
                            preparedStatement.setInt(2, analysis.getDiscountPercentage());
                            preparedStatement.setDouble(3, analysis.getTotalDiscounts());
                            preparedStatement.setInt(4, analysis.getUsageCount());
                        },
                        execOptions,
                        connOptions
                )).name("Insert into discount analysis table");

// Aggregate: Shipping Analysis
        transactionStream.map(transaction -> {
                    Date transactionDate = new Date(System.currentTimeMillis());
                    String shippingMethod = transaction.getShippingMethod();
                    double shippingRevenue = transaction.getShippingCost();
                    return new ShippingAnalysis(transactionDate, shippingMethod, shippingRevenue, 1);
                }).keyBy(ShippingAnalysis::getShippingMethod)
                .reduce((current, incoming) -> {
                    current.setTotalShippingRevenue(current.getTotalShippingRevenue() + incoming.getTotalShippingRevenue());
                    current.setShipmentsCount(current.getShipmentsCount() + incoming.getShipmentsCount());
                    return current;
                }).addSink(JdbcSink.sink(
                        "INSERT INTO shipping_analysis(transaction_date, shipping_method, total_shipping_revenue, shipments_count) " +
                                "VALUES (?, ?, ?, ?) " +
                                "ON CONFLICT (transaction_date, shipping_method) DO UPDATE SET " +
                                "total_shipping_revenue = EXCLUDED.total_shipping_revenue, " +
                                "shipments_count = EXCLUDED.shipments_count",
                        (JdbcStatementBuilder<ShippingAnalysis>) (preparedStatement, analysis) -> {
                            preparedStatement.setDate(1, analysis.getTransactionDate());
                            preparedStatement.setString(2, analysis.getShippingMethod());
                            preparedStatement.setDouble(3, analysis.getTotalShippingRevenue());
                            preparedStatement.setInt(4, analysis.getShipmentsCount());
                        },
                        execOptions,
                        connOptions
                )).name("Insert into shipping analysis table");

// Aggregate: Order Status Metrics
        transactionStream.map(transaction -> {
                    Date transactionDate = new Date(System.currentTimeMillis());
                    String orderStatus = transaction.getOrderStatus();
                    double totalValue = transaction.getFinalAmount();
                    return new OrderStatusMetrics(transactionDate, orderStatus, 1, totalValue);
                }).keyBy(OrderStatusMetrics::getOrderStatus)
                .reduce((current, incoming) -> {
                    current.setOrderCount(current.getOrderCount() + incoming.getOrderCount());
                    current.setTotalValue(current.getTotalValue() + incoming.getTotalValue());
                    return current;
                }).addSink(JdbcSink.sink(
                        "INSERT INTO order_status_metrics(transaction_date, order_status, order_count, total_value) " +
                                "VALUES (?, ?, ?, ?) " +
                                "ON CONFLICT (transaction_date, order_status) DO UPDATE SET " +
                                "order_count = EXCLUDED.order_count, " +
                                "total_value = EXCLUDED.total_value",
                        (JdbcStatementBuilder<OrderStatusMetrics>) (preparedStatement, metrics) -> {
                            preparedStatement.setDate(1, metrics.getTransactionDate());
                            preparedStatement.setString(2, metrics.getOrderStatus());
                            preparedStatement.setInt(3, metrics.getOrderCount());
                            preparedStatement.setDouble(4, metrics.getTotalValue());
                        },
                        execOptions,
                        connOptions
                )).name("Insert into order status metrics table");

// Aggregate: Gift Order Metrics
        transactionStream.map(transaction -> {
                    Date transactionDate = new Date(System.currentTimeMillis());
                    boolean isGift = transaction.getIsGift() != null && transaction.getIsGift();
                    int giftCount = isGift ? 1 : 0;
                    int regularCount = isGift ? 0 : 1;
                    double giftValue = isGift ? transaction.getFinalAmount() : 0.0;
                    return new GiftOrderMetrics(transactionDate, giftCount, regularCount, giftValue);
                }).keyBy(GiftOrderMetrics::getTransactionDate)
                .reduce((current, incoming) -> {
                    current.setGiftOrdersCount(current.getGiftOrdersCount() + incoming.getGiftOrdersCount());
                    current.setRegularOrdersCount(current.getRegularOrdersCount() + incoming.getRegularOrdersCount());
                    current.setGiftOrdersValue(current.getGiftOrdersValue() + incoming.getGiftOrdersValue());
                    return current;
                }).addSink(JdbcSink.sink(
                        "INSERT INTO gift_order_metrics(transaction_date, gift_orders_count, regular_orders_count, gift_orders_value) " +
                                "VALUES (?, ?, ?, ?) " +
                                "ON CONFLICT (transaction_date) DO UPDATE SET " +
                                "gift_orders_count = EXCLUDED.gift_orders_count, " +
                                "regular_orders_count = EXCLUDED.regular_orders_count, " +
                                "gift_orders_value = EXCLUDED.gift_orders_value",
                        (JdbcStatementBuilder<GiftOrderMetrics>) (preparedStatement, metrics) -> {
                            preparedStatement.setDate(1, metrics.getTransactionDate());
                            preparedStatement.setInt(2, metrics.getGiftOrdersCount());
                            preparedStatement.setInt(3, metrics.getRegularOrdersCount());
                            preparedStatement.setDouble(4, metrics.getGiftOrdersValue());
                        },
                        execOptions,
                        connOptions
                )).name("Insert into gift order metrics table");

// Aggregate: Enhanced Category Performance
        transactionStream.map(transaction -> {
                    Date transactionDate = new Date(System.currentTimeMillis());
                    String category = transaction.getProductCategory();
                    double revenue = transaction.getFinalAmount();
                    int itemsSold = transaction.getProductQuantity();
                    return new CategoryPerformance(transactionDate, category, revenue, itemsSold, revenue);
                }).keyBy(CategoryPerformance::getCategory)
                .reduce((current, incoming) -> {
                    current.setTotalRevenue(current.getTotalRevenue() + incoming.getTotalRevenue());
                    current.setItemsSold(current.getItemsSold() + incoming.getItemsSold());
                    // Calculate average order value
                    current.setAvgOrderValue(current.getTotalRevenue() / (current.getItemsSold() > 0 ? current.getItemsSold() : 1));
                    return current;
                }).addSink(JdbcSink.sink(
                        "INSERT INTO category_performance(transaction_date, category, total_revenue, items_sold, avg_order_value) " +
                                "VALUES (?, ?, ?, ?, ?) " +
                                "ON CONFLICT (transaction_date, category) DO UPDATE SET " +
                                "total_revenue = EXCLUDED.total_revenue, " +
                                "items_sold = EXCLUDED.items_sold, " +
                                "avg_order_value = EXCLUDED.avg_order_value",
                        (JdbcStatementBuilder<CategoryPerformance>) (preparedStatement, perf) -> {
                            preparedStatement.setDate(1, perf.getTransactionDate());
                            preparedStatement.setString(2, perf.getCategory());
                            preparedStatement.setDouble(3, perf.getTotalRevenue());
                            preparedStatement.setInt(4, perf.getItemsSold());
                            preparedStatement.setDouble(5, perf.getAvgOrderValue());
                        },
                        execOptions,
                        connOptions
                )).name("Insert into category performance table");

        transactionStream.sinkTo(
                new Elasticsearch7SinkBuilder<Transaction>()
                        .setHosts(new HttpHost("localhost" , 9200 ,"http"))
                        .setBulkFlushInterval(10000) //ref
                        .setEmitter((transaction, runtimeContext, requestIndexer) -> {
                            String json = convertTransactionToJson(transaction);

                            IndexRequest indexRequest = Requests.indexRequest()
                                    .index("transactions")
                                    .id(transaction.getTransactionId())
                                    .source(json, XContentType.JSON);
                            requestIndexer.add(indexRequest);
                        }).build()
        ).name("Elasticsearch");

            //Execute flink program, computation begins
		env.execute("Flink Ecommerce Realtime Streaming");
	}
}









