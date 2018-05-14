PocketETL
=========
Move and transform data anywhere with just a few lines of code. Fluent Java 8 interface allows even complex ETL jobs to be described in a concise and intuitive semantic; no XML, JSON or DSLs required.

Usage
-----
#### Write customer data from an SQL database to S3 as a CSV file
```java
void loadCustomersToS3(DataSource customerDb, DateTime lastActivityDate) {
    Extractor<Customer> dbCustomerExtractor = SqlExtractor.of(customerDb, "SELECT * FROM customer WHERE last_activity > #last_activity_date", Customer.class);
                                                          .withSqlParameters(ImmutableMap.of("last_activity_date", lastActivityDate));
                                                          
    // We pass in a lambda to create the CsvStringSerializer so that a new header row gets written with every new part file in S3
    Loader<Customer> s3CustomerLoader = S3FastLoader.supplierOf("customer-bucket", () -> CsvStringSerializer.of(Customer.class))
                                                    .get();
    
    EtlStream.extract(dbCustomerExtractor)
             .load(Customer.class, s3CustomerLoader)
             .run();
}
```
The S3FastLoader will split the output into multiple part files if the output is bigger than the configured maximum part-file size (which is 128 MiB by default).
#### Read JSON messages from an SQS queue and call a remote API
```java
void processMessages(String queueUrl) {
    EtlStream.extract(SqsExtractor.of(queueUrl, JSONStringMapper.of(InviteRequest.class)))
             .then(load(InviteRequest.class, inviteRequest -> invitationServiceClient.invite(inviteRequest.getCustomerEmail())).withThreads(10))
             .run();
}
```
The call to invite on InvitiationService is slow, so assigning multiple threads (in this case 10) will increase the TPS to that service 10x thus allowing the job to complete faster.

#### Load customer IDs from S3, lookup e-mail address, store results in Redshift
```java
void storeCustomerEmails(DataSource redshiftDb) {
    EtlStream.extract(S3BufferedExtractor.supplierOf("data-bucket", "customers.csv", CsvInputStreamMapper.of(CustomerKey.class)))
             .transform(CustomerKey.class, MapTransformer.of(customerKey -> new Customer(customerKey.getId(), addressService.getEmail(customerKey.getId()))))
             .load(Customer.class, RedshiftBulkLoader.supplierOf(Customer.class)
                                     .withS3Bucket("data-staging")
                                     .withS3Region("us-east-1")
                                     .withRedshiftDataSource(redshiftDb)
                                     .withRedshiftTableName("marketing.customer")
                                     .withRedshiftColumnNames("customer_id", "contact_email")
                                     .withRedshiftIndexColumnNames("customer_id")
                                     .withRedshiftIamRole("arn:aws:iam::1234567890:role/redshift-iam-role")
                                     .get())
             .run();         
}
```

#### Merge multiple EtlStreams with different schemas into a single loader
```java
EtlStream getInternalOrders() {
    return EtlStream.extract(SqlExtractor.of(myDataSource, "SELECT * FROM orders", InternalOrder.class));
}

EtlStream getVendorOrders() {
    return EtlStream.extract(S3BufferedExtractor.supplierOf("vendor-bucket", "orders.csv", CsvInputStreamMapper.of(VendorOrder.class)).get());
}

void processOrders() {
    EtlStream.combine(getInternalOrders(), getVendorOrders())
             .then(load(Order.class, serviceClient::processOrder).withThreads(10))
             .run();
}
```
Data flows through an ETLStream as a key/value map, there is no type associated with the stream itself. That means that your steps can operate on different data classes and it does not matter as long as the names of the fields match up. This even allows you to tunnel fields through steps that don't need to operate on them.

#### Your use-case
These are many more possibilities and configurations, these are just a few examples intended to illustrate the flexibility and power of PocketETL. You can either use the library of useful extractors, transformer and loader classes that are supplied, or create your own as needed.

Reasons to use PocketETL
------------------------
1. You want something up and running in minutes. No special hosting or configuration required, just import the library and construct and execute an EtlStream in a few lines of code.
2. The data you want to move is hosted in AWS services. PocketETL is designed for the cloud: S3, SQS and Redshift support out of the box. Can be embedded in a lambda function.
3. You want something that will continue to be useful beyond the few things you need it for right now. PocketETL has an extensible interface, use lambda functions or bring your own extract, transform and load classes.
4. You need to process large batches of things and doing it in series is not fast enough. PocketETL uses configurable parallelism to give your data pipeline a huge speed boost without any fuss.

Reasons not to use PocketETL
----------------------------
1. You want to embed it in an application that does not run on a JVM.
2. Your stream needs transactional guarantees at a per-record level. Conceptually there is no support for re-driving or rolling-back the transformation or loading of individual records.
3. You have parallel scaling needs beyond a single host. (eg: Spark on EMR)
4. Single-threaded throughput speed is critically important down to microseconds, PocketETL makes several efficiency tradeoffs to increase usability and flexibility.

