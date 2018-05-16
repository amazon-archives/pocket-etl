PocketETL
=========
Extensible Java library that orchestrates batched ETL (extract, transform and load) of data between services using native fluent Java to express your pipeline.

Usage
----
Short form:
```java
EtlStream.extract(myExtractor)                        // Provide one or more extractors to extract objects and serializes them onto the ETL stream
         .transform(MyClass.class, myTransformer)     // Provide one or more transformers that transform objects deserialized into MyClass
         .load(MyLoadClass.class, myLoader)           // Provide a loader to load objects deserialized into MyLoadClass to their final destination
         .run();                                      // Execute the stream until it is exhausted
```
Long form (allows more stream customization):
```java
EtlStream.from(extract(myExtractor).withName("OrderExtraction"))          // Give this extraction step a name that will appear in logs and metrics
         .then(transform(MyClass.class, myTransformer).withThreads(10))            // Split this transformation step into 10 parallel transformers
         .then(load(MyClass.class, myLoader).withObjectLogger(myLogger::logMyClass))      // Provide a custom logger for objects that fail to load
         .run();
```
The two different expression forms can be mixed and combined in the same stream. EtlStream objects and the objects used to describe the stages in the long form expressive syntax are immutable and therefore can be split and safely reused as components in different streams.

Examples
-----
#### Read JSON serialized messages from an SQS queue and call a remote API
```java
void processMessages(String queueUrl) {
  EtlStream.extract(SqsExtractor.of(queueUrl, JSONStringMapper.of(InviteRequest.class)))
           .then(load(InviteRequest.class,
                      inviteRequest -> invitationServiceClient.invite(inviteRequest.getCustomerEmail()))
                   .withThreads(10))
           .run();
}
```
The call to invite on InvitationService is slow, so assigning multiple threads (in this case 10) will increase the TPS to that service 10x thus allowing the job to complete faster.
#### Write customer data from a relational database to S3 as multiple CSV part files
```java
void loadCustomersToS3(DataSource customerDb, DateTime lastActivityDate) {
  Extractor<Customer> dbCustomerExtractor =
    SqlExtractor.of(customerDb, "SELECT * FROM customer WHERE last_activity > #last_activity_date", Customer.class);
                .withSqlParameters(ImmutableMap.of("last_activity_date", lastActivityDate));

  // We pass in a function to create the CsvStringSerializer so that a new header row gets written with every new part file in S3
  Loader<Customer> s3CustomerLoader =
    S3FastLoader.supplierOf("customer-bucket", () -> CsvStringSerializer.of(Customer.class))
                .get();

  // Create and execute the Pocket-ETL stream
  EtlStream.extract(dbCustomerExtractor)
           .load(Customer.class, s3CustomerLoader)
           .run();
}
```
The S3FastLoader will split the output into multiple part files if the output is bigger than the configured maximum part file size (which is 128 MiB by default).
#### Load customer IDs from S3, lookup e-mail address, store results in Redshift
```java
void storeCustomerEmails(DataSource redshiftDb) {
  Extractor<CustomerKey> extractCustomerIds = S3BufferedExtractor.supplierOf("data-bucket",
                                                                             "customers.csv",
                                                                             CsvInputStreamMapper.of(CustomerKey.class))
                                                                 .get();

  Transformer<CustomerKey,Customer> lookupCustomerFromService = MapTransformer.of(
    customerKey -> new Customer(customerKey.getId(), contactInfoService.getEmail(customerKey.getId())));

  EtlStream.extract(extractCustomerIds)
           .transform(CustomerKey.class, lookupCustomerFromService)
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
Data flows through an ETLStream as a key/value map, there is no type associated with the stream itself. This means that
your steps can operate on different data classes as long as the names of the fields match up. This allows you to tunnel
attributes through steps that don't need to operate on them.

#### Your use-case
These are many more possibilities and configurations, these are just a few examples intended to illustrate the
flexibility of the library. A selection of extractors, transformers and loaders are provided to solve common use-cases
involving moving data to and from AWS services and databases, however the library is designed to be easily extended by
implementing a simple interface to provide your own integrations for these ETL components.

Bundled adapters
---------
PocketETL comes with a selection of useful adapters to help you build streams immediately. The interface for each of
these adapter types (Extractor, Transformer and Loader) are public and designed to be easy to implement. There are just
three methods on each: open, close and 'do-the-thing' (what 'the thing' is depends on the interface). Open and close are
optional and do not have to be implemented.

#### Extractors
Extractors produce objects at the head of a stream. Typically they read from some kind of persistent storage. They continue producing objects until the source of the data has been exhausted.

Name | Description
:---|:---
InputStreamExtractor | Maps an input stream into objects and extracts them. An input stream mapper that can read CSV files is provided.
IterableExtractor | Extracts objects from any Java object that implements Iterable.
IteratorExtractor | Extracts objects from any Java object that implements Iterator.
S3BufferedExtractor | Reads a complete file from AWS S3 into memory and then extracts objects from it as an input stream. An input stream mapper that can read CSV files is provided.
SqlExtractor | Executes and extracts objects based on an SQL query against a provided JDBC DataSource.
SqsExtractor | Polls and extracts objects from an AWS SQS Queue. A deserializer that can read JSON strings is provided.

#### Transformers
Transformers take a single data object from the stream and either transform it into a different object, remove the object from the stream, or fan-out into multiple objects.

Name | Description
:---|:---
FilterTransformer | Filters objects based on a custom Lookup dataset and a predicate. An implementation of a cached Lookup is provided that can be used as a Loader in a parallel stream to populate the cache.
MapTransformer | The simple reference Transformer that takes a single object and maps it into another single object.

#### Loaders
Loaders sit at the tail of a stream, take objects from the stream and load them to a final destination. Typically the final destination will be some kind of persistent data store or stream.

Name | Description
:---|:---
DynamoDbLoader | Loads records into an AWS DynamoDB table using a provided function to generate the hash key from each record.
MetricsLoader | Extracts all the numeric values of an object and passes them to a provided metrics logging object.
ParallelLoader | Meta-loader that generates an instance of a different loader for every new thread it sees, allowing non-threadsafe loaders to be used in parallel loader configurations without having to block on each other (eg: loaders that write serial streams).
RedshiftBulkLoader | Loads all records into an AWS Redshift database efficiently as a single batch (using COPY) by first staging the data in AWS S3.
S3FastLoader | Streams objects into files stored in AWS S3. Creates multiple files of a specified maximum part file size.

Reasons to use PocketETL
------------------------
1. You want something up and running in minutes. No special hosting or configuration required, just import the library and construct and execute an EtlStream in a few lines of code.
2. The data you want to move is hosted in AWS services. PocketETL is designed for the cloud: AWS services such as S3, SQS and Redshift are supported out of the box. ETL jobs can be embedded and run in a lambda function.
3. You want something that will continue to be useful beyond the few things you need it for right now. PocketETL has an extensible interface, use lambda functions or bring your own extract, transform and load implementations.
4. You need to process large batches of things and doing it in series is not fast enough. PocketETL uses configurable parallelism to give your data pipeline a huge speed boost without any fuss.

Reasons not to use PocketETL
----------------------------
1. You want to embed it in an application that does not run on a JVM.
2. Your stream needs transactional guarantees at a per-record level. Conceptually there is no support for re-driving or rolling-back the transformation or loading of individual records.
3. You have parallel scaling needs beyond a single host (eg: Apache Spark on AWS EMR).
4. Single-threaded throughput speed is critically important down to microseconds: PocketETL makes several efficiency tradeoffs to increase usability and flexibility.

## License

This library is licensed under the Apache 2.0 License.

## Feedback
* Give us feedback [here](https://github.com/awslabs/pocket-etl/issues).
* If you'd like to contribute a new adapter or bug fix, we'd love to see Github pull requests from you.