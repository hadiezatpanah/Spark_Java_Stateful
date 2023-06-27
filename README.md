# Sessionization - Spark Stateful Processing - flatMapGroupsWithState

## Introduction

This project presents a distributable solution based on Spark Java, aiming to connect start and end session events together in a stateful manner. Rather than relying on identical IDs for event connection, this solution leverages the sequence of events within the same category.
Finding comprehensive resources on Spark's flatMapGroupsWithState functionality can be challenging. Thus, this project serves as a valuable sample demonstrating the strength of stateful processing in Spark. By delving into the provided code and implementation details, experienced users can find this project beneficial in gaining a deeper understanding of effective utilization of stateful processing in Spark for their intricate data processing needs.


## Datasets

The schema of input data stream is a below:
| Column          | Description                                                |
| --------------- | ---------------------------------------------------------- |
| userID          | Represents the user ID                                     |
| sessionID       | Represents the session ID                                  |
| eventType       | Represents the type of event                               |
| timestamp       | Represents the timestamp in the format 'yyyy-MM-dd HH:mm:ss' |



## Requirements

The project aims to connect start and session events as a single event, utilizing a combination of `userID` and `sessionID` as the primary key. The sequence of events is used to connect related events within the same category. In cases where the end session of a user (`userID`, `sessionID`) is not present in the current batch of data, the state of these events is held as incomplete records, which can be completed in upcoming batches of data.

Since Spark operates in a microbatch fashion, an event may or may not be completed within the current batch. To handle incomplete events, the project utilizes `flatMapGroupWithState`functionality. This allows the project to hold the state of events that are not completed in the current batch.


# Solution
## Description

The project uses TypeSafe as a config handler. It has been implemented to be run in different environments (dev, test, production) and has different behavior based on it. There are three different environments in `application.conf` and one common application config that will be overridden by specific environmental configurations. For example, in the `SparkSession` part of `application`, the spark master parameter is set to `yarn`, but in the `dev` part, it is overridden to `local[*]`. 
With this in mind, in the development environment project will use MemoryStream as a streaming source, and random data will be generated and added to it to provide the input stream, while in the test environment, the input stream source is Kafka. Has several benefits to using MemoryStream as a streaming source for the development environment. MemoryStream simplifies the development and testing process by generating and processing streaming data within Spark, eliminating the need for external dependencies. MemoryStream enables rapid iteration and debugging as data ingestion and processing occur in-memory, providing near-instant feedback loops. It allows developers to have control and predictability over the data by generating custom datasets and simulating specific scenarios. MemoryStream reduces setup and maintenance overhead by eliminating the need for additional infrastructure components. It seamlessly integrates with Spark's ecosystem, leveraging its full streaming capabilities and rich set of APIs and functions. For further explanation, please refer to [this repo](https://github.com/hadiezatpanah/Spark_Java_MostValuableCustomers).

To handle incomplete events (events that are not completed in current batch processing), the project utilizes `flatMapGroupWithState`functionality. `flatMapGroupWithState` in Spark is a powerful feature for stateful stream processing. It enables you to maintain and update the state across batches, allowing for event correlation and custom state management. With its scalability and fault tolerance, it is ideal for real-time processing of large-scale data. By leveraging the sequence of events, it provides flexibility in aggregating and analyzing streaming data. `flatMapGroupWithState` empowers you to build complex, reliable, and scalable streaming applications in Spark.

The main algorithm of generating output based on the input batch of data will happen in the `SessionUpdate` class. The input events are classified into four types, Complete, ErrorNullStartTime, ErrorNullEndTime, and Incomplete. If an event gets completed in the current batch of data, it is a complete event. If we have more than one start event of the same class in a row, except the first one, other events will be considered an ErrorNullEndTime event. If we have more than one end event of the same class in a row, except the last one, other events will be considered as an ErrorNullStartTime event. If we have an end event without a corresponding start event in the state store, it will be considered an ErrorNullStartTime event. If a current event is a start event and no end event exists for it in the current batch of data, it will be pushed to the state store as an incomplete event, and the state of that key (`userID`, `sessionID`) will be updated accordingly. **It should be notified that SessionUpdate will be executed for each key separately**. Also, a timeout for the state is considered to remove the incomplete event from the state store after the timout. This also can be set in `application.conf` in the `Extract` part of the application config.


<p align="center">
  <img src = "Images/StandStill-Spark statefull approach.drawio.svg" width=100%>
</p>

## Deployment and Environment Configuration

The project is designed to be easily deployed and run in three different environments: development, test, and production.  To achieve this, the project incorporates the **TypeSafe** Config library as a configuration handler. It provides separate configurations for each environment (development, test, production), along with a common configuration `aaplication config` that can be overridden by the environment-specific configurations. By passing the appropriate application parameter (`dev`, `prod`, `test`), the project can be run in any of the three environments. the run time configuration has been set in `src/main/resources/Sessionization.run.xml`.

## Version Compatibility

Java| Spark|gradle     
--- | --- | ---
1.8.0_321| 3.3.0| 6.7


## Contributing
Contributions are welcome! If you have any ideas, suggestions, or bug fixes, feel free to submit a pull request.

## License
This project is licensed under the MIT License.

## Contact
For any inquiries or support, please contact `hadi.ezatpanah@gmail.com`.

This is just a template, so make sure to customize it with the appropriate details specific to your project.

## Author

👤 **Hadi Ezatpanah**

- Github: [@hadiezatpanah](https://github.com/hadiezatpanah)

## Version History
* 0.1
    * Initial Release
