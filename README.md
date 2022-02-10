# Mysql large resultset experiments

This project fills a containerized/temporary MySql database with a couple
million rows of data, and uses a variety of strategies to traverse the data.

## Sample usage

`sdk env && MAVEN_OPTS="-Xms256m -Xmx512m" mvn clean test`

