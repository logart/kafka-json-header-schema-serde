# Kafka Json Header Schema Serde

This library contains custom json schema aware serde to be used with parquet serializer.
The main objective is to leave payload as is, without adding confluent envelope, but rather to add schema information to
the headers.