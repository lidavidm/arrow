.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

.. _flight-sql:

Arrow Flight SQL
================

Arrow Flight SQL is a protocol for clients and servers to communicate with SQL-like semantics over Arrow Flight.
While Arrow Flight provides basic request flows and data type definitions, it leaves several points up to the application.
This document describes request flows and Protobuf message definitions to enable clients to retrieve metadata about and execute SQL-like queries against compliant Arrow Flight services.

All RPC calls defined here can be found in Flight.proto.
All Protobuf definitions can be found in FlightSql.proto.

.. note:: This specification is still experimental and subject to change.

Metadata Commands
-----------------

Servers expose metadata about the Arrow format, SQL syntax/features, and available tables/resources over Flight's DoAction endpoint.
A client should call DoAction with an action type specified below.
If applicable, the action body should be the serialized form of the specified Protobuf message.
The server should respond with an appropriate number of Flight Result values, where the Result body is the serialized form of the specified Protobuf message.

Message fields which are specified as a "filter pattern" can contain ``%`` and ``_`` wildcards to match multiple values.
These wildcards function as in SQL LIKE clauses.

.. TODO Once FlightSQL is agreed upon, we can link each Protobuf
   message to the corresponding point in the source code to make it
   easier to cross-reference

+---------------+---------------------------------+--------------------------+---------------------------+-------------------+
| Action Type   | Description                     | Request Message          |  Result Message           | Number of results |
+===============+=================================+==========================+===========================+===================+
| GetSqlInfo    | Describe the server version and | N/A                      | ActionGetSqlInfoResult    | Exactly one       |
|               | supported SQL features.         |                          |                           |                   |
+---------------+---------------------------------+--------------------------+---------------------------+-------------------+
| GetCatalogs   | List available catalogs.        | ActionGetCatalogsRequest | ActionGetCatalogResult    | Zero or more      |
+---------------+---------------------------------+--------------------------+---------------------------+-------------------+
| GetSchemas    | List available schemas.         | ActionGetSchemasRequest  | ActionGetSchemasResult    | Zero or more      |
+---------------+---------------------------------+--------------------------+---------------------------+-------------------+
| GetTables     | List available tables.          | ActionGetTablesRequest   | ActionGetTablesResult     | Zero or more      |
+---------------+---------------------------------+--------------------------+---------------------------+-------------------+
| GetTableTypes | List table types.               | N/A                      | ActionGetTableTypesResult | Zero or more      |
+---------------+---------------------------------+--------------------------+---------------------------+-------------------+

.. TODO add sequence diagrams

Query Execution
---------------

Queries can be executed directly, or as prepared statements; they can also be executed either as result-returning statements, or as updates.

Transaction semantics are not currently defined.
All queries are treated as though they run in their own transaction, with an implementation-defined isolation level, and auto-commit enabled.

Statements which return more than one result set, e.g. semicolon-delimited queries, are not supported.

Request Flows
~~~~~~~~~~~~~

Query Statements
++++++++++++++++

This allows clients to execute ad-hoc queries which return tabular results.

To retrieve query results:

1. The client calls GetFlightInfo with a command-type FlightDescriptor.
   The descriptor command contains the serialization of the CommandStatementQuery message.
2. The server returns a FlightInfo with one or more FlightEndpoints, each with an implementation-defined Ticket payload.
   The FlightInfo may optionally be populated the Arrow schema of the returned data.
3. To retrieve the query results: for each returned endpoint, the client calls DoGet with the ticket of that endpoint.
   If there are multiple endpoints, the full result set is the concatentation of the indvididual result sets from each endpoint, in the order given by the server.

To instead retrieve the query schema:

1. The client calls GetSchema with a command-type FlightDescriptor.
   The descriptor command contains the serialization of the CommandStatementQuery message.
2. The server returns a SchemaResult with the serialized Arrow schema.

Insert/Update Statements
++++++++++++++++++++++++

This allows clients to execute ad-hoc queries which do not return tabular results (e.g. DDL statements, insert/update queries).

1. The client calls DoPut with a command-type FlightDescriptor.
   The descriptor command contains the serialization of the CommandStatementUpdate message.
   The client also supplies the schema of the parameter values, if needed.
2. During the DoPut call, the client supplies parameter values as batches of Arrow data.
   Each row is taken to be a set of parameter values and used to execute the query.
3. The server returns a PutResult message containing the serialization of a DoPutUpdateResult message.
   This contains the number of affected rows, which can be zero, or -1 if the number is unknown.

Prepared Statements
+++++++++++++++++++

This allows clients to create prepared statements for more efficient query execution.

To create a prepared statement:

1. The client calls DoAction with type equal to GetPreparedStatement and with body containing a serialized ActionGetPreparedStatementRequest.
2. The server returns a Result with body containing a serialized ActionGetPreparedStatementResult.

To close a prepared statement:

1. The client calls DoAction with type equal to ClosePreparedStatement and with body containing a serialized ActionClosePreparedStatementRequest.

Clients must close prepared statements after they are done using the statement.
Prepared statements may also time out after an implementation-defined duration.
It is an error to close a prepared statement while a query is ongoing.

To use a prepared statement to query values:

1. Optionally, to bind values to the statement:

   1. The client calls DoPut with a command-type FlightDescriptor.
      The descriptor command contains the serialization of a CommandPreparedStatementQuery message.
      The client_execution_handle is a client-chosen value.
      The prepared_statement_handle must come from a prior GetPreparedStatement call.
   2. The client supplies parameter values as batches of Arrow data.
      Each row is taken to be a set of parameter values.

2. The client calls GetFlightInfo with a command-type FlightDescriptor.
   The descriptor command contains the serialization of the CommandPreparedStatementQuery message.
   The client_execution_handle is a client-chosen value, consistent with a prior DoPut call if there are parameters.
   The prepared_statement_handle must come from a prior GetPreparedStatement call.
3. The server returns a FlightInfo with one or more FlightEndpoints, each with an implementation-defined Ticket payload.
   The FlightInfo may optionally be populated the Arrow schema of the returned data.
4. To retrieve the query results: for each returned endpoint, the client calls DoGet with the ticket of that endpoint.
   If there are multiple endpoints, the full result set is the concatentation of the indvididual result sets from each endpoint, in the order given by the server.

To use a prepared statement to insert/update values:

1. The client calls GetFlightInfo with a command-type FlightDescriptor.
   The descriptor command contains the serialization of the CommandPreparedStatementQuery message.
   The client_execution_handle is a client-chosen value.
   The prepared_statement_handle must come from a prior GetPreparedStatement call.
2. During the DoPut call, the client supplies parameter values as batches of Arrow data.
   Each row is taken to be a set of parameter values and used to execute the query.
3. The server returns a PutResult message containing the serialization of a DoPutUpdateResult message.
   This contains the number of affected rows, which can be zero, or -1 if the number is unknown.

client_execution_handle values may not be resued.
They need not be distinct between distinct connections, but also cannot be reused across distinct connections.
They need not be distinct between distinct prepared_statement_handle values.
It is an error to use a prepared statement that is parameterized, but for which parameter values have not been bound.

Error Handling
--------------

.. TODO Now that Flight supports custom error metadata, should we
   define our own set of error codes?

+-----------------------------------------+----------------------------------+--------------------+
|Error Case                               |Applicable RPC Calls              |Error Code          |
+=========================================+==================================+====================+
|Query syntax is unrecognized/invalid.    |- DoAction(GetPreparedStatement)  |INVALID_ARGUMENT    |
|                                         |- DoPut                           |                    |
|                                         |- GetFlightInfo                   |                    |
|                                         |- GetSchema                       |                    |
+-----------------------------------------+----------------------------------+--------------------+
|Ticket is unrecognized/invalid.          |- DoGet                           |INVALID_ARGUMENT    |
+-----------------------------------------+----------------------------------+--------------------+
|The prepared statement handle is         |- DoAction(ClosePreparedstatement)|NOT_FOUND           |
|unrecognized/invalid.                    |- DoPut                           |                    |
|                                         |- GetFlightInfo                   |                    |
|                                         |- GetSchema                       |                    |
+-----------------------------------------+----------------------------------+--------------------+
|Too many or too few parameter values     |- GetFlightInfo                   |INVALID_ARGUMENT    |
|have been bound to the given             |- DoPut                           |                    |
|client_execution_handle prior to         |                                  |                    |
|execution.                               |                                  |                    |
+-----------------------------------------+----------------------------------+--------------------+
|Prepared statement is in use and cannot  |- DoAction(ClosePreparedstatement)|INVALID_ARGUMENT    |
|be closed.                               |                                  |                    |
+-----------------------------------------+----------------------------------+--------------------+
|The client_execution_handle value was    |- DoPut                           |ALREADY_EXISTS      |
|already used for the given               |- GetFlightInfo                   |                    |
|prepared_statement_handle value.         |                                  |                    |
+-----------------------------------------+----------------------------------+--------------------+

Edge cases:

- It is not an error if the server returns a different schema between GetFlightInfo and DoGet, or between a GetPreparedStatement action and DoGet.

Protocol Buffer Definitions
---------------------------

.. literalinclude:: ../../../format/FlightSql.proto
   :language: protobuf
   :linenos:
