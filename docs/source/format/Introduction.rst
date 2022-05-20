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

============
Introduction
============

.. note:: This document is non-normative.

The Apache Arrow ("Arrow") project consists of a set of **specifications**,
together with **implementations** of those specifications in various languages
and **subprojects** that build upon the implementations to provide higher-level
or more specialized functionality. This document will briefly introduce the
fundamental concepts behind the specification and provide references that go
into further detail on each topic.

.. seealso::
   :doc:`./Glossary`
        The glossary may be useful as a reference.

The Arrow Columnar Format
=========================

- Core of the project
- How to lay out data in memory (provide examples)
- Tour types, buffers, arrays

.. seealso::
   :doc:`./Columnar`

The Arrow IPC Format
====================

- Part of the columnar format
- How to serialize data (e.g., how to lay out data on disk)
- Tour fields, messages, schemas, batches
- Note the design means that while (de)serialization still happens, it can be
  done very cheaply
- Explicitly note that these batches are different than implementation batches
- Tour stream format, note how it can be used on disk, in pipes/sockets, etc.
- Tour file format, note how it builds on stream format, note how it can be used on disk

Other Arrow Specifications
==========================

Arrow C Data Interface
----------------------

- The columnar format defines the layout of buffers but does not say what
  happens with the metadata around them; the C Data Interface does

Arrow C Stream Interface
------------------------

Tensors
-------

- Not supported by all implementations

Arrow Implementations
=====================

Arrow C++/Arrow (R)/PyArrow
---------------------------

- Link to the individual overviews instead
- Make sure the C++ overview is comprehensive
- Note that the C++ overview is intended to introduce functionality that all bindings have access to

Arrow Subprojects
=================

Arrow C++ Query Engine
----------------------

Arrow Flight
------------

Arrow Flight SQL
----------------

DataFusion
----------

Gandiva
-------
