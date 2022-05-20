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

========
Glossary
========

.. glossary::
   :sorted:

   array
   vector
       A *contiguous*, *one-dimensional* sequence of values with known
       length where all values have the same type.  An array consists
       of zero or more :term:`buffers <buffer>`, a non-negative
       length, and a :term:`data type`.  The buffers of an array are
       laid out according to the data type as defined by the columnar
       format.

       Arrays are *contiguous* in the sense that iterating the values of
       an array will iterate through a consistent set of buffers, even
       though an array may consist of multiple disjoint buffers, or
       may consist of child arrays that themselves span multiple
       buffers.

       Arrays are *one-dimensional* in that they are a sequence of
       :term:`slots <slot>` or values, and not a sequence of
       :term:`rows <row>`, even though for some data types (like
       structs or unions), a slot may represent multiple values.

       Defined by the :doc:`./Columnar`.

   buffer
       A *contiguous* region of memory with a given length.

   child array
   parent array
       foo

   child type
   parent type
       foo

   chunked array
       A *discontiguous*, *one-dimensional* sequence of values with
       known length where all values have the same type.

       Consists of zero or more :term:`arrays <array>`.

       Chunked arrays are discontiguous in the sense that iterating
       the values of a chunked array may require iterating through
       different buffers for different indices.

       Not part of the columnar format; this term is specific to
       certain language implementations of Arrow.

       .. seealso:: :term:`record batch`, :term:`table`

   complex type
   nested type
       A :term:`data type` whose structure depends on one or more
       other child data types. For instance, ``List`` is a nested type
       that has one child.

       Two nested types are equal if and only if their child types are
       equal.

   data type
   type
       A type that a value can take, such as ``Int8`` or
       ``List[Utf8]``. The type of an array determines how its values
       are laid out in memory according to :doc:`./Columnar`.

       .. seealso:: :term:`logical type`, :term:`nested type`,
                    :term:`primitive type`

   dictionary
   dictionary-encoding
       foo

   extension type
       foo

   field
       Denotes a column in a :term:`record batch` or :term:`table`.
       Consists of a field name, a :term:`data type`, a flag
       indicating whether the field is nullable or not, and optional
       key-value metadata.

   IPC file format
   file format
   random-access format
       foo

   IPC message
   message
       foo

   IPC streaming format
   streaming format
       foo

   logical type
       An application-facing :term:`data type` that is implemented as
       (and has the same layout as) some :term:`primitive type`. For
       example, a 128-bit decimal may be stored as a 16-byte fixed
       width binary field.

   physical type
       foo

   primitive type
       foo

       .. seealso:: :term:`data type`

   RecordBatch message
       An :term:`IPC message` TODO <>.

   record batch
       "Record batch" refers to two closely related, but distinct,
       concepts. This term defines it as generally used by language
       implementations; see :term:`RecordBatch message` for the term
       as used in the IPC specification.

       In many language implementations of Arrow, a record batch is a
       *contiguous*, *two-dimensional* collection of columns, where
       each column is an :term:`array`.  Each array has the same
       length, but may have a different type.

       Like arrays, record batches are contiguous in the sense that <>.

   row
       foo

   schema
       foo

   slot
       foo

   table
       In many language implementations of Arrow, a record batch is a
       *discontiguous*, *two-dimensional* collection of columns, where
       each column is a :term:`chunked array`.  Each chunked array has
       the same length, but may have a different type.  Different
       columns may be chunked differently.

       Not part of the columnar format; this term is specific to
       certain language implementations of Arrow.

       .. seealso:: :term:`chunked array`, :term:`record batch`

   VectorSchemaRoot
       2-D
