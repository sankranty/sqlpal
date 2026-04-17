# SqlPal

Ultralight ORM for Kotlin.

* laconic - no boilerplate code
* fast - near raw JDBC performance
* flexible - all the strength of SQL + dynamic building of queries

## Quick Start

Add the dependencies:
```kotlin
plugins {
    // Used to specify bind parameters directly in the query string.
    id("io.exoquery.terpal-plugin") version "1.8.21-0.1.0"
}

dependencies {
    implementation("org.sqlpal:sqlpal:1.0.3")
}
```
And most of the operations are done in a single line (no need to manually map bind parameters):
```kotlin
enum class Hobby { Sports, Art, Travelling, Coding }

class Person(
    val name: String, 
    val hobbies: List<Hobby>,
    @Id @AutoGen val id: Long? = null)

fun main() {
    // Specify datasource
    SqlPal.setDataSource("jdbc:postgresql://localhost:5433/db", "test", "test")

    // Single insert (note support for lists and enums)
    val kate = Person("Kate", listOf(Hobby.Art, Hobby.Sports))
    insert(kate) // id property is updated automatically
    
    // Batch insert 
    val friends = listOf(
        Person("Mike", listOf(Hobby.Sports, Hobby.Coding, Hobby.Travelling)),
        Person("Alice", listOf(Hobby.Art, Hobby.Coding, Hobby.Travelling)),
    )
    insertMany(friends)
    
    // Select by ID
    val id = (0..2).random()
    var p = selectById<Person>(id)
        
    // Select by conditions (specified values are automatically transformed into bind parameters)
    var coders = select<Person>(-"id > $id AND any(hobbies) = ${Hobby.Coding} ORDER BY name")
    
    // Complex select with dynamic conditions
    val activeOnly = true
    coders = read<Person>(-"""
        SELECT p.id, p.name, p.hobbies
        FROM person p JOIN employees e ON p.id = e.person_id
        WHERE any(p.hobbies) = ${Hobby.Coding}
        $If $activeOnly AND e.active = true""")
    
    // Do something in a transaction
    transaction {
        p = selectOne<Person>(-"name = 'Mike'", it)
        // Do some stuff
        update(p, it) // entity is identified by property annotated with @Id
        delete(p, it)
    }
    
    // Update without an object
    update(where = -"id = $id", Person::name to "Bob")
}
```

## Why another ORM?

Because none of the numerous ORMs is at the same time laconic, fast and flexible. Common approaches are:

* **DSL that is transformed to SQL by the framework.** 
It's JPQL/HQL in JPA/Hibernate, Kotlin Exposed, JOOQ and QueryDSL.
While this approach provides strong typing, it has a number of drawbacks:
  * DSL does not provide all features of native SQL. 
For instance, JOOQ or QueryDSL do not directly support the `STD_Within` spatial function
(which provides effective search within a given radius, using a spatial index), 
or the `<->` operator (which allows getting the nearest locations very fast). 
  * Not all DSLs are as expressive as SQL - e.g., `id >= 10 and created <= '01.01.1990'` 
is much clearer than `Person.id.gte(10).and(Person.created.loe("01.01.1990"))`
  * Transformation of DSL into SQL adds performance overhead that can be significant, depending on the case.

* **Native SQL queries with bind parameters.** 
This is provided by most ORMs, including Spring Data JPA/JDBC, Kapper, MyBatis. Main drawbacks are:
  * It requires you to manually specify all bind parameters.
  * When it is done via annotations, there is no way to specify conditions dynamically.

* **Query construction.** 
It allows you to conditionally construct a query. It's provided, for example, by JPA Specification. Drawbacks:
  * Too wordy - code is very hard to read and maintain.
  * Additional performance overhead for query construction.

* **Entity manager for DML operations.** 
It's standard for JPA. The drawbacks are:
  * Lack of flexibility as operations are fully controlled by the manager.
  * Additional performance overhead due to state management 
while it's generally not needed in microservice architecture.

SqlPal is an effort to combine the three objectives, specified above, to deal with the mentioned problems.

## General idea
SQL is an expressive language, and modern IDEs provide syntax highlighting and code completion
for SQL strings inside program code. We just need a convenient way to specify parameters 
and the possibility to do it dynamically.

For this purpose, SqlPal uses standard Kotlin string interpolation 
and the [Terpal](https://github.com/ExoQuery/Terpal) plugin. 
When you write `-"SELECT * FROM person WHERE id = $id"` (note unary minus before the string) 
it is compiled into an object that contains the query string and bind parameters from provided values, 
so SqlPal just executes it, thus there is no runtime overhead for query construction 
and no need to manually specify bind parameters.

Also, SqlPal provides methods to perform most of the routine tasks with a single line, like in JPA.

## Setup

There are 3 options:

**1. Set datasource parameters**
```kotlin
SqlPal.setDataSource("jdbc:driver://address/db", "user", "pass")
```
SqlPal will create a Hikari connection pool and use it for all operations. 
Optionally you can specify other parameters.

**2. Provide an existing datasource**
```kotlin
val ds = // get datasource from your framework
SqlPal.setDataSource(ds)
```
SqlPal will use this datasource for all operations, closing connections after use 
or returning them to the pool, if it's a pooled datasource.

This is suitable for cases when you already have a framework that provides a datasource, 
or if you for some reason don't want the Hikari connection pool to be used.

**3. Provide a connection on method call**

All SqlPal methods allow you to optionally specify a connection. For such a call, no setup is needed.

This is suitable for cases where SqlPal is used alongside another database library, and 
for instance, you need to use both of them within a single transaction.

## Features

### Supported databases

SqlPal is DBMS agnostic. It works with any database that provides a JDBC driver.

### Supported types

* All Kotlin primitive types
* DateTime types: LocalDate, LocalTime, LocalDateTime, OffsetTime, OffsetDateTime, ZonedDateTime, Instant
* BigDecimal and Currency
* Enums (are converted to varchar if the database does not support enums)
* Blob, Clob and SQLXML
* List, Array and typed arrays, e.g. IntArray. They can be stored as database arrays if the database supports arrays, or as JSON.
* ByteArray (mapped to BLOB)
* UUID
* Any user-defined type if a mapper is provided. See `SqlPal.addTypeMapper` for details.

### Smart mapping

#### When reading data
SqlPal creates an object by calling the primary constructor 
and automatically mapping columns from the result set to primary constructor parameters. 

SqlPal detects constructor parameters with default values and correctly processes them
even if the result set does not contain the corresponding columns.

If the result set contains columns that do not match constructor parameters, 
then SqlPal tries to map them to mutable properties declared in the class body and base classes.

When reading query results, SqlPal automatically converts names of columns regardless of naming convention 
(first_name, FIRST_NAME, firstname, "First Name" - all will be correctly mapped to the `firstName` property, 
and property naming can be different as well).

#### When generating queries
SqlPal uses snake_case for database objects by default, 
but you can set `SqlPal.convertNamesToSnakeCase` to `false` if names in database are the same as in code.

Also, the `@SqlName` annotation can be used to specify the name explicitly.

### Support for collections and arrays

SqlPal supports lists, arrays and typed arrays (ByteArray, IntArray, etc.) both as object properties 
and as query parameters.

When `List<>` is specified as a query parameter it must be prefixed with the `-`.
Unary minus operator is overloaded by SqlPal and extracts the generic type of the `List`.
It's necessary to handle empty lists, because unlike an array, an empty list does not contain information 
about its generic type at runtime, which makes it impossible to map an empty list to the appropriate SQL type.
```kotlin
    val myHobbies = listOf(Hobby.Art, Hobby.Coding, Hobby.Travelling)
    var soulmates = select<Person>(-"hobbies = ${-myHobbies} ORDER BY name")

    val noHobbies = emptyList<Hobby>()
    var busyPeople = select<Person>(-"hobbies = ${-noHobbies} ORDER BY name")
```

### Dynamic queries

In all methods that accept a query, you can form the query dynamically.  
By `$If $condition` (where `condition` is a boolean variable or value) 
you can conditionally include or exclude part of the query.
If `condition` is `false`, then the rest of the content up to the line break is not included in the query:
```kotlin
val activeOnly = true
read<Person>(-"""
        SELECT p.id, p.name, p.hobbies
        FROM person p JOIN employees e ON p.id = e.person_id
        $If $activeOnly WHERE e.active = true
        ORDER BY id
        """)
```

To inline something directly into the query string (instead of treating it as a bind parameter), use `$I$` instead of `$`.
```kotlin
val sortColumn = if (sortByName) "name" else "creation_date"
read<Person>(-"SELECT * FROM person ORDER BY $I$sortColumn")
```

### Select

* `selectById` - select a single entity by ID. The ID column is identified by a property annotated with `@Id`.

Methods to select from a single table (SELECT and FROM clauses are automatically generated, 
you provide only the part after the WHERE keyword):
* `selectOne` - single entity.
* `select` - list of entities.

Methods to select from any source (you provide a full SELECT query):
* `readOne` - single entity.
* `read` - list of entities.

Methods to select values from any source (you provide a full SELECT query):
* `readValue` - single value.
* `readValues` - list of values.

### Insert

* `insert` - inserts an entity and by default updates properties annotated with `@AutoGet` with values from the database.
* `insertMany` - inserts entities from any iterable source. It is optimized for inserting many items.

### Update

* `update` - updates a single entity, identified by a property annotated with `@Id`, 
or entities that match the criteria if `where` parameter is specified.
* `update` (with `propList` parameter) - same as above, but updates only the specified properties.
* `update` (without `entity` parameter) - updates specified values for entities that match the `where` parameter criteria.
Allows updating values without creating an entity object.

### Delete

* `delete` - deletes a single entity, identified by a property annotated with `@Id`,
  or entities that match criteria if `where` parameter is specified.

### Execute

* `exec` - executes a query and returns the number of rows affected.

Methods to get values of generated columns:
* `execWithResult` - executes a query and returns the first generated value.
* `execWithResults` - executes a query and returns a map of 'colName - generated value' for the first inserted/updated row.

### Transactions

* `transaction` - wraps operations in a transaction.

### Customization

By default, SqlPal tries to store lists and arrays as database arrays. 
If your database does not support columns of array type, or you want to store lists as JSON in a varchar column,
then set `SqlPal.storeArraysAs` to the appropriate value.

If the database supports arrays of enums, then SqlPal will store lists and arrays of enums as database arrays of enums.
If it's not desired, then set `SqlPal.useEnumArrays` to `false`, to store enums as strings.

If you need to read/store some custom type, it can be done by implementing the `ValueMapper` interface.
If only storing or only reading is necessary, then you can leave the corresponding function (`readValue` or `writeValue`) empty.
Here is an example of how support for the locationtech `Point` can be easily added:
```kotlin
import org.locationtech.jts.geom.Point

// It is recommended to define Mapper as an object to avoid unnecessary creation of class instances on each call.
object PointMapper : ValueMapper {
    private val reader = WKBReader()
    private val writer = WKBWriter()

    // Assumes that the column is wrapped with ST_AsBinary in the SELECT statement.
    override fun readValue(resultSet: ResultSet, colIndex: Int) = reader.read(resultSet.getBytes(colIndex))

    override fun writeValue(value: Any?, statement: PreparedStatement, paramIndex: Int, componentType: KClass<*>?): Boolean {
      val bytes = writer.write(value as Point)
      statement.setBytes(paramIndex, bytes)
      return true
    }
}

// Add the mapper to use it across the app.
SqlPal.addTypeMapper(Point::class, PointMapper)

// Or annotate particular property if you want to apply mapper only to it.
class Person (
    @Mapper(PointMapper::class)
    val location: Point
)
```

### Performance tuning

SqlPal is designed to be as fast as possible out of the box. Tuning is necessary only in rare cases. 
In most cases, the main performance impact depends on optimization of queries and database indexes.

For performance-critical queries, SqlPal has a `read` overload that accepts a `createItem` callback. 
For this case, SqlPal provides extension methods on `ResultSet` for more convenient manual reading of values:
```kotlin
fun readPerson(r: ResultSet) = Person(
    r long "id",
    r str "name",
    r enumVal "gender",
    r date "birth_date",
    r enum "education",
    r intVal "height",
)
```
This approach is a bit faster, as it does not use reflection to create objects, thus you get the performance of raw JDBC.
But for most queries that execute in a few milliseconds, the difference will be about 5%. 
So it makes sense only for queries that are executed on the microsecond scale.