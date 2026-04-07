# SqlPal

Ultra lightweight ORM for Kotlin.

* laconic - no boilerplate code
* fast - near raw JDBC performance
* flexible - all strength of SQL + dynamic building of queries

## Quick Overview

Add to dependencies:
```kotlin
plugins {
    // Is used to specify bind parameters directly in query string.
    id("io.exoquery.terpal-plugin") version "1.8.21-0.1.0"
}

dependencies {
    implementation("org.sqlpal:sqlpal:1.0.3")
}
```
And most of the operations are done via single line (no need to manually map bind parameters):
```kotlin
enum class Hobby { Sports, Art, Travelling, Coding }

class Person(
    val name: String, 
    val hobbies: List<Hobby>,
    @Id @AutoGen val id: Long? = null)

fun main() {
    // Specify datasource
    SqlPal.setDataSource("jdbc:postgresql://localhost:5433/db", "test", "test")

    // Single insert (note support of lists and enums)
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
        
    // Select by conditions (specified values automatically transform to bind parameters)
    val coders = select<Person>(-"id > $id AND any(hobbies) = ${Hobby.Coding} ORDER BY name")
    
    // Complex select with dynamic conditions
    val activeOnly = true
    coders = read<Person>(-"""
        SELECT p.id, p.name, p.hobbies
        FROM person p JOIN employees e ON p.id = e.person_id
        WHERE any(p.hobbies) = ${Hobby.Coding}
        $If $activeOnly AND e.active = true""")
    
    // Do something in transaction
    transaction {
        p = selectOne<Person>(-"name = 'Mike'", it)
        // Do some stuff
        update(p, it) // entity is identified by property annotated with @Id
        delete(p, it)
    }
    
    // Update without object
    update(where = -"id = $id", Person::name to "Bob")
}
```

## Why another ORM?

Because none of plenty ORMs is at the same time laconic, fast and flexible. Common approaches are:

* **DSL that is transformed to SQL by framework.** 
It's JPQL/HQL in JPA/Hibernate, Kotlin Exposed, JOOQ and QueryDSL.
While this approach provides strong typing, it has number of drawbacks:
  * DSL does not provide all features of native SQL. 
For instance, there is no direct support in JOOQ and QueryDSL for STD_Within spatial function, 
that allows to effectively search within given radius using spatial index. 
  * Not all DSL are as expressive as SQL, e.g. `id >= 10 and created <= '01.01.1990'` 
is much clearer than `Person.id.gte(10).and(Person.created.loe("01.01.1990"))`
  * Transformation of DSL into SQL adds performance overhead that can be significant, depending on case.

* **Native SQL queries with bind parameters.** 
Is provided by most ORMs, including Spring Data JPA/JDBC, Kapper, MyBatis. Main drawbacks are:
  * It requires to manually specify all bind parameters.
  * When it is done via annotations, there is no way to specify conditions dynamically.

* **Query construction.** 
Allows to conditionally construct query. Is provided, for example, by JPA Specification. Drawbacks:
  * Too wordy, code is very hard to read and maintain.
  * Additional performance overhead for query construction.

* **Entity manager for DML operations.** 
It's standard for JPA. The drawbacks are:
  * Lack of flexibility as operations are fully controlled by manager.
  * Additional performance overhead due to state maintaining 
while it's generally not needed in microservice architecture.

SqlPal is the effort to combine three objectives, specified above, to deal with mentioned problems.

## General idea
SQL is expressive language, and modern IDEs provide syntax highlighting and code completion
for SQL strings inside program code. We just need convenient way specify parameters 
and possibility to do it dynamically.

For this SqlPal uses standard Kotlin string interpolation and [Terpal](https://github.com/ExoQuery/Terpal) plugin. 
When you write `-"SELECT * FROM person WHERE id = $id"` (note unary minus before string) 
it is compiled into object that contains query string and bind parameters from provided values, 
so SqlPal just executes it, thus no runtime overhead for query construction 
and no need to manually specify bind parameters.

Also, SqlPal provides methods to perform most of routine tasks with single line, like in JPA.

## Setup

There are 3 options:

**1. Set datasource parameters**
```kotlin
SqlPal.setDataSource("jdbc:driver://address/db", "user", "pass")
```
SqlPal will create Hikari connection pool and use it for all operations. 
Optionally you can specify other parameters.

**2. Provide existing datasource**
```kotlin
val ds = // get datasource from your framework
SqlPal.setDataSource(ds)
```
SqlPal will use this datasource for all operations, closing connections after use, 
or returning them to pool, if it's pooled datasource.

Is suitable for case when you already have some framework that provides datasource, 
or if you for some reason don't want Hikari connection pool to be used.

**3. Provide connection on methods call**

All SqlPal methods allow to optionally specify connection. For such call setup is not needed.

Is suitable for case when SqlPal is used alongside with other database library, and 
for instance, you need to use both of them within single transaction.

## Features

### Supported databases

SqlPal is DBMS agnostic. It works with any database that provides JDBC driver.

### Supported types

* All Kotlin primitive types
* DateTime types: LocalDate, LocalTime, LocalDateTime, OffsetTime, OffsetDateTime, ZonedDateTime, Instant
* BigDecimal and Currency
* Enums (are converted to varchar if database does not support enums)
* Blob, Clob and SQLXML
* List, Array and typed arrays, e.g. IntArray. Can be stored as database array if database support arrays, or as JSON.
* ByteArray (is mapped to BLOB)
* UUID
* Any user defined type if mapper is provided. See `SqlPal.addTypeMapper` for details.

### Object naming
When reading query results SqlPal automatically converts names of columns regardless of naming convention 
(first_name, FIRST_NAME, firstname, "First Name" - all will be correctly mapped to firstName property).

When generating queries SqlPal uses snake_case for database objects by default, 
but you can set `SqlPal.convertNamesToSnakeCase` to false if names are the same as in code.

Also `@SqlName` annotation can be used to specify name explicitly.

### Dynamic queries

In all methods that accept query you can conditionally exclude part of query
by `$If $condition` where 'condition' is boolean variable or value.
If 'condition' is false, then rest of content to line break is not included into query:
```kotlin
val activeOnly = true
read<Person>(-"""
        SELECT p.id, p.name, p.hobbies
        FROM person p JOIN employees e ON p.id = e.person_id
        $If $activeOnly WHERE e.active = true
        ORDER BY id
        """)
```
To inline something directly into string (instead of treating it as bind parameter), use $I$ instead of $.

### Select

When reading data SqlPal creates object by call to primary constructor.

Mapping is done by both primary constructor parameters and mutable properties declared in class body.
SqlPal detects constructor parameters with default values and correctly process them 
even if result set does not contain corresponding column.

* `selectById` - select single entity by ID. ID column is identified by property annotated with `@Id`.

Methods to select from single table (SELECT and FROM clauses are automatically generated, 
you provide only part after WHERE keyword):
* `selectOne` - single entity.
* `select` - list of entities.

Methods to select from any source (you provide full SELECT query):
* `readOne` - single entity.
* `read` - list of entities.

Methods to select values from any source (you provide full SELECT query):
* `readValue` - single value.
* `readValues` - list of values.

### Insert

* `insert` - inserts entity and by default updates properties annotated with `@AutoGet` with values from database.
* `insertMane` - inserts entities from any iterable source. Is optimized for insertion of many items.

### Update

* `update` - updates single entity, identified by property annotated with `@Id`, 
or entities that match criteria if `where` parameter is specified.
* `update` (with `propList` parameter) - same as above, but updates only specified properties.
* `update` (without `entity` parameter) - updates specified values for entities that match `where` parameter criteria.
Allows to update values without creation of entity object.

### Delete

* `delete` - deletes single entity, identified by property annotated with `@Id`,
  or entities that match criteria if `where` parameter is specified.

### Execute

* `exec` - executes query and returns number of rows affected.

Methods to get values of generated columns:
* `execWithResult` - executes query and returns the first generated value.
* `execWithResults` - executes query and returns map of 'colName - generated value' for the first inserted/updated row.

### Transactions

* `transaction` - wraps operations in transaction.

### Customization

By default, SqlPal tries to store list and arrays as database arrays. 
If your database does not support columns of array type, or you want to store lists as JSON in varchar column,
then set `SqlPal.storeArraysAs` to appropriate value.

If database supports arrays of enum, then SqlPal will store lists and arrays of enum as database array of enum.
If it's not desired, then set `SqlPal.useEnumArrays` to false, to store enums as strings.

### Performance tuning

SqlPal is designed to be as fast as possible out of the box, tuning is necessary only in rare cases. 
In most cases the main performance impact depends on optimization of queries and database indexes.

For performance critical queries SqlPal has `read` overload that accepts `createItem` callback. 
For this case SqlPal provides extension methods on `ResultSet` for more convenient manual reading of values:
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
This approach is a bit faster, as it does not use reflection to create objects, thus you get performance of raw JDBC.
But for most queries, that are executed in about few ms, the difference will be about 5%. 
So it makes sense only for queries that are executed on microsecond scale.