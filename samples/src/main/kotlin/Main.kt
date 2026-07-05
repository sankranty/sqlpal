
import java.time.LocalDate
import kotlin.collections.ArrayList
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.Point
import org.locationtech.jts.io.WKBReader
import org.locationtech.jts.io.WKBWriter
import java.sql.PreparedStatement
import java.sql.ResultSet
import kotlin.math.ceil
import kotlin.math.cos
import org.sqlpal.*
import org.sqlpal.query.*
import kotlin.math.round
import kotlin.reflect.KClass

suspend fun main(args: Array<String>)
{
    val base = if (args.contains("big")) "test_big" else "test"
    SqlPal.setDataSource("jdbc:postgresql://localhost:5431/$base", base, base)

    SqlPal.addTypeMapper(Point::class, PointMapper)

    when {
        //args.contains("timings") -> selectWithTimings()
        args.contains("ins") -> insInTran()
        args.contains("ins-many") -> insMany()
        args.contains("upd") -> updInTran()
        args.contains("find") -> find()
        args.contains("sel") -> sel()
        args.contains("coll") -> collections()
        args.contains("json") -> json()
        args.contains("where-in") -> whereIn()
        args.contains("if-else") -> ifElse()
        args.contains("point") -> point()
        args.contains("blob") -> blob()
        args.contains("unsigned") -> unsigned()
        args.contains("gen") -> insert(args.contains("big"))
        else -> select(args.contains("prep"), args)
    }
}

object PointMapper : ValueMapper {
    private val reader = WKBReader()
    private val writer = WKBWriter()

    // Assumes that column is wrapped with ST_AsBinary in SELECT statement.
    override fun readValue(resultSet: ResultSet, colIndex: Int) = reader.read(resultSet.getBytes(colIndex))

    override fun writeValue(value: Any?, statement: PreparedStatement, paramIndex: Int, componentType: KClass<*>?): Boolean {
        val bytes = writer.write(value as Point)
        statement.setBytes(paramIndex, bytes)
        return true
    }
}

fun point() {
    val location = Location(GeometryFactory().createPoint(Coordinate(30.1, 60.3)))
    insert(location)
    read<Location>(-"SELECT id, ST_AsBinary(point) as point FROM location").forEach { println(it) }
}

fun blob() {
    val p = PersonB("Mike", byteArrayOf(33, 35, 112), null)
    transaction { insert(p, it) }
    select<PersonB>(-"true").forEach { println(it) }
}

@OptIn(ExperimentalUnsignedTypes::class)
fun unsigned() {
    var p = PersonU("Mike", 40_000u, 220u, ulongArrayOf(10u, 20u), ubyteArrayOf(189u, 176u))
    insert(p)
    p = PersonU("Nina", 10_000u, 227u, null, ubyteArrayOf(189u, 176u))
    insert(p)
    select<PersonU>(-"num = ${40_000u} or num2 = ${p.num2}").forEach { println(it) }
}

fun ifElse() {
    var female = true
    var economist = true
    var education = true

    println("Only female")
    select<Person>(-"""
        $If $female gender = 'female'
        $Else$If $economist work = 'Economist'
        $Else$If $education education = 'scienceDegree'
        $Else height < 160 
        LIMIT 10
    """).forEach { println(it.gender) }

    female = false
    println()
    println("Economists")
    select<Person>(-"""
        $If $female gender = 'female'
        $Else$If $economist work = 'Economist'
        $Else$If $education education = 'scienceDegree'
        $Else height < 160 
        LIMIT 10
    """).forEach { println(it.work) }

    economist = false
    println()
    println("Who has scienceDegree")
    select<Person>(-"""
        $If $female gender = 'female'
        $Else$If $economist work = 'Economist'
        $Else$If $education education = 'scienceDegree'
        $Else height < 160 
        LIMIT 10
    """).forEach { println(it.education) }

    education = false
    println()
    println("With height 160")
    select<Person>(-"""
        $If $female gender = 'female'
        $Else$If $economist work = 'Economist'
        $Else$If $education education = 'scienceDegree'
        $Else height < 160 
        LIMIT 10
    """).forEach { println(it.height) }
}

fun whereIn() {
    Education.values().forEach { insert(Pal(edu = it)) }

    val empty = emptyList<Int>()
    val count = select<Pal>(-"id in $empty").size
    println("Empty list test: list size - $count")

    val ids = listOf(0, 1, 2, 11)
    select<Pal>(-"id in $ids").forEach { println(it) }

    val eduList = listOf(Education.high, Education.scienceDegree, Education.school)
    select<Pal>(-"edu in $eduList").forEach { println(it) }

    println("By full list equality")
    val edu = listOf(Education.high, Education.middle)
    select<Person3>(-"edu = ${-edu}").forEach { println(it) }
}

@OptIn(ExperimentalUnsignedTypes::class)
fun json() {
    SqlPal.storeArraysAs = ArrayStorageType.JsonExceptByteArray

    var p = PersonJ(name = "Katerina",
        num = arrayOf(12, 11, 27),
        numArr = IntArray(4) { it * 3 },
        edu = listOf(Education.high, Education.middle),
        edus = setOf(Education.high, Education.school),
        edua = arrayOf(Education.high, Education.scienceDegree),
        relations = mapOf(null to false, Education.school to true, Education.high to true, Education.middle to null),
        numUByte = ubyteArrayOf(255u, 231u),
        numUShort = ushortArrayOf(65_000u)
    )
    insert(p)

    update(-"id >= ${44}", PersonJ::name to "Ekaterina")

    val persons = select<PersonJ>(-"${"edu" includes "high"}")
    persons.forEach { println(it) }

    p = PersonJ(name = "Katerina",
        edu = listOf(Education.high, Education.middle, Education.scienceDegree),
        edus = setOf(Education.high, Education.school),
        edua = arrayOf(Education.high, Education.scienceDegree),
        relations = mapOf(null to null, Education.high to true, Education.scienceDegree to true),
    )
    insert(p)
    println("\nPersons with match by relations map content:")
    select<PersonJ>(-"relations = ${-p.relations}").forEach { println(it) }
}

fun insInTran() {
    val p = Person2(name = "Katerina", gender = Gender.female, birthDate = LocalDate.now(), education = Education.high,
        edu = listOf(Education.high, Education.middle))
    transaction {
        insert(p, it)
        insert(p, it)
    }
    println("Inserted person ID is ${p.id} and about is ${p.about2}")
}

fun insListAndArray() {
    var p = Person3(name = "Katerina", num = mutableListOf(0, 7), num2= arrayOf(7, 8),
        edu = arrayOf(Education.high, Education.middle), edu2 = mutableListOf(Education.scienceDegree))
    insert(p)

    p = Person3(name = "Katerina", num = emptyList(), num2= emptyArray(), edu = emptyArray())
    insert(p)

    println("Inserted person ID is ${p.id} and numbers are ${p.num}")
}

fun insMany() {
    delete<Person3>(-"id >= 30")

    val list = mutableListOf<Person3>()
    repeat(10_000) {
        val num = (0..9).random()
        val edu = when ((0..3).random()) {
            1 -> Education.school
            2 -> Education.middle
            3 -> Education.high
            else -> Education.undefined
        }
        list.add(
            Person3(
                name = "Katerina",
                num = mutableListOf(num, num + 7),
                num2 = arrayOf(7, 8, 9),
                edu = arrayOf(edu, if (it % 2 == 0) Education.middle else Education.high),
                edu2 = mutableListOf()
            )
        )
    }
    val insertedCount = insertMany(list)
    println("Inserted $insertedCount rows")
}

fun updInTran() {
    val p2 = selectById<Person2>(10)
    p2.height = 175
    update(p2, listOf("height" to p2.height))

    transaction {
        val name = "Katerina"
        val p = Person2(name = name, gender = Gender.female, birthDate = LocalDate.now(), education = Education.high,
            edu = listOf(Education.high, Education.middle))
        p.work = "Economist"
        update(p, it, -"name = $name")

        p.about = "I like economics"
        update(p, it)
    }
}

fun updSet() {
    var p = selectById<Person3>(11)
    p.name = "Ekaterina"
    p.num = listOf(500)
    p.num2 = arrayOf(1000)
    update(p) { only(::name, ::num) }

    p = selectById(4)
    update(p) { set(::num to listOf(1300)) }
    println(p)

    p = selectById(7)
    p.name = "Ekaterina"
    p.num = listOf(1, 2, 3, 4)
    p.edu2 = listOf(Education.school, Education.scienceDegree)
    update(p) { except(::name) }
}

fun find() {
    val p = selectById<Person2>(20)
    println("Education of person with ID ${p.id} is ${p.edu}")

    val p3 = selectById<Person3>(1)
    println("Education of person with ID ${p3.id} is ${p3.name}")
}

fun sel() {
    val a = arrayOf<String>()
    val b = arrayListOf<String>()
    val arr = a::class.java.isArray
    val barr = b::class.java.isArray
    println(arr)
    println(barr)

    val work = "Economist"
    select<Person>(-"work = $work and gender = ${Gender.female}").forEach { println("Person $it") }

    println("\nPersons with ID > 20")
    val persons = select<Person>(-"id < 20 $If ${false} and gender = ${Gender.female}")
    persons.forEach { println("Person $it") }

    println("Person with ID 1 exists: " + exists<Person>(1))
    println("First person in the list exists: " + exists(persons[0]))
}

fun collections() {
    val p = Person3("Katerina", listOf(1, 4, 8),
        num2 = arrayOf(10, 20, 30),
        numArr = IntArray(4) { it * 3 },
        edu = arrayOf(Education.high, Education.middle),
        edu2 = listOf(Education.high, Education.scienceDegree),
        edus = setOf(Education.high, Education.school),
    )
    insert(p)
    select<Person3>(-"""
        edu = ${p.edu} and edu2 = ${-p.edu2} and edus = ${-p.edus} 
        and num2 = ${p.num2} and num_arr = ${p.numArr}
        """).forEach { println(it) }
    //select<Person3>(-"edu = ${p.edu} and edu2 = ${-p.edu2} and edus = ${-p.edus}").forEach { println(it) }

    println("Select Person3")
    var educations: List<Education> = mutableListOf(Education.high)
    val educationsArr = emptyArray<Education>()
    val num = emptyArray<Int>()
    val num2 = emptyList<Int>()
    //select<Person3>(-"num = $num and num2 = ${-num2}").forEach { println(it) }
    select<Person3>(-"edu2 = ${-educations}", includeOptional = true).forEach { println(it) }

    println("Select Person3 with filter by empty list")
    educations = emptyList()
    select<Person3>(-"num2 = $num and edu2 = ${-educations}").forEach { println(it) }
}

var selectedRowsCount = Array(0) { 0 to 0.0 }

suspend fun select(prepare: Boolean, args: Array<String>) {
    val threadCount = args.indexOf("parallel").let { if (it >= 0) args[it + 1].toInt() else 4 }

    // Warm up (otherwise result will be incorrect as it will include JIT compilation and other stuff).
    selectedRowsCount = Array(2) { 0 to 0.0 }
    coroutineScope {
        launch { selectLoopWithCoords(0, prepare, 1) }
        launch { selectLoopWithCoords(1, prepare, 1) }
    }

    do {
        selectedRowsCount = Array(threadCount) { 0 to 0.0 }
        val durInSeconds = seconds {
            coroutineScope {
                repeat(threadCount) { launch { selectLoopWithCoords(it, prepare) } }
            }
        }
        var rowsRead = 0
        var durA = 0.0
        var durB = 0.0
        for ((i, p) in selectedRowsCount.withIndex()) {
            rowsRead += p.first
            if (threadCount > 1) {
                if (i % 2 == 0) durA += p.second
                else durB += p.second
                println("Thread $i run for ${p.second} seconds")
            }
        }
        if (threadCount > 1) {
            val diff = (durB - durA) / 2
            println("Difference is $diff seconds, ${"%.2f".format(diff * 100 / (durA / 2))} %")
        }
        println("\nJDBC select for $rowsRead rows is done in $durInSeconds seconds\n")
    } while (readln().isBlank())
}
/*
suspend fun selectWithTimings() {
    // Warm up (otherwise result will be incorrect as it will include JIT compilation and other stuff).
    coroutineScope {
        launch { selectLoop(0, false, 1) }
        launch { selectLoop(1, false, 1) }
    }
    // Reset values to zero as they were changed during warmup.
    prepareSql = 0L
    getConnection = 0L
    prepareStatement = 0L
    setBindParams = 0L
    exec = 0L
    getColIndexes = 0L
    getConstructor = 0L
    getReaders = 0L
    createArrayForValues = 0L
    getCreateObjectAndCreateList = 0L
    readValues = 0L
    createEntity = 0L
    setOptionalProperties = 0L
    totalInQuery = 0L

    var from = System.nanoTime()
    selectLoop(1, false)
    total = System.nanoTime() - from

    println("\nSelect for ${"%,d".format(selectedRowsCount[1].first)} rows.\n")
    printTiming("prepareSql", prepareSql)
    printTiming("getConnection", getConnection)
    printTiming("prepareStatement", prepareStatement)
    printTiming("setBindParams", setBindParams)
    println()

    printTiming("exec", exec)
    printTiming("getColIndexes", getColIndexes)
    printTiming("getConstructor", getConstructor)
    printTiming("getReaders", getReaders)
    printTiming("createArrayForValues", createArrayForValues)
    printTiming("getCreateObjectAndCreateList", getCreateObjectAndCreateList)
    printTiming("readValues", readValues)
    printTiming("createEntity", createEntity)
    printTiming("setOptionalProperties", setOptionalProperties)
    println("-----------------")
    printTiming("Sum in Query", exec + getColIndexes + getConstructor + getReaders +
            createArrayForValues + getCreateObjectAndCreateList + readValues + createEntity + setOptionalProperties)
    printTiming("Total in Query", totalInQuery)

    println()
    printTiming("Total", total)

    from = System.nanoTime()
    selectLoop(0, false)
    var totalManual = System.nanoTime() - from

    from = System.nanoTime()
    selectLoop(1, false)
    total += System.nanoTime() - from

    from = System.nanoTime()
    selectLoop(0, false)
    totalManual += System.nanoTime() - from

    println()
    printTiming("Auto average total", total/2, totalManual/2)
    printTiming("Manual average total", totalManual/2, totalManual/2)
    println()
}
*/
var total = 0L

fun printTiming(param: String, nanoSec: Long, totalSec: Long = total) =
    println("${"%-28s".format(param)} - " +
            "${"%.2f".format(nanoSec / 1_000_000_000.0)} sec  " +
            "${"%3d".format(nanoSec * 100 / totalSec)} %")

inline fun seconds(block: () -> Unit): Double {
    val from = System.nanoTime()
    block()
    return round((System.nanoTime() - from) / 10_000_000.0) / 100
}

fun selectLoop(index: Int, prepare: Boolean, iterationsCount: Int = 1000) {
    val from = System.nanoTime()
    var rowsRead = 0

    for (i in 0..iterationsCount) {
        val id = (10_000..110_000L).random()
        val activity = LocalDate.now().minusYears(1).plusDays((1..500L).random())

        val list = read<Person>(-"""
            SELECT id, name, gender, birth_date, about, education, work, height, city, activity_date
            FROM person
            WHERE
                birth_date BETWEEN ${LocalDate.of(1920, 1, 1)} AND ${LocalDate.of(2020, 12, 21)}
                AND gender = ${Gender.female} 
                AND activity_date <= $activity AND id < $id
            ORDER BY
                activity_date DESC,
                id DESC
            LIMIT $I${100}
            """, 100, ::readPerson)

        rowsRead += list.count()
    }
    val diff = (System.nanoTime() - from) / 1_000_000_000.0
    selectedRowsCount[index] = rowsRead to diff
}

fun selectLoopWithCoords(index: Int, prepare: Boolean, iterationsCount: Int = 300) {
    val from = System.nanoTime()
    var rowsRead = 0

    val dx = ceil(30_000.0 / (111_110 * cos(Math.toRadians(30.35))) * 100).toInt()
    val dy = ceil(30_000.0 / 111_320 * 100).toInt()
    val x1 = 3035 - dx
    val x2 = 3035 + dx
    val y1 = 6001 - dy
    val y2 = 6001 + dy
    /*
    val dx = 20_000.0 / (111_110 * cos(Math.toRadians(30.35)))
    val dy = 20_000.0 / 111_320
    val x1 = 30.35 - dx
    val x2 = 30.35 + dx
    val y1 = 60.01 - dy
    val y2 = 60.01 + dy
    */
    //select = con.prepareStatement(query)
    for (i in 0..iterationsCount) {
        //if (!prepare)
        val apply = true
        val activity = LocalDate.now().minusYears(1).plusDays((200..500L).random())
        val id = (500_000..910_000L).random()
/*
        val list = sqlpal("""
            SELECT 
                id, name, gender, birth_date, about, education, work, height, city, activity_date,
                ST_X(location::geometry) as x, 
                ST_Y(location::geometry) as y
            FROM person
            WHERE
                birth_date BETWEEN ${LocalDate.of(1940, 1, 1)} AND ${LocalDate.of(1970, 12, 21)} 
                AND gender = ${Gender.female} 
                $If $apply AND activity_date <= $activity AND id < $id
            ORDER BY
                activity_date DESC,
                id DESC
            LIMIT $I${100}
            """
        ).get(100, ::readPerson)
        */
/*
        val list = sqlpal("""
            SELECT id
            FROM person
            WHERE
                birth_date BETWEEN ${LocalDate.of(1920, 1, 1)} AND ${LocalDate.of(1980, 12, 21)}
                AND gender = ${Gender.female} 
                AND ST_DWithin(location, ST_MakePoint(${30.35}, ${60.01}), ${30.0*1000})
            LIMIT 1000"""
        ).read(100, ::readPerson)
*/
/*
        val list = sqlpal("""
            WITH box AS (
              SELECT
                floor(${30.35} * 100)::int AS x_i,
                floor(${60.01} * 100)::int AS y_i,
                ceil(20000 / (111300.0 * cos(radians(${30.35}))) * 100)::int AS dx,
                ceil(20000 / 111100.0 * 100)::int AS dy
            )
            SELECT 
                id, name, gender, birth_date, about, education, work, height, city, activity_date,
                ST_X(location::geometry) as x, 
                ST_Y(location::geometry) as y
            FROM person CROSS JOIN box b
            WHERE
                birth_date BETWEEN ${LocalDate.of(1940, 1, 1)} AND ${LocalDate.of(1970, 12, 21)}
                AND gender = ${Gender.female} 
                $If $apply AND activity_date <= $activity AND id < $id
                AND x_int BETWEEN b.x_i - b.dx AND b.x_i + b.dx
                AND y_int BETWEEN b.y_i - b.dy AND b.y_i + b.dy
            ORDER BY
                activity_date DESC,
                id DESC
            LIMIT $I${100}
            """
        ).read(100, ::readPerson)
*/

        val list = if (index % 2 == 1)
            read<Person>(-"""
            SELECT id, name, gender, birth_date, about, education, work, height, city, activity_date, x, y, edu
            FROM person
            WHERE
                birth_date BETWEEN ${LocalDate.of(1920, 1, 1)} AND ${LocalDate.of(1980, 12, 21)}
                AND gender = ${Gender.female} 
                $If $apply AND activity_date <= $activity AND id < $id
                AND x BETWEEN $x1 AND $x2
                AND y BETWEEN $y1 AND $y2
            ORDER BY
                activity_date DESC,
                id DESC
            LIMIT $I${100}
            """
            , 100)
        else
            read(-"""
            SELECT id, name, gender, birth_date, about, education, work, height, city, activity_date, x, y, edu
            FROM person
            WHERE
                birth_date BETWEEN ${LocalDate.of(1920, 1, 1)} AND ${LocalDate.of(1980, 12, 21)}
                AND gender = ${Gender.female} 
                $If $apply AND activity_date <= $activity AND id < $id
                AND x BETWEEN $x1 AND $x2
                AND y BETWEEN $y1 AND $y2
            ORDER BY
                activity_date DESC,
                id DESC
            LIMIT $I${100}
            """
            ,100, ::readPerson)

        rowsRead += list.count()
    }
    val diff = (System.nanoTime() - from) / 1_000_000_000.0
    selectedRowsCount[index] = rowsRead to diff
}

// ST_Distance(location, ST_MakePoint('30.35', '60.01')) as dist,
// and ST_DWithin(location, ST_MakePoint(${30.35}, ${60.01})::geography, ${20.0*1000}, false)
// and location <-> ST_MakePoint(${30.35}, ${60.01})::geography < ${20.0*1000}
// location <-> ST_MakePoint(${30.35}, ${60.01})::geography

fun select100Rows(select: PreparedStatement): Int {
//    select.clearParameters()
//    select.setObject(1, LocalDate.of(1920, 1, 1))
//    select.setObject(2, LocalDate.of(2020, 12, 21))
//    select.setObject(3, Gender.female, Types.OTHER)
//    select.setDouble(4, 30.35)
//    select.setDouble(5, 60.01)
//    select.setDouble(6, 20.0*1000)
//    select.setObject(7, )
//    select.setLong(8, )

    //val locationReader = WKBReader()
    //val strReader = WKTReader()
    val pers = ArrayList<Person>(100)

    val res = select.executeQuery()
    while (res.next()) {
        pers.add(readPerson(res /*, locationReader, strReader*/))
    }

    return pers.count()
}

fun readPerson(res: ResultSet /*, locationReader: WKBReader, strReader: WKTReader*/) = Person(
        res.getLong("id"),
        res.getString("name"),
        Gender.valueOf(res.getString("gender")),
        res.getDate("birth_date").toLocalDate(),
        res.getString("about"),
        Education.valueOf(res.getString("education")),
        res.getString("work"),
        res.getInt("height"),
        res.getString("city"),
        //res.getInt("x"),
        //res.getInt("y"),
        //locationReader.read(res.getBytes("location")) as Point,
        res.getDate("activity_date").toLocalDate(),
        //res.getDouble("dist"),
        //emptyList()
    )


fun readPersonByIndex(res: ResultSet, /*, locationReader: WKBReader, strReader: WKTReader*/) = Person(
        res.getLong(1),
        res.getString(2),
        Gender.valueOf(res.getString(3)),
        res.getDate(4).toLocalDate(),
        res.getString(5),
        Education.valueOf(res.getString(6)),
        res.getString(7),
        res.getInt(8),
        res.getString(9),
        //res.getInt(10),
        //res.getInt(11),
        //locationReader.read(res.getBytes(10)) as Point,
        res.getDate(11).toLocalDate(),
        //emptyList()
    )

fun insert(big: Boolean) {
    val batch = StringBuilder()
    val thousandsOfRows = if (big) 1000 else 100
    for (i in 0..thousandsOfRows)
         generate1KInsert(i, batch)
    SqlPal.withConnection { it.prepareStatement(batch.toString()).execute() }
}

fun generate1KInsert(iteration: Int, builder: StringBuilder) {
    val nameChars = ('A'..'Z') + ('a'..'z')
    val aboutChars = ('A'..'Z') + ('a'..'z') + ('0'..'9') +
            ' ' + ' ' + ' ' + ' ' + '.' + ','
    with(builder) {
        append("insert into person (id, name, gender, birth_date, about, education, work, height, city, location, activity_date) values")
        for (i in 2..999) {
            append('(')

            append(iteration * 1000 + i)
            append(",'")

            val nameLen = (5..20).random()
            append(List(nameLen) { nameChars.random() }.joinToString(""))
            append("','")

            append(Gender.values().random())
            append("','")

            append((1920..1999).random())
            append('-')
            append((1..12).random())
            append('-')
            append((1..28).random())
            append("','")

            val aboutLen = (0..100).random()
            append(List(aboutLen) { aboutChars.random() }.joinToString(""))
            append("','")

            append(Education.values().random())
            append("','")

            append(List(nameLen) { nameChars.random() }.joinToString(""))
            append("',")

            append((140..200).random())
            append(",'")

            val x = when (iteration % 3) {
                0 -> 29
                1 -> 37
                else -> 39
            }
            val y = when (iteration % 3) {
                0 -> 59
                1 -> 55
                else -> 45
            }
            append("St. Petersburg',ST_MakePoint(")
            append((Math.random() * (0..3).random()) + x)
            append(',')
            append((Math.random() * (0..3).random()) + y)
            append(")::geography,'")

            append((2024..2026).random())
            append('-')
            append((1..12).random())
            append('-')
            append((10..28).random())

            append("')")
            append(if (i < 999) ',' else ';')
        }
    }
}
