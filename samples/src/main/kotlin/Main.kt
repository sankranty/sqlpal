
import java.time.LocalDate
import kotlin.collections.ArrayList
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.time.Duration
import java.time.LocalTime
import kotlin.math.ceil
import kotlin.math.cos
import org.sqlpal.*
import org.sqlpal.query.*

suspend fun main(args: Array<String>)
{
    val base = if (args.contains("big")) "test_big" else "test"
    SqlPal.setDataSource("jdbc:postgresql://localhost:5431/$base", base, base)

    when {
        args.contains("ins") -> ins3()
        args.contains("insm") -> insMany()
        args.contains("upd") -> upd3()
        args.contains("find") -> find()
        args.contains("sel") -> sel3()
        args.contains("json") -> json()
        args.contains("gen") -> insert(args.contains("big"))
        else -> select(args.contains("prep"))
    }
}

fun json() {
    SqlPal.storeArraysAs = ArrayStorageType.JsonExceptByteArray

    val p = PersonJ(name = "Katerina", edu = listOf(Education.high, Education.middle),
        edua = arrayOf(Education.high, Education.scienceDegree))
    insert(p)

    update<PersonJ>(-"id >= ${44}", PersonJ::name to "Ekaterina")

    val persons = select<PersonJ>(-"edu LIKE '%high%'")
    persons.forEach { println(it) }
}

fun ins() {
    val p = Person2(name = "Katerina", gender = Gender.female, birthDate = LocalDate.now(), education = Education.high,
        edu = listOf(Education.high, Education.middle))
    transaction {
        insert(p, it)
        insert(p, it)
    }
    println("Inserted person ID is ${p.id} and about is ${p.about2}")
}

fun ins3() {
    //val p = Person3(name = "Katerina", num = listOf(4, 7))
    var p = Person3(name = "Katerina", num = mutableListOf(0, 7), num2= arrayOf(7, 8),
        edu = arrayOf(Education.high, Education.middle), edu2 = mutableListOf(Education.scienceDegree)
    )
    insert(p)
    p = Person3(name = "Katerina", num = emptyList(), num2= emptyArray(), edu = emptyArray())
    insert(p)
    println("Inserted person ID is ${p.id} and numbers are ${p.num}")
    Person3::name.name
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

fun upd() {
    val p2 = selectById<Person2>(10)
    p2.height = 175
    update(p2, listOf("height" to p2.height))

    val name = "Katerina"
    val p = Person2(name = name, gender = Gender.female, birthDate = LocalDate.now(), education = Education.high,
        edu = listOf(Education.high, Education.middle))

    transaction {
        p.work = "Economist"
        update(p, it, -"name = $name")

        p.about = "I like economics"
        update(p, it)
    }
}

fun upd3() {
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
}

fun sel3() {
    val educations: List<Education> = mutableListOf(Education.high)
    val educationsArr = emptyArray<Education>()
    val num = emptyArray<Int>()
    val num2 = emptyList<Int>()
    //select<Person3>(-"num = $num and num2 = ${-num2}").forEach { println("Person $it") }
    select<Person3>(-"edu2 = ${-educations}", includeOptional = true).forEach { println("Person $it") }

    //val educations = emptyList<Education>()
    //select<Person3>(-"num2 = $num and edu2 = ${-educations}").forEach { println("Person $it") }
//    class A (val a: Int)
//    val str = arrayOf("a", "b")
//    val obj = arrayOf(A(1), A(2))
}

var selectedRowsCount = arrayOf(0 to 0.0, 0 to 0.0, 0 to 0.0, 0 to 0.0)

suspend fun select(prepare: Boolean) {
    val from = LocalTime.now()
    coroutineScope {
        launch { selectLoop(0, prepare) }
        launch { selectLoop(1, prepare) }
        launch { selectLoop(2, prepare) }
        launch { selectLoop(3, prepare) }
    }
    val dur = Duration.between(from, LocalTime.now())
    val durInSeconds = dur.seconds + dur.nano / 1_000_000_000.0
    var rowsRead = 0
    var durA = 0.0
    var durB = 0.0
    for ((i, p) in selectedRowsCount.withIndex()) {
        println("Thread $i run for ${p.second} seconds")
        rowsRead += p.first
        if (i % 2 == 0) durA += p.second else durB += p.second
    }
    println("Difference is ${(durB - durA) / 2} seconds")
    println("\nJDBC select for $rowsRead rows is done in $durInSeconds seconds\n")
}


fun selectLoop(index: Int, prepare: Boolean) {
    val from = LocalTime.now()
    var rowsRead = 0
//    val sel = SelectNoPos()
//    sel.selectLoop(index, prepare)
//    return
//
//    val d = Dynamic()
//    d.selectLoop(index, prepare)
//    return

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
    for (i in 0..300) {
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
    val dur = Duration.between(from, LocalTime.now())
    val diff = dur.seconds + dur.nano / 1_000_000_000.0
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
        res.getInt("x"),
        res.getInt("y"),
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
        res.getInt(10),
        res.getInt(11),
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
