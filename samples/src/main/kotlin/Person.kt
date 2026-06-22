
import org.locationtech.jts.geom.Point
import org.sqlpal.*
import java.time.LocalDate

data class Person (
    var id: Long? = null,
    var name: String,

    var gender: Gender,
    var birthDate: LocalDate,
    var about: String = "",
    var education: Education = Education.undefined,
    var work: String = "",
    var height: Int = 0,
    var city: String = "",

    //var x: Int,
    //var y: Int,
    var activityDate: LocalDate = LocalDate.MIN,
    //var edu: List<Education>?
)

open class Base (
    @Id @AutoGen
    var id: Long? = null,
)

@SqlName("\"some table\"")
data class Person3 (
    @SqlName("\"first-name\"")
    var name: String,
    var num: List<Int>,
    var num2: Array<Int>? = null,
    var numArr: IntArray? = null,
    var edu: Array<Education>,
    var edu2: List<Education>? = emptyList(),
    var edus: Set<Education>? = emptySet(),
    @SqlIgnore
    var gen: Gender = Gender.female
) : Base()

@OptIn(ExperimentalUnsignedTypes::class)
data class PersonJ (
    @Id @AutoGen
    var id: Long? = null,
    var name: String,
    var num: Array<Int>? = null,
    var numArr: IntArray? = null,
    var edu: List<Education>,
    var edus: Set<Education>?,
    var edua: Array<Education>,
    var relations: Map<Education?, Boolean?>?,
    var numUByte: UByteArray? = null,
    var numUShort: UShortArray? = null
)
data class PersonB (
    var name: String,
    var bin: ByteArray? = null,
    var large: ByteArray? = null,
    @Id @AutoGen
    var id: Long? = null,
)

@OptIn(ExperimentalUnsignedTypes::class)
data class PersonU (
    var name: String,
    var num: UInt,
    var num2: UByte? = null,
    var arr: ULongArray? = null,
    var arr2: UByteArray,
    @Id @AutoGen
    var id: Long? = null,
)

data class Pal (
    @Id @AutoGen
    var id: Long? = null,
    var edu: Education
    )

class Location(
    var point: Point,
    @Id @AutoGen
    var id: Long? = null,
)

data class Person2 (
    @Id @AutoGen
    var id: Long? = null,
    var name: String,

    var gender: Gender,
    var birthDate: LocalDate,
    var work: String = "",
    var about: String = "",
    var height: Int = 0,
    var education: Education = Education.undefined,
    var city: String = "",
    var edu: List<Education>?,
    @AutoGen
    var about2: String = "",
)

enum class Gender {
    male,
    female
}

@SqlName("edu")
enum class Education {
    undefined,
    school,
    middle,
    high,
    scienceDegree
}