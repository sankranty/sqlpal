
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

    var x: Int,
    var y: Int,
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
    var num2: Array<Int>,
    var edu: Array<Education>,
    var edu2: List<Education>? = emptyList(),
    @SqlIgnore
    var gen: Gender = Gender.female
) : Base()

data class PersonJ (
    @Id @AutoGen
    var id: Long? = null,
    var name: String,
    var edu: List<Education>,
    var edua: Array<Education>
)

data class Pal (
    @Id @AutoGen
    var id: Long? = null,
    var edu: Education
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