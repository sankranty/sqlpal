
import org.sqlpal.*
import java.time.LocalDate

class Person (
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

@SqlName("\"some table\"")
data class Person3 (
    @Id @AutoGen
    var id: Long? = null,
    @SqlName("\"first-name\"")
    var name: String,
    var num: List<Int>,
    var num2: Array<Int>,
    var edu: Array<Education>,
    var edu2: List<Education>? = emptyList(),
    var gen: Gender = Gender.female
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