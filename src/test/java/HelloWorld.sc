import ch.hsr.geohash.GeoHash
import org.apache.commons.lang3.StringUtils

val arr = Array[String]("1","2","3","","5","")

val all = arr ++ "7"


val code1 = GeoHash.withBitPrecision(39.990464d,116.481488d,60).toBase32
val code2 = GeoHash.withBitPrecision(39.990484d,116.481468d,60).toBase32
val code3 = GeoHash.withBitPrecision(39.990784d,116.481368d,60).toBase32


println(code1)
println(code2)
println(code3)