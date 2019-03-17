import java.net.URL

val url:String = "http://a.baidu.com/a/b/c02354?p1=1&p2=2"

val u = new URL(url)
println(u.getHost)
println(u.getPath)
println(u.getPort)
println(u.getProtocol)
println(u.getProtocol)
println(u.getAuthority)
println(u.getQuery)
println(u.getContent)
println(u.getRef)
println(u.getUserInfo)
