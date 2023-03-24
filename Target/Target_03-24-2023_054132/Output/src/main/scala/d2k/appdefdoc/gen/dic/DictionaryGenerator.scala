package d2k.appdefdoc.gen.dic

import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 object DictionaryGenerator extends App {
val (baseUrl, branch) = (args(0), args(1))
println(s"[Start Dictionary Generate] ${ args.mkString(" ") }")
GenerateDictionary(baseUrl).generate(branch)
}