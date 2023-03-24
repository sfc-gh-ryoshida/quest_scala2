package d2k.common

import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 object PostCodeNormalizer {
   val PARENT_CORRECT_SIZE = Seq(3)

   val CHILD_CORRECT_SIZE = Seq(2, 4)

   val NO_HYPHEN = 1

   val EXIST_HYPHEN = 2

   val PARENT_POSITION = 0

   val CHILE_POSITION = 1

   private [this] def checkPostCode(sizeList: Seq[Int])(postCode: String) = Option(postCode).flatMap{ p =>if (p.forall(_.isDigit) && sizeList.contains(p.size))
      Some(p)
else
      None
}.getOrElse("")

   val parent = checkPostCode(PARENT_CORRECT_SIZE)_

   val child = checkPostCode(CHILD_CORRECT_SIZE)_

   def apply(postCode: String) : String = Option(postCode).map{ pCode => val splitted = pCode.split("-")
splitted.size match {
      case EXIST_HYPHEN => val p = parent(splitted(PARENT_POSITION))
 val c = child(splitted(CHILE_POSITION))
if (c.isEmpty)
         p
else
         s"${ p }-${ c }"
      case NO_HYPHEN => val target = splitted.head
 val parentSize = PARENT_CORRECT_SIZE.head
parent(target.take(parentSize)) + child(target.drop(parentSize))
   }
}.getOrElse("")

   def single(postCode: String) : String = apply(postCode)
}