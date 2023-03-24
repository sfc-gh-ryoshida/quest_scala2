package d2k.common

import java.security.MessageDigest
import org.apache.commons.codec.binary.Base64
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 object MD5Utils {
   val charEnc = "utf-8"

   def getMD5Str(targetStr: String) : String = {
   val md5 = MessageDigest.getInstance("MD5")
 val md5Data = md5.digest(targetStr.getBytes(charEnc))
md5Data.foldLeft(""){(l, r) => val i: Int = r.asInstanceOf[Int]
 val result = if (i < 0)
         {
         i + 256
         }
else
         {
         i
         }
if (result < 16)
         {
         s"${ l }0${ Integer.toHexString(result) }"
         }
else
         {
         s"${ l }${ Integer.toHexString(result) }"
         }
}
   }

   def getMD5Str(targetStr: String, md5WordStr: String) : String = getMD5Str(targetStr.trim + getMD5Str(md5WordStr))

   def getMD5Base64Str(targetStr: String, md5WordStr: String, flag: Boolean = true) : String = {
   val bytes = MessageDigest.getInstance("MD5").digest((targetStr.trim + getMD5Str(md5WordStr)).getBytes(charEnc))
 val base64 = new String (Base64.encodeBase64(bytes))
if (flag && base64.takeRight(2) == "==")
         {
         base64.dropRight(2)
         }
else
         {
         base64
         }
   }
}