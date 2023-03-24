package d2k.app.test.common

import d2k.common.InputArgs
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
case class TestArgs (confPath: String = "conf", dataPath: String = "data", projectId: String = "projectId", processId: String = "processId", applicationId: String = "appId", runningDateFileFullPath: String = "test/dev/RUNNING_DATE.txt") {
   def toArray = Array("test", "dev", confPath, dataPath, projectId, processId, 
applicationId, runningDateFileFullPath)

   def toInputArgs = InputArgs("test", "dev", confPath, dataPath, projectId, processId, 
applicationId, runningDateFileFullPath)
}