package d2k.app.test.common

import d2k.common.InputArgs

case class TestArgs(confPath: String = "conf", dataPath: String = "data",
                    projectId: String = "projectId", processId: String = "processId", applicationId: String = "appId",
                    runningDateFileFullPath: String = "test/dev/RUNNING_DATE.txt") {
  def toArray = Array("test", "dev", confPath, dataPath, projectId, processId,
    applicationId, runningDateFileFullPath)
  def toInputArgs = InputArgs("test", "dev", confPath, dataPath, projectId, processId,
    applicationId, runningDateFileFullPath)
}
