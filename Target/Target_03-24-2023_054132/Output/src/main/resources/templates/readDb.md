## [Db] DB入力定義

### 01. DBインスタンス情報
##### val readDbInfo: DbInfo = DbConnectionInfo.bat1

実装不要

%%name%%
### 03. 読込Column指定
##### val columns: Array[String] = Array.empty[String]

実装不要

### 04. DB読込時条件(引数無)
##### val readDbWhere: Array[String] = Array.empty[String]

実装不要

### 05. DB読込時条件(引数有)
##### def readDbWhere(inArgs: InputArgs): Array[String] = Array.empty[String]

実装不要

