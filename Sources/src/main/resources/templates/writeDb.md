## [Db] DB出力

### 01. Table名
##### lazy val writeTableName: String = componentId

実装不要

### 02. DB書込みモード
##### val writeDbMode: WriteDbMode = Insert

実装不要

### 03. DB保存モード
##### val writeDbSaveMode: SaveMode = SaveMode.Append

実装不要

### 04. DB更新キー
##### val writeDbUpdateKeys: Set[String] = Set.empty[String]

実装不要

### 05. DB非更新カラム指定
##### val writeDbUpdateIgnoreColumns: Set[String] = Set.empty[String]

実装不要

### 06. 共通項目付与指定
##### val writeDbWithCommonColumn: Boolean = true

実装不要

### 07. DB情報指定
##### val writeDbInfo: DbInfo = DbConnectionInfo.bat1

実装不要

### 08. NA文字(空文字若しくはnull)スペース変換指定
##### val writeDbConvNaMode: Boolean = false

実装不要

### 09. Oracle ヒント句指定
##### val writeDbHint: String = ""

実装不要

