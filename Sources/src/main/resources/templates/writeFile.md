## [File] File出力

### 01. 出力ファイル名
##### lazy val writeFileName: String = componentId

実装不要

### 02. 出力ファイルパス
##### def writeFilePath(implicit inArgs: InputArgs): String = inArgs.baseOutputFilePath

実装不要

### 03. 出力ファイル種別
##### val writeFileMode: WriteFileMode = Fixed

実装不要

### 04. 出力ファイルパラメータ

#### 04.01. 出力項目長(only Fixed)
##### itemLengths: Int*

実装不要

#### 04.02. ダブルクォート対象カラム名(only Csv)
##### wrapTargetCols: String*

実装不要

### 05. 強制ダブルクォートモード
##### val writeFileVariableWrapDoubleQuote: Boolean = true

実装不要

### 06. パーティション出力対象項目リスト
##### val writeFilePartitionColumns: Seq[String] = Seq.empty[String]

実装不要

### 07. パーティション出力ファイル拡張子
##### val writeFilePartitionExtention: String = ""

実装不要

### 08. 可変長出力エスケープ文字列
##### val writeFileVariableEscapeChar: String = ""

実装不要

