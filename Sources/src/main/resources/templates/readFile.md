## [File] File入力

### 01. 項目情報ファイルID
##### lazy val itemConfId: String = componentId

実装不要

### 02. 読込ファイル情報
##### val fileInputInfo: FileInputInfoBase

#### 02.01. 読込ファイル形式
##### FileInputInfoBase

    %%読込ファイル形式%

#### 02.02. 読込ファイル名
##### inputFiles: Set[String]

    %%読込ファイル名%%

#### 02.03. 読込ファイルパス設定名
##### envName: String = FileInputInfoBase.ENV\_NAME\_DEFAULT

実装不要

#### 02.04. ダブルクォーテーション削除(only TsvInfo)
##### dropDoubleQuoteMode: Boolean = false

実装不要

#### 02.05. ヘッダ有無
##### header: Boolean = false

実装不要

#### 02.06. フッタ有無
##### footer: Boolean = false

実装不要

#### 02.07. 改行有無
##### newLine: Boolean = true

実装不要

#### 02.08. 連番指定有無(only FixedInfo)
##### withIndex: Boolean = false

実装不要

#### 02.09. レコード長チェック有無(only FixedInfo)
##### recordLengthCheck: Boolean = false

実装不要

#### 02.10. 文字コード
##### charSet: String = "MS932"

実装不要

#### 02.11. 改行コード
##### newLineCode: NewLineCode = LF

実装不要

#### 02.12. ドメインコンバート事前Filter(only FixedInfo)
##### preFilter: (Seq[String], Map[String, String] => Boolean) = null

実装不要

