## [Pq] Parquet入力

%%name%%
### 02. 読込Parquetパス
##### def readPqPath(implicit inArgs: InputArgs): String = inArgs.baseInputFilePath

実装不要

### 03. Pqファイル存在チェック
##### val readPqStrictCheckMode: Boolean = true

実装不要

### 04. 空DataFrame用Schema
##### val readPqEmptySchema: Seq[(String, String)] = Seq.empty[(String, String)]

実装不要

