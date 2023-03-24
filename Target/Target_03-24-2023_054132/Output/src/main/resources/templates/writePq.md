## [Pq] PQ出力

### 01. Parquet書込名
##### lazy val writePqName: String = componentId

実装不要

### 02. Parquet書込パス
##### def writePqPath(implicit inArgs: InputArgs): String = inArgs.baseOutputFilePath

実装不要

### 03. Partition分割対象カラム名
##### val writePqPartitionColumns: Seq[String] = Seq.empty[String]

実装不要

