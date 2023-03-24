# 郵便番号コード変換Component 仕様

## 概要

DataFrameより入力された郵便番号を指定された変換先にしたがって変換し、DataFrameのカラムとして返却する

[FW要望チケット](http://10.47.125.225:3000/redmine/issues/41862)

## 呼出関数

### package

    d2k.common.df.component.cmn

### 関数シグネチャ

##### 地方公共団体コード変換

    PostCodeConverterapply()(implicit inArgs: InputArgs)
      .localGovernmentCode(postCodeName1: String, postCodeName2: String = "")(outName1: String, outName2: String = "")

### 引数

##### implicit inArgs: InputArgs [必須]

SparkApp::exec関数の引数として渡されるアプリケーしょん入力パラメータ
暗黙引数として定義されている為、指定する必要はない

##### postCodeName1: String [必須]

入力する郵便番号が格納されている__カラム名__を指定する
_postCodeName1_のみが指定された場合、郵便番号の親番号と子番号が接続された状態として扱う

###### 入力可能コード例
    
    "300-0001" "3000001" "30001" "300" "300    "

##### postCodeName2: String = ""

入力する郵便番号が格納されている__カラム名__を指定する
_postCodeName2_が指定された場合、_postCodeName1_には親番号、_postCodeName2_には子番号が格納されている状態として扱う

###### 入力可能コード例
    
    "0001" "01" "  "

##### outName1: String [必須]

変換された__県コード__を格納する為の__カラム名__を指定する
_outName1_のみが指定された場合__県コード__のみ設定する

##### outName2: String

変換された__市区町村コード__を格納する為の__カラム名__を指定する
_outName1_及び_outName2_が指定された場合、__県コード__/__市区町村コード__の両方設定する

### 仕様

入力郵便番号が__null,empty string, unmatch__の場合、及びマスターデータの県コード/市区町村コードに__null, empty string__が設定されていた場合、"99"及び"999"を返却する

### 実装例

#### 県コードのみを返却
    
```scala
df ~> PostCodeConverter().localGovernmentCode("CD_POST")("CD_KENCD")
```

#### 県コード/市区町村コードを返却
    
```scala
df ~> PostCodeConverter().localGovernmentCode("CD_POST")("CD_KENCD", "CD_DEMEGRPCD")
```

#### 郵便番号親子分割時
    
```scala
df ~> PostCodeConverter().localGovernmentCode("CD_POST1","CD_POST2")("CD_KENCD", "CD_DEMEGRPCD")
```

#### 同一アプリケーション内で複数回コード変換を呼び出す場合

実行パフォーマンスに影響がある為、同一アプリケーションで複数回、郵便番号変換を行う場合は、一旦インスタンスを変数に格納してメソッドを呼び出す。

```scala
val pcc = PostCodeConverter() //インスタンスを格納
val df2 = df ~> pcc.localGovernmentCode("CD_POST1")("CD_KENCD")
val df3 = df ~> pcc.localGovernmentCode("CD_POST1","CD_POST2")("CD_KENCD", "CD_DEMEGRPCD")
```
