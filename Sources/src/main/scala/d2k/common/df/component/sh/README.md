# Commission拠点別チャネル判定Component生成関数仕様書

## 概要

選択された業務種別毎に、コミッション拠点チャネル判定値を取得するComponentを生成するための関数群

## 呼出関数

### package

    d2k.common.df.component.sh

### 関数シグネチャ

##### COM試算

    CommissionBaseChannelSelector.comm試算
    CommissionBaseChannelSelector.comm試算(uniqueKeys: String*)

##### COM実績_月次手数料

    CommissionBaseChannelSelector.comm実績_月次手数料
    CommissionBaseChannelSelector.comm実績_月次手数料(uniqueKeys: String*)

#####  COM実績_割賦充当

    CommissionBaseChannelSelector.comm実績_割賦充当
    CommissionBaseChannelSelector.comm実績_割賦充当(uniqueKeys: String*)

#####  COM実績_直営店

    CommissionBaseChannelSelector.comm実績_直営店
    CommissionBaseChannelSelector.comm実績_直営店(uniqueKeys: String*)

#####  毎月割一時金

    CommissionBaseChannelSelector.comm毎月割一時金
    CommissionBaseChannelSelector.comm毎月割一時金(uniqueKeys: String*)

### 引数

##### uniqueKeys: String*

入力するレコードを一意とする為の項目名リスト
引数を指定しない場合は、内部で自動的にUnique Keyを生成し一意Keyとする。
___但し、Unique Keyの生成はコストが掛かる為、可能な限りuniqueKeyを指定する事___

### 実装例

    inputDataFrame ~> comm試算.run
    inputDataFrame ~> comm試算("key1","key2").run

## 実装詳細

1. DBのMAA300[COMチャネル制御パラメータ]テーブルより、指定された業務種別の処理区分[DV_DISPODIV]に従って必要なレコードだけを取得する。"comm試算"の場合はWhere条件に"DV_DISPODIV = '01'"を指定する

1. 入力対象のDataFrameに設定された、CD_CHNLGRPCD[チャネルグループコード]及びCD_CHNLDETAILCD[チャネル詳細コード]をキーにMAA300[COMチャネル制御パラメータ]テーブルと[Join](#Join条件)する

1. Join後、MAA300[COMチャネル制御パラメータ]よりDV_OUTOBJDIV[出力対象区分]及び、DV_TRICALCOBJDIV[試算対象区分]を設定する

1. Join結果がUnmatchの場合、[Unmatch時業務種別設定値](#Unmatch時業務種別設定値)に従ってDV_OUTOBJDIV[出力対象区分]及び、DV_TRICALCOBJDIV[試算対象区分]を設定する。

1. 呼出関数に指定されたレコードの一意キーを元にGroupingを行い、DV_DISCRDIV[識別区分]を降順にSortして先頭レコードを抽出する。
引数で一意キーが指定されない場合、内部一意キー("_uniqKey_")を生成し、処理を行う。

1. 抽出したレコードより、不要なカラムを削除する

----

#### Join条件
    (
      ([MAA300.識別区分] = "01" and [MAA300.チャネルコード] = [input.チャネルグループコード])
      or
      ([MAA300.識別区分] = "02" and [MAA300.チャネルコード] = [input.チャネル詳細コード]))
    )

#### Unmatch時業務種別設定値

|      業務種別       |        処理区分        |  識別区分  | 出力対象区分 | 試算対象区分 |
| ------------------- | ---------------------- | ---------- | ------------ | ------------ |
| comm試算            | 01(COM試算)            | 00(その他) | 1(対象)      | 1(対象)      |
| comm実績_月次手数料 | 02(COM実績_月次手数料) | 00(その他) | 1(対象)      | 1(対象)      |
| comm実績_割賦充当   | 03(COM実績_割賦充当)   | 00(その他) | 1(対象)      | 1(対象)      |
| comm実績_直営店     | 04(COM実績_直営店)     | 00(その他) | 0(対象外)    | 0(対象外)    |
| comm毎月割一時金    | 05(毎月割一時金)       | 00(その他) | 1(対象)      | 1(対象)      |
