# 0. 準備

## 0. データセットの入手の準備

本ハンズオンで利用するデータセットの一部はGit LFSを介して提供されている。Git LFSをインストールせよ。

## 1. データセットの入手

本ハンズオンではAmazonのShopping Queries Datasetを利用する。以下から `git clone` せよ。

https://github.com/amazon-science/esci-data

## 2. データの読み込みの準備

このデータセットの一部はParquet形式である。Parquet形式のファイルを読み込むためのライブラリをインストールせよ。

## 3. 製品データの読み込み

ライブラリの機能で製品データ（以下）をメモリに読み込め。

```
./esci-data/shopping_queries_dataset/shopping_queries_dataset_products.parquet
```

（想定実行時間：1分以内）

## 4. 製品データの確認

製品を1つランダムに取り出し表示せよ。何度か実行せよ。