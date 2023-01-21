# 3. 定量評価と検定

## 0. 準備

英語結合データにおいてクエリは `query_id` で区別される。
`query_id` ごとに `product_id` を辞書順にソートすることで、**ベースライン**のランキングを行え。

## 1. ランキング改善

ベースラインのランキングには明らかに改善の余地がある。
`query` 中の単語が `product_description` 中に出現する回数の降順にソートすることで、**改善版**のランキングを行え。
ただし、単語はスペースで区切られているものとする。

## 2. 適合率

ベースラインと改善版のランキングそれぞれについて、10件適合率の平均値を示せ。
ただし、`esci_label` が `I` **ではない**製品を適合とする。

## 3. MAP

10件適合率は10件の中での順序を考慮していなかった。
ベースラインと改善版のランキングそれぞれについて、平均適合率 (average precision, AP) の平均値 (mean average precision, MAP) を示せ。
ただし、計算の対象はランキング上位10件までに**限らない**ことに注意せよ。
また、英語結合データ中に全ての適合な製品が含まれているものと仮定してよい（この仮定は単純化のために置いたもので、誤りである）。

## 4. DCG

英語結合データにおいては、製品に**適合度**があった。
ベースラインと改善版のランキングそれぞれについて、discounted cumulative gain (DCG) @10の平均値を示せ。
ただし、`esci_label` が `E`, `S`, `C`, `I` の製品のgainをそれぞれ `4`, `2`, `1`, `0` とする。

## 5. nDCG

製品をgainの降順にソートしたとき、DCGは理想値 (ideal DCG) をとる。
英語結合データにおいて、`query_id` ごとのideal DCG@10を求めよ。
ただし、英語結合データ中に全ての適合な製品が含まれているものと仮定してよい。

また、ベースラインと改善版のランキングそれぞれについて、normalized discounted cumulative gain (nDCG) @10の平均値を示せ。

## 6. 符号検定

ベースラインと改善版のランキングのnDCG@10に差がないと仮定すると、ある `query_id` について両者を比較したとき、どちらが大きいかは五分五分である。
現にどちらかが大きかった回数をそれぞれ数えよ。
この結果について符号検定を行え。

## 7. *t*検定

ベースラインと改善版のランキングのnDCG@10の平均値の差について、対応のある*t*検定を行い*p*値を求めよ。
ただし、対応は `query_id` ごとに取るものとする。

## 8. 多重検定

任意のさらなる改善を加えた**再改善版**のランキングを構成せよ。
例えば本ハンズオンの別章を参考にせよ。

再改善版のランキングについてもnDCG@10を計算せよ。
また、ベースライン、改善版、再改善版の各ランキングについて、例えば有意水準0.05および0.01として、nDCG@10の平均値の差の検定を行え。
このとき、多重検定になるので、ボンフェローニ補正を行え。

## 9. 正規性の検定

対応のある*t*検定は、本来データの正規性を前提としている。
そこで、ベースラインと改善版のランキングのnDCG@10それぞれの分布について、正規性の検定を行え。