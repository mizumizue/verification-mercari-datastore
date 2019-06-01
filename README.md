# VerificationMercariDatastore

## 個人的な検証用PJ

下記検証ライブラリ。
書き方の差異による性能検証やそもそもの使い方、書き方の検証が目的。

- [mercari/datastore](https://github.com/mercari/datastore)
- [boom](https://github.com/mercari/datastore/tree/master/boom) 

## PJ情報

- Go1.9.7
- パッケージ管理([dep](https://github.com/golang/dep))

## Datastore認証

下記のようにGCPのサービスアカウントの認証鍵ファイルでDatastoreClientを作成している。

```go
func CreateBoom(ctx context.Context) *boom.Boom {
	credentialPath := os.Getenv("CREDENTIAL_FILE_PATH")
	opts := datastore.WithCredentialsFile(credentialPath)
	cli, _ := clouddatastore.FromContext(ctx, opts)
	return boom.FromClient(ctx, cli)
}
```

下記手順を実施することで実行可能。

- Datastore書き込み権限のあるサービスアカウントを作成
- サービスアカウントの鍵ファイルをDL
- 鍵ファイルのPATHを`CREDENTIAL_FILE_PATH`の環境変数に設定

