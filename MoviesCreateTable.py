# %%
# 各種設定

from __future__ import print_function  # Python 2/3 compatibility
import boto3
import json
import decimal
from boto3.dynamodb.conditions import Key, Attr
import pandas as pd
from pandas.io.json import json_normalize

# import numpy as np
# import matplotlib.pyplot as plt

pd.set_option("display.max_columns", 100)
pd.set_option("display.max_rows", 500)


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


# %%
# ステップ 1: テーブルを作成する
# MoviesCreateTable.py
# https://docs.aws.amazon.com/ja_jp/amazondynamodb/latest/developerguide/GettingStarted.Python.01.html

# %%
dynamodb = boto3.resource(
    "dynamodb", region_name="us-west-2", endpoint_url="http://localhost:8000"
)

table = dynamodb.create_table(
    TableName="Movies",
    KeySchema=[
        {"AttributeName": "year", "KeyType": "HASH"},  # Partition key
        {"AttributeName": "title", "KeyType": "RANGE"},  # Sort key
    ],
    AttributeDefinitions=[
        {"AttributeName": "year", "AttributeType": "N"},
        {"AttributeName": "title", "AttributeType": "S"},
    ],
    ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
)

print("Table status:", table.table_status)


# %%
# ステップ 2: サンプルデータをロードする
# MoviesLoadData.py
# https://docs.aws.amazon.com/ja_jp/amazondynamodb/latest/developerguide/GettingStarted.Python.02.html#GettingStarted.Python.02.01

dynamodb = boto3.resource(
    "dynamodb", region_name="us-west-2", endpoint_url="http://localhost:8000"
)

table = dynamodb.Table("Movies")

with open("moviedata.json") as json_file:
    movies = json.load(json_file, parse_float=decimal.Decimal)
    for movie in movies:
        year = int(movie["year"])
        title = movie["title"]
        info = movie["info"]

        print("Adding movie:", year, title)

        table.put_item(Item={"year": year, "title": title, "info": info})


# %%
# ステップ 3: 項目を作成、読み込み、更新、削除する
# https://docs.aws.amazon.com/ja_jp/amazondynamodb/latest/developerguide/GettingStarted.Python.03.html
# MoviesItemOps01.py


# %%
# 3.1 新しい項目の作成
# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


dynamodb = boto3.resource(
    "dynamodb", region_name="us-west-2", endpoint_url="http://localhost:8000"
)

table = dynamodb.Table("Movies")

title = "The Big New Movie"
year = 2015

response = table.put_item(
    Item={
        "year": year,
        "title": title,
        "info": {"plot": "Nothing happens at all.", "rating": decimal.Decimal(0)},
    }
)

print("PutItem succeeded:")
print(json.dumps(response, indent=4, cls=DecimalEncoder))


# %%
# ステップ 3.2: 項目を読み取る
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

# Helper class to convert a DynamoDB item to JSON.

try:
    response = table.get_item(
        Key={"year": year, "title": title}
    )  # 　最初のキースキーマで設定したやつだな。
except ClientError as e:
    print(e.response["Error"]["Message"])
else:
    item = response["Item"]
    print("GetItem succeeded:")
    print(json.dumps(item, indent=4, cls=DecimalEncoder))


# %%
# ステップ 3.3: 項目を更新する
# Helper class to convert a DynamoDB item to JSON.

response = table.update_item(
    Key={"year": year, "title": title},
    UpdateExpression="set info.rating = :r, info.plot=:p, info.actors=:a",  # こんな書き方するのか。
    ExpressionAttributeValues={
        ":r": decimal.Decimal(5.5),
        ":p": "Everything happens all at once.",
        ":a": ["Larry", "Moe", "Curly"],
    },
    ReturnValues="UPDATED_NEW",
)

print("UpdateItem succeeded:")
print(json.dumps(response, indent=4, cls=DecimalEncoder))


# %%
# ステップ 3.4: アトミックカウンターを増分する

# Helper class to convert a DynamoDB item to JSON.

response = table.update_item(
    Key={"year": year, "title": title},
    UpdateExpression="set info.rating = info.rating + :val",  # これ使えそう。
    ExpressionAttributeValues={":val": decimal.Decimal(1)},
    ReturnValues="UPDATED_NEW",
)

print("UpdateItem succeeded:")
print(json.dumps(response, indent=4, cls=DecimalEncoder))


# %%
# ステップ 3.5: 項目を更新する (条件付き)
# Helper class to convert a DynamoDB item to JSON.

# Conditional update (will fail)
print("Attempting conditional update...")

try:
    response = table.update_item(
        Key={"year": year, "title": title},
        UpdateExpression="remove info.actors[0]",
        ConditionExpression="size(info.actors) > :num",  # ここで条件を指定している
        ExpressionAttributeValues={":num": 3},  # ここでnumの値を指定
        ReturnValues="UPDATED_NEW",
    )
except ClientError as e:
    if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
        print(e.response["Error"]["Message"])
    else:
        raise
else:
    print("UpdateItem succeeded:")
    print(json.dumps(response, indent=4, cls=DecimalEncoder))


# %%
# ステップ 3.6: 項目を削除する
# Helper class to convert a DynamoDB item to JSON.
print("Attempting a conditional delete...")

try:
    response = table.delete_item(
        Key={"year": year, "title": title},
        ConditionExpression="info.rating <= :val",  # 条件を指定して誤爆防止している
        ExpressionAttributeValues={":val": decimal.Decimal(7.5)},
    )
except ClientError as e:
    if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
        print(e.response["Error"]["Message"])
    else:
        raise
else:
    print("DeleteItem succeeded:")
    print(json.dumps(response, indent=4, cls=DecimalEncoder))


# %%
# ステップ 4: データをクエリおよびスキャンする
# https://docs.aws.amazon.com/ja_jp/amazondynamodb/latest/developerguide/GettingStarted.Python.04.html

# %%
# ステップ 4.1: クエリ - 1 年間にリリースされたすべての映画
# Helper class to convert a DynamoDB item to JSON.

table = dynamodb.Table("Movies")

print("Movies from 1985")

response = table.query(KeyConditionExpression=Key("year").eq(1984))

# for i in response["Items"]:
#     print(i["year"], ":", i["title"])
#     # print(json.dumps(i, cls=DecimalEncoder))

df = json_normalize(response[u"Items"])

# %%
# ステップ 4.2: クエリ - 1 年間にリリースされた特定のタイトルを持つすべての映画
print("Movies from 1992 - titles A-L, with genres and lead actor")

response = table.query(
    ProjectionExpression="#yr, title, info.genres, info.actors[0]",  # 抽出する要素を選べる。
    ExpressionAttributeNames={
        "#yr": "year"
    },  # Expression Attribute Names for Projection Expression only. # yearはこうして指定しないとエラーになる。
    KeyConditionExpression=Key("year").eq(1992)
    & Key("title").between("A", "L"),  # クエリ書ける。
)

for i in response[u"Items"]:
    print(json.dumps(i, cls=DecimalEncoder))


# %%

# pd.DataFrame(response[u"Items"])

json_normalize(response[u"Items"])
