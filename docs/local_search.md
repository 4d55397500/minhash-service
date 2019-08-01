#### Example local minhash search in SQL

```

 WITH eph AS (
                SELECT
                    key,
                    [ARRAY_TO_STRING(ARRAY(SELECT CAST(num AS STRING) FROM UNNEST(minHashes) num WITH OFFSET pos WHERE pos >= 0 AND pos < 3), '-'),
ARRAY_TO_STRING(ARRAY(SELECT CAST(num AS STRING) FROM UNNEST(minHashes) num WITH OFFSET pos WHERE pos >= 1 AND pos < 4), '-'),
ARRAY_TO_STRING(ARRAY(SELECT CAST(num AS STRING) FROM UNNEST(minHashes) num WITH OFFSET pos WHERE pos >= 2 AND pos < 5), '-'),
ARRAY_TO_STRING(ARRAY(SELECT CAST(num AS STRING) FROM UNNEST(minHashes) num WITH OFFSET pos WHERE pos >= 3 AND pos < 6), '-'),
ARRAY_TO_STRING(ARRAY(SELECT CAST(num AS STRING) FROM UNNEST(minHashes) num WITH OFFSET pos WHERE pos >= 4 AND pos < 7), '-'),
ARRAY_TO_STRING(ARRAY(SELECT CAST(num AS STRING) FROM UNNEST(minHashes) num WITH OFFSET pos WHERE pos >= 5 AND pos < 8), '-'),
ARRAY_TO_STRING(ARRAY(SELECT CAST(num AS STRING) FROM UNNEST(minHashes) num WITH OFFSET pos WHERE pos >= 6 AND pos < 9), '-'),
ARRAY_TO_STRING(ARRAY(SELECT CAST(num AS STRING) FROM UNNEST(minHashes) num WITH OFFSET pos WHERE pos >= 7 AND pos < 10), '-'),
ARRAY_TO_STRING(ARRAY(SELECT CAST(num AS STRING) FROM UNNEST(minHashes) num WITH OFFSET pos WHERE pos >= 8 AND pos < 11), '-'),
ARRAY_TO_STRING(ARRAY(SELECT CAST(num AS STRING) FROM UNNEST(minHashes) num WITH OFFSET pos WHERE pos >= 9 AND pos < 12), '-'),
ARRAY_TO_STRING(ARRAY(SELECT CAST(num AS STRING) FROM UNNEST(minHashes) num WITH OFFSET pos WHERE pos >= 10 AND pos < 13), '-'),
ARRAY_TO_STRING(ARRAY(SELECT CAST(num AS STRING) FROM UNNEST(minHashes) num WITH OFFSET pos WHERE pos >= 11 AND pos < 14), '-'),
ARRAY_TO_STRING(ARRAY(SELECT CAST(num AS STRING) FROM UNNEST(minHashes) num WITH OFFSET pos WHERE pos >= 12 AND pos < 15), '-'),
ARRAY_TO_STRING(ARRAY(SELECT CAST(num AS STRING) FROM UNNEST(minHashes) num WITH OFFSET pos WHERE pos >= 13 AND pos < 16), '-'),
ARRAY_TO_STRING(ARRAY(SELECT CAST(num AS STRING) FROM UNNEST(minHashes) num WITH OFFSET pos WHERE pos >= 14 AND pos < 17), '-'),
ARRAY_TO_STRING(ARRAY(SELECT CAST(num AS STRING) FROM UNNEST(minHashes) num WITH OFFSET pos WHERE pos >= 15 AND pos < 18), '-'),
ARRAY_TO_STRING(ARRAY(SELECT CAST(num AS STRING) FROM UNNEST(minHashes) num WITH OFFSET pos WHERE pos >= 16 AND pos < 19), '-'),
ARRAY_TO_STRING(ARRAY(SELECT CAST(num AS STRING) FROM UNNEST(minHashes) num WITH OFFSET pos WHERE pos >= 17 AND pos < 20), '-')] partialHashes
                FROM
                    `default-168404.foodataset.minhashes`
                WHERE
                    key IN ('doc207', 'doc339')
            )
            SELECT
                eph.key,
                APPROX_TOP_COUNT(flattenedDocHashes, 11)
            FROM
               `default-168404.foodataset.hashmap`, eph, UNNEST(docHashes) flattenedDocHashes
            WHERE
                partialMinHash IN UNNEST(eph.partialHashes)
            GROUP BY
                eph.key
                
```                
