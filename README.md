minhash-service
----


[![Build Status](https://travis-ci.org/4d55397500/dataflow-minhash.svg?branch=master)](https://travis-ci.org/4d55397500/minhash-service)


Large scale document min-hashing implemented at the 'serverless' layer, over Cloud Dataflow and BigQuery.

### Background
See the wikipedia [article](https://en.wikipedia.org/wiki/MinHash) on min-hashing.

### Design
A Google Cloud Dataflow job converts documents to minhash representations and stores those representations along with partial minhash projections in BigQuery tables.  The documents are first tokenized as [shingles](https://github.com/4d55397500/minhash-service/blob/9d9dae3508e8859527f47f67de27fc4bc2e19f29/src/main/kotlin/MinHash.kt#L313-L318), with shingles compressed to a [4-byte representation](https://github.com/4d55397500/minhash-service/blob/9d9dae3508e8859527f47f67de27fc4bc2e19f29/src/main/kotlin/MinHash.kt#L324-L326). The resulting two BigQuery tables facilitate a [nearest neighbor search](https://github.com/4d55397500/minhash-service/blob/9d9dae3508e8859527f47f67de27fc4bc2e19f29/src/main/kotlin/LocalSearch.kt#L14-L31) as a join, filter and then group by. See the example [local search](./docs/local_search.md) finding neighbors for a given query set of documents.


![Architecture](./minhash_architecture.png)


### API
See the [API doc](docs/api.md).

Two primary operations are supported:
 1) converting documents to minhashes and persisting
 2) nearest neighbor lookup for each of a given subset of documents


 
### Run Tests
To run tests,
```
./gradlew test
```


### License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details

