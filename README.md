minhash-service
----


[![Build Status](https://travis-ci.org/4d55397500/dataflow-minhash.svg?branch=master)](https://travis-ci.org/4d55397500/minhash-service)


A large scale min-hashing service for documents. A Google Cloud Dataflow job converts to documents to minhash representations and stores those representations along with partial projections in BigQuery tables. Queries against those partial projections give similarity scores.

A form of [nearest neighbor search](https://github.com/4d55397500/minhash-service/blob/9d9dae3508e8859527f47f67de27fc4bc2e19f29/src/main/kotlin/LocalSearch.kt#L14-L31) is implemented directly in BigQuery. Typically in nearest neighbor search, a hashmap maps each document into one bucket for each partial hash in the minhash representation. In this case, we mimic the hashmap in the BigQuery column store representation by a join, filter and then group by. 

### Background
See the wikipedia [article](https://en.wikipedia.org/wiki/MinHash) on min-hashing.

### Design
[Shingles](https://github.com/4d55397500/minhash-service/blob/9d9dae3508e8859527f47f67de27fc4bc2e19f29/src/main/kotlin/MinHash.kt#L313-L318) are computed for each document, compressed to a [4-byte representation](https://github.com/4d55397500/minhash-service/blob/9d9dae3508e8859527f47f67de27fc4bc2e19f29/src/main/kotlin/MinHash.kt#L324-L326). Minhashes are computed and stored along with a 'hashmap' reprentation in two BigQuery tables. These two tables facilitate a nearest neighbor lookup over the minhash representation.

![Architecture](./minhash_architecture.png)


### API
See [api doc](docs/api.md)

### *Disclaimer*
This system demonstrates large scale minhashing for document lookup using Google's Dataflow and BigQuery. If you wish to use deep learning you can train embeddings and use a nearest neighbor index on the vector representations for lookup.


