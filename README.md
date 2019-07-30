minhash-service
----


[![Build Status](https://travis-ci.org/4d55397500/dataflow-minhash.svg?branch=master)](https://travis-ci.org/4d55397500/minhash-service)


A large scale managed min-hashing service for documents. 


### Background
See the wikipedia [article](https://en.wikipedia.org/wiki/MinHash) on min-hashing.

### Design
A Google Cloud Dataflow job converts documents to minhash representations and stores those representations along with partial minhash projections in BigQuery tables.  The documents are first tokenized as [shingles](https://github.com/4d55397500/minhash-service/blob/9d9dae3508e8859527f47f67de27fc4bc2e19f29/src/main/kotlin/MinHash.kt#L313-L318), with shingles compressed to a [4-byte representation](https://github.com/4d55397500/minhash-service/blob/9d9dae3508e8859527f47f67de27fc4bc2e19f29/src/main/kotlin/MinHash.kt#L324-L326). The resulting two BigQuery tables facilitate a [nearest neighbor search](https://github.com/4d55397500/minhash-service/blob/9d9dae3508e8859527f47f67de27fc4bc2e19f29/src/main/kotlin/LocalSearch.kt#L14-L31) as a join, filter and then group by. 


![Architecture](./minhash_architecture.png)


### API
See the [API doc](docs/api.md).

### To Do
Add a UI.

#### *Disclaimer*
This system is intended to demonstrate minhashing for large scale document search. If you wish to use deep learning instead you can train embeddings and use a nearest neighbor index on the vector representations for lookup.


