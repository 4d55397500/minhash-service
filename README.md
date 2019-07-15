min-hash
----

[![Build Status](https://travis-ci.org/4d55397500/dataflow-minhash.svg?branch=master)](https://travis-ci.org/4d55397500/minhash-service)

A large scale min-hashing service for documents. Currently a Google Cloud Dataflow job converts to documents to minhash representations and stores those representations along with partial projections for 'hashmap lookup' in BigQuery tables. 

A form of [nearest neighbor search](https://github.com/4d55397500/minhash-service/blob/9d9dae3508e8859527f47f67de27fc4bc2e19f29/src/main/kotlin/LocalSearch.kt#L14-L31) is implemented directly in BigQuery. Typically in nearest neighbor search, a hashmap maps each document into one bucket for each partial hash in the minhash representation. In this case, we mimic the hashmap in the BigQuery column store representation by a join, filter and then group by. 

### *Disclaimer*
As of 2019 document similarity search is much better had by training embeddings and using an efficient nearest neighbors index on the vector representations. This project rather is intended to demonstrate the use of BigQuery and Dataflow for large scale operations-free min-hashing.

### Background
See the wikipedia [article](https://en.wikipedia.org/wiki/MinHash) on min-hashing.

### Design
[Shingles](https://github.com/4d55397500/minhash-service/blob/9d9dae3508e8859527f47f67de27fc4bc2e19f29/src/main/kotlin/MinHash.kt#L313-L318) are computed for each document, compressed to a [4-byte representation](https://github.com/4d55397500/minhash-service/blob/9d9dae3508e8859527f47f67de27fc4bc2e19f29/src/main/kotlin/MinHash.kt#L324-L326). Minhashes are computed and stored along with a 'hashmap' reprentation in two BigQuery tables, as shown below.

One with the min hashes

<img src="minhashes.png" width="200"/>


The other with min hash projections for looking up neighbors (similar documents)

<img src="partialhashes.png" width="400"/>

(nice architecture doodle goes here)

### Run

instructions to be added


### API
