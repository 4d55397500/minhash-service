min-hash
----

[![Build Status](https://travis-ci.org/4d55397500/dataflow-minhash.svg?branch=master)](https://travis-ci.org/4d55397500/minhash-service)

A large scale min-hashing service for documents. Currently a Google Cloud Dataflow job converts to documents to minhash representations and stores those representations along with partial projections for 'hashmap lookup' in BigQuery tables.

### Background
See the wikipedia article on min-hashing.

### Design

Currently the two BigQuery tables look like below. With appropriate SQL queries (to be added) one can find efficiently find similar documents.


One with the min hashes

<img src="minhashes.png" width="200"/>


The other with min hash projections for looking up neighbors (similar documents)

<img src="partialhashes.png" width="400"/>

(nice architecture doodle goes here)

### Run

instructions to be added







