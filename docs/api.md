API
----


* **URL**

  `/dataflow`

* **Method:**
  

	`POST`
  
*  **URL Params**

   **Required:**
 	    
   **Optional:**
 

* **Data Params**

	```
	[
	 { key: 'fooKey',
	   documentPath: 'gs://.../doc.txt'
	 },
	 ...
	]
	```
* **Sample Call:**

	```
	POST http://localhost:8080/dataflow
	Content-Type: application/json
	
	[
	 { key: 'fooKey',
	   documentPath: 'gs://.../doc.txt'
	 },
	 ...
	]
	```
* **Response:**

--

* **URL**

  `/localsearch`

* **Method:**
  

	`POST`
  
*  **URL Params**

   **Required:**
 	    
   **Optional:**
 

* **Data Params**

	```
	["docKey1", "docKey2", ...]
	```
* **Sample Call:**

	```
	POST http://localhost:8080/localsearch
	Content-Type: application/json
	
	["docKey1", "docKey2", ...]
	```
* **Response:**

	```
	{"doc301":[{"docKey":"doc483","score":7},{"docKey":"doc318","score":7},{"docKey":"doc773","score":6},{"docKey":"doc768","score":6},
	...
	```
