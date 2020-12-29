## Access Library for a Nuclear Jason Database

### Definitions
A **patch** represents a tissue image of size 4096x4096

A **segmented patch** is a binary image showing locations of nuclei 
in a patch. Nuclei segments have a value of 1 while the background has 
a value of 0

A **3D patch** is a group of layered patches representing a 3D volume 
of tissue.

**Autonomous Json Database (AJB)** is a managed database in Oracle Cloud 
dedicated to the storage of documents and collections of documents. 

A **collection** is a group of documents identified by a collection name. 
In our case, a collection stores information about all segments in a 3D patch.

A **document** is a JSON object, which in our case, stores information 
about a single segment. 

**Metadata** represents data about the segmentation run. There is one metadata 
document in the database for each 3D patch. It includes information 
such as block name, patch ID, model name, and others.

### Objective
Provide an API set to store data and retrieve nuclear data from AJD

### Usage
Download the files `constant.py` and `dbaccess_rest.py` and use the 
class and methods to access the Nuclear AJD that has been set up in 
Oracle Cloud. The file `examples.py` shows 
examples of using the class and methods.

Access to the database requires a user name and a password configured 
as environment variables.

### Available operations
1. Start a collection (option available at class instantiation)

2. List all collections in the database

3. Delete a collection

4. Add one document (data from one segment) to the collection

5. Add multiple documents (data from multiple segments) to the collection

6. Retrieve all nuclear segment entries for a z layer

7. Add a metadata document to a collection

8. Read the metadata document for a collection

9. Extract tile data (allows reading patch data tile by tile). This should be 
the preferred method to read patch data

### Naming and Format conventions

The names of fields and data representation formats follow the Segmentation Pipeline specifications: 
https://www.dropbox.com/sh/uc6i1pcp7jra0hh/AADfpmX9Xw0MAXuZju7_-gova?dl=0




