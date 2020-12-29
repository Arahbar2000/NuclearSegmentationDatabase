""" A list of examples on how to use the DB Access methods to insert and retrieve
data from an AJB Database in Oracle Cloud.
"""
import os
import sys
import dbaccess_rest as dba
import numpy as np

USER = os.getenv("AJDUSER", None)
PASSWORD = os.getenv("AJDPASS", None)

COLL_NAME = "b123___p123__20201230"

if PASSWORD is None:
    print("No password defined to run the code")
    sys.exit(1)

if USER is None:
    print("No user name defined to run the code")


def example_add_item():
    """Add a single document to the collection."""
    my_collection = "b123__p123__20201010"
    jdb = dba.JsonDatabase(user=USER, password=PASSWORD, collection_name=my_collection, create_coll=True)
    item = generate_one_item()

    jdb.add_item(item)
    print("Item has been added to {}".format(my_collection))


def example_add_multiple_items():
    """ Add multiple documents to the database."""
    my_collection = "b123__p123__20201011"
    it1 = generate_one_item()
    it2 = generate_one_item()
    items = [it1, it2]

    jdb = dba.JsonDatabase(user=USER, password=PASSWORD, collection_name=my_collection, create_coll=True)

    jdb.add_multiple_items(items_list=items)
    print("Items have been added to {}".format(my_collection))


def example_create_collection():
    """ Create a collection. """
    dba.JsonDatabase(user=USER, password=PASSWORD, collection_name=COLL_NAME, create_coll=True)


def example_delete_collection():
    """ Delete a collection. """

    # 1. Create a collection
    my_collection = "test_coll_dec29"
    mydb = dba.JsonDatabase(user=USER, password=PASSWORD, collection_name=my_collection, create_coll=True)

    # 2. Show available collections
    print("List after creating a test collection:")
    for coll in mydb.list_collections():
        print(coll)

    # 3. Delete the recent collection
    mydb.delete_collection()

    # 4. Show available collections
    print("List after deleting the test collection:")
    for coll in mydb.list_collections():
        print(coll)


def example_add_and_retrieve_data():
    # 1. Create a collection
    my_collection = "b123__p123__20201012"
    mydb = dba.JsonDatabase(user=USER, password=PASSWORD, collection_name=my_collection, create_coll=True)

    # 2. Add items to the collection
    items = list()
    items.append(generate_one_item_at_layer(0))
    items.append(generate_one_item_at_layer(0))
    items.append(generate_one_item_at_layer(0))
    items.append(generate_one_item_at_layer(1))

    mydb.add_multiple_items(items_list=items)

    # 3. Retrieve items
    for layer in [0, 1, 2]:
        print("---- layer {} ------".format(layer))
        items = mydb.extract_items(z_layer=layer)
        for item in items:
            print(item)


def example_list_collections():
    """ Extract a list of collections."""
    jdb = dba.JsonDatabase(user=USER, password=PASSWORD, collection_name=COLL_NAME)
    colls = jdb.list_collections()
    print("List of collections:")
    print("\n".join(colls))


def example_add_metadata():
    """ Add metadata to a collection."""
    my_collection = "b123__p123__20201015"
    jdb = dba.JsonDatabase(user=USER, password=PASSWORD, collection_name=my_collection, create_coll=True)

    meta = {
        "block_name": "b123",
        "patch_id": "p123",
        "registration_version": "20201015",
        "segmentation_model": "UXYZ123",
        "deduplication_method": "M2_V1",
        "roi_size": [2048, 2048],
        "roi_offset": [1024, 1024]
    }

    jdb.add_metadata(metadata_dict=meta)


def example_read_metadata():
    """ Read the metadata for a collection."""
    my_collection = "b123__p123__20201015"
    jdb = dba.JsonDatabase(user=USER, password=PASSWORD, collection_name=my_collection)
    meta_dict = jdb.extract_metadata()
    print("Extracted metadata:")
    print(meta_dict)


def example_add_metadata_twice():
    """ This example should return an error because there is only one metadata document per collection"""
    my_collection = "b123__p123__20201017"
    jdb = dba.JsonDatabase(user=USER, password=PASSWORD, collection_name=my_collection, create_coll=True)

    meta = {
        "block_name": "b123",
        "patch_id": "p123",
        "registration_version": "20201015",
        "segmentation_model": "UXYZ123",
        "deduplication_method": "M2_V1",
        "roi_size": [2048, 2048],
        "roi_offset": [1024, 1024]
    }

    jdb.add_metadata(metadata_dict=meta)
    print("Added metadata for the first time")
    jdb.add_metadata(metadata_dict=meta)
    print("Added metadata a second time")


def generate_one_item():
    """Generate a dummy item for database insert operations. It generates an item with different area,
    perimeter, x, y, and z values."""
    target_area = int(np.random.uniform(400, 600))
    target_perimeter = int(np.random.uniform(100, 220))
    target_x = int(np.random.uniform(0, 2048))
    target_y = int(np.random.uniform(0, 2048))
    target_z_layer = int(np.random.uniform(0, 3))

    item = dict()
    item["type"] = "Feature"
    item["geometry"] = {"type": "Point", "coordinates": [target_x, target_y, target_z_layer]}
    item["properties"] = {
        "rle": "2098177 8 2100225 8 2102273 8 2104321 8 2106369 8 2108417 8 2110465 8 2112513 8",
        "area": target_area,
        "perimeter": target_perimeter,
    }
    return item


def generate_one_item_at_layer(z_layer):
    """Generate a dummy item for database insert operations. It generates an item with random area,
    perimeter, x, and y. The z coordinate is defined by the z_layer argument."""
    target_area = int(np.random.uniform(400, 600))
    target_perimeter = int(np.random.uniform(100, 220))
    target_x = int(np.random.uniform(0, 2048))
    target_y = int(np.random.uniform(0, 2048))
    target_z_layer = z_layer

    item = dict()
    item["type"] = "Feature"
    item["geometry"] = {"type": "Point", "coordinates": [target_x, target_y, target_z_layer]}
    item["properties"] = {
        "rle": "2098177 8 2100225 8 2102273 8 2104321 8 2106369 8 2108417 8 2110465 8 2112513 8",
        "area": target_area,
        "perimeter": target_perimeter,
    }
    return item


if __name__ == "__main__":

    # Add an item to the database
    # example_add_item()

    # Create a collection called testcoll
    # example_create_collection()

    # Create and delete a collection
    # example_delete_collection()

    # List collections
    example_list_collections()

    # Insert multiple items
    # example_add_multiple_items()

    # Extract item IDs
    # example_add_and_retrieve_data()

    # Add metadata
    # example_add_metadata()
    # print("Metadata has been added. ")
    # example_read_metadata()

    # Attempt to add metadata twice. It should return an error.
    # example_add_metadata_twice()
