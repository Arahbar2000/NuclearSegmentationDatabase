""" Autonomous JSON Database (AJD) access using REST APIs.

Define a class and helper functions to store and retrieve data from
a managed JSON database from Oracle. The APIs defined here use SODA
commands exchanged using a REST protocol.
"""
import sys
import requests
import json
import copy
import constants as ct


class DatabaseUnableToCreate(Exception):
    def __init__(self, message):
        super(DatabaseUnableToCreate, self).__init__(message)


class DatabaseUnableToExtractData(Exception):
    def __init__(self, message):
        super(DatabaseUnableToExtractData, self).__init__(message)


class DatabaseUnableToAccess(Exception):
    def __init__(self, message):
        super(DatabaseUnableToAccess, self).__init__(message)


class DatabaseUnableToDelete(Exception):
    def __init__(self, message):
        super(DatabaseUnableToDelete, self).__init__(message)


class DatabaseUnableToAddItem(Exception):
    def __init__(self, message):
        super(DatabaseUnableToAddItem, self).__init__(message)


class DatabaseCollectionExists(Exception):
    def __init__(self, message):
        super(DatabaseCollectionExists, self).__init__(message)


class DatabaseTooManyMetadataDocs(Exception):
    def __init__(self, message):
        super(DatabaseTooManyMetadataDocs, self).__init__(message)


class DatabaseMetadataAlreadyExists(Exception):
    def __init__(self, message):
        super(DatabaseMetadataAlreadyExists, self).__init__(message)


class JsonDatabase:
    """Class that provides access to the Nuclear Json DB in Oracle Cloud.
    Args:
        user: the name of a user with authorization to access the database
        password: the user password
        collection_name: the name of the target collection
        create_coll: A boolean flag. If True, it creates a collection. If it exists, it
           raises an error. If the flag is False, the named collection should be available
           prior to instantiation.
    """

    def __init__(self, user, password, collection_name, create_coll=False, overwrite=False):
        self.__user = user
        self.__pword = password
        self.__collname = collection_name

        url_tail = user + "/soda/latest/" + collection_name
        self.__baseurl = ct.DB_BASE_URL + url_tail

        if create_coll:
            self.__create_collection(overwrite)

    def __create_collection(self, overwrite):
        """Create a new collection in the database"""
        all_colls = self.list_collections()
        if self.__collname in all_colls:
            if overwrite:
                self.delete_collection()
            else:
                raise DatabaseCollectionExists(
                    "{} already exists".format(self.__collname))
        resp = requests.put(url=self.__baseurl,
                            auth=(self.__user, self.__pword))
        if resp.status_code > 299:
            error_desc = str(resp.status_code) + " " + resp.reason
            raise DatabaseUnableToCreate(error_desc)
        else:
            return 1

    def list_collections(self, only_names=True):
        """List all the collections available in the database.
        Returns:
            A list of collection names if the argument only_names is True. Otherwise, it returns
            a dictionary with schema info for all the database collections
        """
        url_tail = self.__user + "/soda/latest"
        resp = self.__make_get_request(target_url=ct.DB_BASE_URL + url_tail)

        if resp.get("status") == "ok":
            resp_dict = json.loads(resp.get("data"))
            if only_names:
                items = resp_dict.get("items", None)
                if items is None:
                    return []
                else:
                    names = [it.get("name") for it in items]
                    return names
            else:
                return resp_dict
        else:
            error_msg = str(resp.get("code")) + " " + resp.get("reason")
            raise DatabaseUnableToAccess(message=error_msg)

    def delete_collection(self):
        """Delete a collection. Returns 1 if successful. It raises an error if there is
        a problem.
        """
        resp = self.__make_delete_request(target_url=self.__baseurl)
        if resp["status"] == "error":
            error_msg = str(resp.get("code")) + " " + resp.get("reason")
            raise DatabaseUnableToDelete(message=error_msg)
        else:
            return 1

    def add_item(self, item_dict):
        """Add an item (document) to the database. The item is defined as a dictionary that will be saved as a
        json object in the database. Hence, the dictionary must be such that it can be translated
        into a json object. This method returns 1 if successful. Otherwise it raises an exception.
        """
        resp = self.__make_post_request(
            target_url=self.__baseurl, payload_dict=item_dict)
        if resp['status'] == "error":
            error_msg = str(resp.get("code")) + " " + resp.get("reason")
            raise DatabaseUnableToAddItem(message=error_msg)
        else:
            return 1

    def add_metadata(self, metadata_dict):
        """Add metadata to the collection. The metadata object is a list of key/value pairs stored
        in metadata_dict. This method returns 1 if successful. Otherwise it raises an exception.
        A collection can have only 1 metadata document. An attempt to create a second metadata
        document raises an error.
        """
        formatted_dict = dict()
        formatted_dict["type"] = "Metadata"
        formatted_dict["content"] = copy.deepcopy(metadata_dict)

        current_meta = self.extract_metadata()

        if current_meta:
            error_msg = "A collection can have only 1 metadata document."
            raise DatabaseMetadataAlreadyExists(error_msg)

        resp = self.__make_post_request(
            target_url=self.__baseurl, payload_dict=formatted_dict)
        if resp['status'] == "error":
            error_msg = str(resp.get("code")) + " " + resp.get("reason")
            raise DatabaseUnableToAddItem(message=error_msg)
        else:
            return 1

    def extract_metadata(self):
        extract_url = self.__baseurl + "?action=query"
        qdata = {"type": {"$eq": "Metadata"}}
        resp = self.__make_post_request(
            target_url=extract_url, payload_dict=qdata)

        if resp['status'] == 'ok':
            returned_data = json.loads(resp.get("data"))
            all_items = returned_data.get("items")
            count = returned_data.get("count")

            if count > 1:
                error_msg = "Found {} metadata documents. Only one is allowed.".format(
                    count)
                raise DatabaseTooManyMetadataDocs(error_msg)

            elif count == 0:
                return {}
            else:
                extracted = all_items[0].get("value").get("content")
                return extracted
        else:
            error_msg = str(resp.get("code")) + " " + resp.get("reason")
            raise DatabaseUnableToExtractData(message=error_msg)

    def add_multiple_items(self, items_list):
        """Add multiple items (documents) to the database. An item_list contains a list of items. Each
        item is a dictionary that will be saved as a json object in the database. Hence, the dictionary
        must be such that it can be translated into a json object.
        Returns 1 if successful. Otherwise it raises an error.
        """
        insert_url = self.__baseurl + "?action=insert"
        resp = self.__make_post_request(
            target_url=insert_url, payload_dict=items_list)
        if resp['status'] == "error":
            error_msg = str(resp.get("code")) + " " + resp.get("reason")
            raise DatabaseUnableToAddItem(message=error_msg)
        else:
            return 1

    def extract_items(self, z_layer):
        """ Extract nuclear items at a given z layer"""
        offset = 0
        has_more = True
        items = []
        while has_more:
            extract_url = self.__baseurl + \
                "?action=query&offset={}".format(offset)
            qdata = {"geometry.coordinates[2]": {"$eq": z_layer}}
            resp = self.__make_post_request(
                target_url=extract_url, payload_dict=qdata)
            if resp['status'] == 'ok':
                returned_data = json.loads(resp.get("data"))
                all_items = returned_data.get("items")
                count = returned_data.get("count")
                has_more = returned_data.get("hasMore")
                offset += count

                if count > 0:
                    extracted = [copy.deepcopy(it.get("value"))
                                 for it in all_items]
                    items += extracted
            else:
                error_msg = str(resp.get("code")) + " " + resp.get("reason")
                raise DatabaseUnableToExtractData(message=error_msg)
        return items

    def extract_tile_data(self, z_layer, x0, y0, tile_size=256):
        """Extract nuclear data for a tile of a given size whose origin is at (x0, y0).
        Note: This function uses the $between comparison operator to check if a point belongs to
        a tile. We may want to replace this operation and use the $within operator for GeoJson data. It
        might be a faster way to perform queries.
        """
        xf = x0 + tile_size - 1
        yf = y0 + tile_size - 1
        offset = 0
        has_more = True
        items = []
        while has_more:
            extract_url = self.__baseurl + \
                "?action=query&offset={}".format(offset)
            qdata = {"geometry.coordinates[2]": {"$eq": z_layer},
                     "geometry.coordinates[0]": {"$between": [x0, xf]},
                     "geometry.coordinates[1]": {"$between": [y0, yf]},
                     }
            resp = self.__make_post_request(
                target_url=extract_url, payload_dict=qdata)
            if resp['status'] == 'ok':
                returned_data = json.loads(resp.get("data"))
                all_items = returned_data.get("items")
                count = returned_data.get("count")
                has_more = returned_data.get("hasMore")
                offset += count

                if count > 0:
                    extracted = [copy.deepcopy(it.get("value"))
                                 for it in all_items]
                    items += extracted
            else:
                error_msg = str(resp.get("code")) + " " + resp.get("reason")
                raise DatabaseUnableToExtractData(message=error_msg)
        return items

    def extract_region(self, z_layer, x0, y0, xf, yf):
        """Extract nuclei data for a specified bounding box at specified layer. 
        The top left coordinate of the bounding box is (x0, y0) and the bottom 
        right coordinate is (xf, yf).
        """
        offset = 0
        has_more = True
        items = []
        while has_more:
            extract_url = self.__baseurl + \
                "?action=query&offset={}".format(offset)
            qdata = {"geometry.coordinates[2]": {"$eq": z_layer},
                     "geometry.coordinates[0]": {"$between": [x0, xf]},
                     "geometry.coordinates[1]": {"$between": [y0, yf]},
                     }
            resp = self.__make_post_request(
                target_url=extract_url, payload_dict=qdata)
            if resp['status'] == 'ok':
                returned_data = json.loads(resp.get("data"))
                all_items = returned_data.get("items")
                count = returned_data.get("count")
                has_more = returned_data.get("hasMore")
                offset += count
                if count > 0:
                    extracted = [copy.deepcopy(it.get("value"))
                                 for it in all_items]
                    items += extracted
            else:
                error_msg = str(resp.get("code")) + " " + resp.get("reason")
                raise DatabaseUnableToExtractData(message=error_msg)
        return items

    def __make_post_request(self, target_url, payload_dict):
        # Note: the json argument automatically sets the content-type header to application/json
        resp = requests.post(url=target_url, auth=(
            self.__user, self.__pword), json=payload_dict)
        if 200 <= resp.status_code <= 299:
            return {"status": "ok", "code": resp.status_code, "data": resp.text}
        else:
            return {"status": "error", "code": resp.status_code, "reason": resp.reason}

    def __make_get_request(self, target_url):
        resp = requests.get(url=target_url, auth=(self.__user, self.__pword))
        if 200 <= resp.status_code <= 299:
            return {"status": "ok", "code": resp.status_code, "data": resp.text}
        else:
            return {"status": "error", "code": resp.status_code, "reason": resp.reason}

    def __make_delete_request(self, target_url):
        resp = requests.delete(
            url=target_url, auth=(self.__user, self.__pword))
        if resp.status_code == 200:
            return {"status": "ok", "code": resp.status_code}
        else:
            return {"status": "error", "code": resp.status_code, "reason": resp.reason}
