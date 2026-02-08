# Databricks notebook source
# MAGIC %pip install --quiet -U databricks-sdk==0.40.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.platform import NotFound
from databricks.sdk.service.sql import CreateWarehouseRequestWarehouseType
from databricks.sdk.service.sql import WarehouseAccessControlRequest
from databricks.sdk.service.sql import WarehousePermissionLevel

import pyspark.sql.functions as F
import json

class NestedNamespace:

    def __init__(self, dictionary: dict = None, prefix=None):
        prefix = prefix + '.' if prefix else ''
        self.__setattr_direct('dictionary', dictionary or dict())
        self.__setattr_direct('prefix', prefix)
        self.__setattr_direct('iterator', None)

    def __getattr__(self, name):
        name = self.prefix + name
        return self.dictionary.get(name, NestedNamespace(dictionary=self.dictionary, prefix=name))

    def __setattr__(self, name, value):
        name = self.prefix + name
        self.dictionary[name] = value

        # since we've overwritten the node in the tree, prune branch by deleting any children/ancestors
        name += '.'
        children = [k for k in filter(lambda x: x.startswith(name), self.dictionary.keys())]
        for k in children:
            del(self.dictionary[k])

    # bypass overridden behaviour to directly set attributes
    def __setattr_direct(self, name, value):
        super().__setattr__(name, value)

    def __repr__(self):
        args = [f"{key}='{self[key]}'" for key in self]
        return f"{self.__class__.__name__} ({', '.join(args)})" if args else ""

    def __iter__(self):
        self.__setattr_direct(
            'iterator',
            filter(
                lambda x: x.startswith(self.prefix),
                iter(self.dictionary)
            )
        )

        return self

    def __next__(self):
        return next(self.iterator).removeprefix(self.prefix) if self.iterator else None

    def __getitem__(self, name):
        return self.__getattr__(name)

    def __setitem__(self, name, value):
        return self.__setattr__(name, value)

class DBAcademyHelper(NestedNamespace):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.workspace = WorkspaceClient()

        try:
            default_catalog = self.workspace.settings.default_namespace.get().namespace.value
        except:
            default_catalog = 'impetuslearning'

        # meta = f'{default_catalog}.ops.meta'
        catalog = "impetuslearning"
        schema = None

        from py4j.protocol import Py4JJavaError
        from pyspark.errors import PySparkException

        # try:
        #     rows = spark.table(meta).collect()
        # except Py4JJavaError:
        #     raise Exception(f'Error accessing metadata table {meta}; are you using serverless or DBR >= 15.1?')
        # except PySparkException:
        #     raise Exception(f'Metadata table {meta} not found or accessible; are you running in a properly configured metastore?')

        # query the metadata table and populate self with key/values

        # Get the notebook context as a JSON string
        context_json = dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson()

        # Parse the JSON string into a Python dictionary
        context_dict = json.loads(context_json)

        # Access the 'user' attribute from the 'tags' or 'attributes' dictionary
        # The exact path might vary slightly depending on your Databricks runtime and configuration.
        # Common paths include 'tags.user' or 'attributes.user'.
        user_email = context_dict.get('tags', {}).get('user') or context_dict.get('attributes', {}).get('user')
        cluster_name = spark.conf.get("spark.databricks.clusterUsageTags.clusterName", "Unknown Cluster")
        volume_name = user_email.replace(".","_").replace("@","_")
        meta_table_name = 'meta'+user_email.split('@')[0].replace(".","_")
        config = {
            "username": user_email,
            "catalog_name": "impetuslearning",
            "schema_name": user_email.split('@')[0].replace(".","_"),
            "paths.working_dir": f"/Volumes/impetuslearning/ops/{volume_name}/{user_email.split('@')[0].replace(".","_")}",
            "cluster_name": cluster_name,
            # "warehouse_name": "shared_warehouse",
            "iam.secondary": user_email,
            "secrets": user_email,
            "pseudonym": user_email.split('@')[0].replace(".","_")
        }

        # try:
        #     created_warehouse = self.workspace.warehouses.create(
        #         name="shared_warehouse",
        #         warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
        #         cluster_size="2X-Small",  # Options: "X-Small", "Small", "Medium", etc.
        #         max_num_clusters=1,
        #         auto_stop_mins=5,
        #         enable_serverless_compute=True
        #     ).result()

        #     self.workspace.warehouses.set_permissions(warehouse_id= created_warehouse.id,access_control_list= [
        #         WarehouseAccessControlRequest(
        #             group_name="users",
        #             permission_level=WarehousePermissionLevel.CAN_USE
        #         )
        #     ])
        # except Exception as e:
        #     print(e)
        #     print(f"WARNING : If shared_warehouse does not exists ask the admin to create a warehouse with name :shared_warehouse at assign use permission on the same")

        token = self.workspace.tokens.create(comment=f"impetusLearning", lifetime_seconds=21600)
        token_value = token.token_value

        try:
            self.workspace.secrets.create_scope(user_email)
        except Exception as e:
            print(e)

        self.workspace.secrets.put_secret(
            scope=user_email,
            key="secondary_token",
            string_value=token_value
        )

        for key, value in config.items():
            setattr(self, key, value)
            if 'key' == 'catalog_name':
                catalog = value
            elif key == 'schema_name':
                schema = value


        # for row in rows:
        #     setattr(self, row['key'], row['value'])

        #     if row['key'] == 'catalog_name':
        #         catalog = row['value']
        #     elif row['key'] == 'schema_name':
        #         schema = row['value']

        try:
            spark.sql(f'CREATE CATALOG IF NOT EXISTS {catalog}')
            spark.sql(f'CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}')
            spark.sql(f'CREATE SCHEMA IF NOT EXISTS {catalog}.ops')
        except Exception as e:
            print(
                f"{e}\n"
                f"Do you have enough permissions?\n"
                f"Ask an admin to create the following resources and grant permission on them:\n"
                f"  - Catalog: {catalog}\n"
                f"  - Schema: {catalog}.{schema}\n"
                f"  - Table: {catalog}.ops"
            )
        
        try:
            spark.sql(f'CREATE OR REPLACE TABLE {catalog}.ops.{meta_table_name} (owner STRING, object STRING, key STRING, value STRING)')
            spark.sql(f'CREATE VOLUME IF NOT EXISTS {catalog}.ops.{volume_name}')
        except Exception as e:
            raise Exception(
                f"{e}\n"
                f"Do you have enough permissions?\n"
                f"Ask an admin to create the following resources and grant permission on them:\n"
                f"  - Catalog: {catalog}\n"
                f"  - Schema: {catalog}.ops\n"
                f"  - Table: {catalog}.ops.{meta_table_name}\n"
                f"  - Volume: {catalog}.ops.{volume_name}"
            ) from e


        for key, value in config.items():
            spark.sql(f'''INSERT INTO {catalog}.ops.{meta_table_name} VALUES ('{user_email}', NULL, '{key}', '{value}')''')

        # set default catalog and schema according to metadata
        if catalog:
            spark.sql(f'USE CATALOG {catalog}')

            if schema:
                spark.sql(f'USE SCHEMA {schema}')
    
    @staticmethod
    def uc_safename(name: str):
        # as per https://docs.databricks.com/en/sql/language-manual/sql-ref-names.html
        # - no periods, spaces, or forward slashes (we will replace those with _)
        # - no control characters (0x00 - 0x1f) or DELETE (0x7f) (we will omit those)
        # - all lowercase
        # - limited to 255 chars
        return ''.join(
            map(
                lambda x: '_' if x in ['.',' ','/'] else '' if ord(x) < 0x20 or ord(x) == 0x7f else x,
                name
            )
        ).lower()[0:255]

    # add an initializer. Initializers can be chained are are all called when DA.init() is called.
    # This pattern makes it easier to dynamically augment the class across cells or notebooks.
    # There's a couple ways to use this, but using as a function decorator is easiest:
    #   @DBAcademyHelper.add_init
    #   def init(self)
    #       ...
    # Alternatively:
    #   def init(self):
    #       ...
    #   DBAcademyHelper.add_init(init)
    #
    # When DA.init() is called, all initializers are called in the order they were added

    @classmethod
    def add_init(cls, function_ref):
        try:
            initializers = getattr(cls, '_initializers')
        except AttributeError:
            initializers = list()

        initializers += [function_ref]
        setattr(cls, '_initializers', initializers)
        return function_ref

    # add a class method (aka "monkey patch"). This pattern makes it easier to dynamically augment the class
    # across cells or notebooks.
    # There's a couple ways to use this, but this is easiest:
    #   @DBAcademyHelper.add_method
    #   def method(self)
    #       ...
    # Alternatively:
    #   def method(self):
    #       ...
    #   DBAcademyHelper.add_method(method)
    #
    # Ultimately, the new method can be called from within notebook code:
    #   DA.method()
    
    @classmethod
    def add_method(cls, function_ref):
        setattr(cls, function_ref.__name__, function_ref)
        return function_ref

    def init(self):

        for key in self:
            value = self[key]

            if value and type(value) == str:
                try:
                    spark.conf.set(f'DA.{key}', value)
                    spark.conf.set(f'da.{key}', value)
                except:
                    # fails on serverless
                    pass

        try:
            for i in getattr(self.__class__, '_initializers'):
                i(self)

        except AttributeError:
            pass

    def print_copyrights(self):
        datasets = self.datasets

        for i in datasets:
            catalog = datasets[i].split('.')[0]
            description = spark.sql(
                f'DESCRIBE CATALOG {catalog}'
            ).where(
                F.col('info_name') == 'Comment'
            ).select(
                'info_value'
            ).collect(
            )[0]['info_value']
            print(description)
    
    # Perform common lookups via the SDK. For example to find a structure's ID given a name. Example uses:
    # DA.workspace_find("catalogs", "main") -> return SDK structure representing catalog named "main"
    # DA.workspace_find("cluster_policies", "DBAcademy DLT") -> return structure representing named policy
    # Note: for this to work, SDK must have an API named by "item_type" with a "list" api, and it assumes you
    # want to look up based on a "name" element. But in cases all these conditions aren't true, you can use
    # "member" and "api" to tweak behaviour without having to implement your own lookup function. Some examples:
    # DA.workspace_find("clusters", "0913-023811-rzeq07rk", "cluster_id") -> returns cluster structure with
    # matching value of "cluster_id"
    # DA. workspace_find('pipelines', pipeline_name, api='list_pipelines') -> returns structure representing
    # the named DLT pipeline
    def workspace_find(
        self,
        item_type: str,
        value: str=None,
        member: str='name',
        api: str='list'
    ):
        # locate the API (item type), then grab the "list" method
        method = getattr(getattr(self.workspace, item_type), api)

        # iterate over the what the list() returned
        for item in method():
            if getattr(item, member) == value:
                return item

    def unique_name(self, sep: str) -> str:
        return self.pseudonym.replace(' ', sep)
    

    def display_config_values(self, config_values):
        """
        Displays list of key-value pairs as rows of HTML text and textboxes
        :param config_values: list of (key, value) tuples

        Returns
        ----------
        HTML output displaying the config values

        Example
        --------
        DA.display_config_values([('catalog',DA.catalog_name),('schema',DA.schema_name)])
        """
        html = """<table style="width:100%">"""
        for name, value in config_values:
            html += f"""
            <tr>
                <td style="white-space:nowrap; width:1em">{name}:</td>
                <td><input type="text" value="{value}" style="width: 100%"></td></tr>"""
        html += "</table>"
        displayHTML(html)

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT ALL PRIVILEGES ON CATALOG impetuslearning TO `account users`;
# MAGIC GRANT MANAGE ON CATALOG impetuslearning TO `account users`;
# MAGIC GRANT USE CATALOG ON CATALOG impetuslearning TO `account users`;