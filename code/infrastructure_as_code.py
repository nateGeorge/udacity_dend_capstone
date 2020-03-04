"""
This creates the AWS RedShift resources for the data warehouse.
"""
import os
import time
import json

import pandas as pd
import boto3


import configparser

class redshift_creator:
    """
    Creates AWS Redshift cluster programmatically.
    """
    def __init__(self,
                config_location='~/.aws_config/',
                config_filename='solar_dwh.cfg',
                connection_filename='solar_cluster.cfg',
                s3_access=False
                ):
        """
        config_location - string; location of config file
        config_filename - string; filename of config file
        connection_filename - string; filename of config file with connection details
        s3_access - boolean; if True, allows S3 read access for Redshift cluster
            (e.g. for importing data)
        """
        self.config_location = config_location
        self.config_filename = config_filename
        self.connection_filename = connection_filename
        self.s3_access = s3_access
        self.set_configs()
        self.create_api_connections()


    def set_configs(self):
        """
        Sets configuration variables from config file.
        path - string; path to config file
        fn - string; filename of config file
        """
        config = configparser.ConfigParser()
        # template cfg file is in repo; don't post your api key and secret online
        config_path = os.path.join(os.path.expanduser(self.config_location), self.config_filename)
        config.read_file(open(config_path))

        # API credentials for admin account
        self.KEY                    = config.get("AWS", "KEY")
        self.SECRET                 = config.get("AWS", "SECRET")

        # cluster specifications
        self.DWH_CLUSTER_TYPE       = config.get("DWH", "DWH_CLUSTER_TYPE")
        self.DWH_NUM_NODES          = config.get("DWH", "DWH_NUM_NODES")
        self.DWH_NODE_TYPE          = config.get("DWH", "DWH_NODE_TYPE")

        self.DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
        self.DWH_DB                 = config.get("CLUSTER", "DB_NAME")
        self.DWH_DB_USER            = config.get("CLUSTER", "DB_USER")
        self.DWH_DB_PASSWORD        = config.get("CLUSTER", "DB_PASSWORD")
        self.DWH_PORT               = config.get("CLUSTER", "DB_PORT")

        if self.s3_access:
            self.DWH_IAM_ROLE_NAME  = config.get("DWH", "DWH_IAM_ROLE_NAME")


    def create_api_connections(self, region='us-west-2'):
        """
        Create API connections to AWS EC2, IAM and redshift
        """
        self.ec2 = boto3.resource('ec2',
                        region_name=region,
                            aws_access_key_id=self.KEY,
                            aws_secret_access_key=self.SECRET
                            )

        self.s3 = boto3.resource('s3',
                        region_name=region,
                            aws_access_key_id=self.KEY,
                            aws_secret_access_key=self.SECRET
                            )

        self.iam = boto3.client('iam',
                            region_name=region,
                            aws_access_key_id=self.KEY,
                            aws_secret_access_key=self.SECRET
                            )

        self.redshift = boto3.client('redshift',
                            region_name=region,
                            aws_access_key_id=self.KEY,
                            aws_secret_access_key=self.SECRET
                            )


    def create_s3_role(self):
        """
        Creates an IAM role with S3 access for the redshift cluster
        """
        self.dwh_s3_role = self.iam.create_role(
            Path='/',
            RoleName=self.DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole', # sts = security token status
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )

        self.iam.attach_role_policy(RoleName=self.DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']


    def write_connection_cfg(self):
        """
        Writes config file which allows for connection to DB via psycopg2.
        """
        filepath = os.path.expanduser(os.path.join(self.config_location, self.connection_filename))
        with open(filepath, 'w') as f:
            f.write('[CLUSTER]\n')
            f.write('HOST=' + self.DWH_ENDPOINT + '\n')
            f.write('DB_NAME=' + self.DWH_DB + '\n')
            f.write('DB_USER=' + self.DWH_DB_USER + '\n')
            f.write('DB_PASSWORD=' + self.DWH_DB_PASSWORD + '\n')
            f.write('DB_PORT=' + self.DWH_PORT + '\n')
            if self.s3_access:
                f.write('\n')
                f.write('[IAM_ROLE]\n')
                f.write('ARN=' + self.DWH_ROLE_ARN)


    def create_redshift_cluster(self):
        """
        Creates Redshift cluster.  Writes new .cfg file with details for connecting via psycopg2.
        """
        iam_roles = []
        # get IAM ARN for creating cluster
        if self.s3_access:
            self.create_s3_role()
            s3_role_arn = self.iam.get_role(RoleName=self.DWH_IAM_ROLE_NAME)['Role']['Arn']
            iam_roles.append(s3_role_arn)

        try:
            response = self.redshift.create_cluster(        
                # add parameters for hardware
                ClusterType=self.DWH_CLUSTER_TYPE,
                NodeType=self.DWH_NODE_TYPE,
                NumberOfNodes=int(self.DWH_NUM_NODES),

                # add parameters for identifiers & credentials
                DBName=self.DWH_DB,
                ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER,
                MasterUsername=self.DWH_DB_USER,
                MasterUserPassword=self.DWH_DB_PASSWORD,
                
                # add parameter for role (to allow s3 access)
                IamRoles=iam_roles 
            )
            self.wait_until_cluster_ready()
            self.get_endpoint()
            self.enable_connections()
            if self.s3_access:
                self.get_iam_role()

            self.write_connection_cfg()
        except Exception as e:
            print(e)


    def RedshiftProps(self):
        """
        View Redshift cluster status.
        """
        properties = self.redshift.describe_clusters(ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        keysToShow = ["ClusterIdentifier",
                        "NodeType",
                        "ClusterStatus",
                        "MasterUsername",
                        "DBName",
                        "Endpoint",
                        "NumberOfNodes", 
                        "VpcId"]
        x = [(k, v) for k,v in properties.items() if k in keysToShow]
        return pd.DataFrame(data=x, columns=["Key", "Value"])


    def wait_until_cluster_ready(self):
        """
        Waits until Redshift cluster status is 'available'.
        """
        print('waiting for cluster to become available...')
        while True:
            df = self.RedshiftProps()
            available = (df[df['Key'] == 'ClusterStatus']['Value'].iloc[0] == 'available')
            if available:
                print('cluster ready.')
                break

            time.sleep(0.5)


    def get_endpoint(self):
        """
        Get host address for DB, necessary for connecting to DB.
        The endpoint is the host address used for psycopg2.
        """
        properties = self.redshift.describe_clusters(ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        self.DWH_ENDPOINT = properties['Endpoint']['Address']
    

    def get_iam_role(self):
        """
        Gets IAM role for copying from S3 buckets.
        """
        properties = self.redshift.describe_clusters(ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        self.DWH_ROLE_ARN = properties['IamRoles'][0]['IamRoleArn']


    def enable_connections(self, ip='0.0.0.0/0'):
        """
        Open TCP port for access to cluster endpoint.
        ip - string; ip address to allow access from.  0.0.0.0/0 is all IPs.
        """
        try:
            properties = self.redshift.describe_clusters(ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            vpc = self.ec2.Vpc(id=properties['VpcId'])
            defaultSg = list(vpc.security_groups.all())[0]
            print(defaultSg)
            
            defaultSg.authorize_ingress(
                GroupName='default',
                CidrIp=ip,
                IpProtocol='TCP',
                FromPort=int(self.DWH_PORT),
                ToPort=int(self.DWH_PORT)
            )
        except Exception as e:
            print(e)


    def delete_cluster(self, final_snapshot=False):
        """
        Deletes Redshift Cluster and IAM role.
        """
        # Function asks if you want a final snapshot, but redshift API asks if you want to skip it
        skip_snapshot = not final_snapshot
        self.redshift.delete_cluster(ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER,
                                    SkipFinalClusterSnapshot=skip_snapshot)

        if self.s3_access:
            self.iam.detach_role_policy(RoleName=self.DWH_IAM_ROLE_NAME,
                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
            self.iam.delete_role(RoleName=self.DWH_IAM_ROLE_NAME)
        
        # delete config file if exists
        filepath = os.path.expanduser(os.path.join(self.config_location, self.connection_filename))
        if os.path.exists(filepath):
            os.remove(filepath)