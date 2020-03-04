from infrastructure_as_code import redshift_creator

rc = redshift_creator(s3_access=True)
rc.create_redshift_cluster()