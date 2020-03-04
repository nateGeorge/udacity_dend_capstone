from infrastructure_as_code import redshift_creator

rc = redshift_creator()
rc.delete_cluster()