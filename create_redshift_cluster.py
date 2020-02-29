from infrastructure_as_code import redshift_creator

rc = redshift_creator()
rc.create_redshift_cluster()
# rc.delete_cluster()