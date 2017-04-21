# Bootstrap

The Bootstrap and Bootstrap Google Cloud Functions allow a cluster of Transicator Change Servers to backup and restore 
from a Google Cloud Store. This helps avoid a potential issue when scaling up the Change Server cluster where existing 
clients may attach to the new Change Server, receive a "snapshot too old" message because the new Change Server
doesn't have all necessary context, and force the clients to request new snapshots.

The algorithm at this point is simple: If the backup file age > minBackupAge configured on the bootstrap, the backup 
offered by the Change Server is accepted and stored. A future enhancement may be to consider a delta of changes made 
between backups or similar. 

## Using

1. Install the Google Cloud Functions as detailed in [cloud_functions/README.md]()
2. Configure Change Server to point to the correct URLs.

## Testing

1. As above, install the Google Cloud Functions.
2. If you haven't set the correct REGION and PROJECT_ID env vars in your terminal already, do so now.  
3. Run `go test`

Note: The Tests may not always succeed 100% do to the asynchronous and distributed nature of Google Cloud Functions and 
Google Cloud Storage.
