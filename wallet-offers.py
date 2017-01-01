#!/usr/bin/python
import time
import sys

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

## Configuration
contactpoints = ['10.0.0.252']

# This will be ignored if your cluster does not have authentication enabled
auth_provider = PlainTextAuthProvider (username='username', password='Password123')

# Will need to update wallet_new_offer.scala if you change this
keyspace = "wallet"

## End Configuration

print "Connecting to cluster"

cluster = Cluster( contact_points=contactpoints,
                   auth_provider=auth_provider )

session = cluster.connect()

query1 = "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;" % (keyspace)
query2 = "CREATE TABLE IF NOT EXISTS %s.wallet_by_id ( id timeuuid, rev timeuuid, createdtime text, deleted boolean, email text, firstname text, hashedemail text, lastname text, loyalityid text, profileid text, updatedtime text, PRIMARY KEY (id, rev));" % (keyspace)
query3 = "CREATE TABLE IF NOT EXISTS %s.wallet_offers ( ownedbywallet text, id text, createdtime text, deleted text, effectivebegindate text, effectiveenddate text, itemid text, otype text, promocode text, shareable text, supcused text, updatedtime text, PRIMARY KEY (ownedbywallet, id));" % (keyspace)
query4 = "CREATE TABLE IF NOT EXISTS %s.wallet_newitems ( id text PRIMARY KEY, createdtime text, deleted boolean, effectivebegindate text, effectiveenddate text, itemid text, promocode text, shareable text, supcused text, type text, updatedtime text);" % (keyspace)
query5 = "INSERT INTO %s.wallet_newitems (id , effectiveenddate, createdtime, deleted, effectivebegindate, itemid, promocode, shareable, supcused, type, updatedtime) VALUES ( '1','12/30/2017','12/25/2016', false, '12/31/2016', '1', 'NEW ITEM', 'no', 'something', 'coupon', '12/27/2016');" % (keyspace)

print "Creating keyspace and tables"
session.execute(query1)
session.execute(query2)
session.execute(query3)
session.execute(query4)
print "Creating new offer"
session.execute(query5)

print "Creating wallets (CTRL+C to stop)"
id = 1
count = 1
loop = 1
while loop == 1:
   query = "INSERT INTO %s.wallet_by_id (id, firstName, lastName, hashedEmail, profileId, loyalityId, createdTime, updatedTime, deleted, rev) VALUES (now(), 'Russ2', 'Katz2', 'russ@email.com', 'p12346', 'l12346', '12/30/2016', '12/30/2016', false, now());" % (keyspace)
   session.execute(query)
   id = id + 1
   count = count + 1
   if count == 10000:
      print "Wallets created: ", id
      count = 0

cluster.shutdown()
sys.exit(0)
