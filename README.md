# Wallet offers spark batch
1. Read in all wallet IDs from wallets_by_id
2. Read in new offer from wallet_newoffers
3. Write new offer for each wallet ID into wallet_offers

# Pre-reqs
1. DSE installed and running in Analytical mode
2. Python with the Datastax Python drivers installed

# How-to
1. Edit contactpoints in wallet-offers.py to point to your cluster.
2. Optional: Edit AuthProvider with username and password in wallet-offers.py
3. Run ./wallet-offers.py to create keyspace+tables and generate wallets.
  * The longer it runs the more wallets it will create
4. Login to DSE Anaytical node and run `dse spark`
5. Paste wallet_new_offer.scala into the Spark prompt
6. Eat a sandwich
7. See new offers created in wallet_offers, one for each wallet.

# Notes

* I am sure this is NOT the best way to do this.
* Feel free to tell me how I can optimize anything!
* I'd like to write all the new wallet_offers rows into a RDD/DF and THEN iterate through and saveToCassandra, instead of doing saveToCassandra in the for loop.. but I couldn't work out how :)
