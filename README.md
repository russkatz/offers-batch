# Wallet offers spark batch
1. Read in all wallet IDs from wallets_by_id
2. Read in new offer from wallet_newoffers
3. Write new offer for each wallet ID into wallet_offers

# how-to
1. Edit contact point(s) wallet-offers.py
2. Run ./wallet-offers.py to create keyspace+tables and generate wallets.
  *The longer it runs the more wallets it will create
3. Login to DSE Anaytical node and run `dse spark`
4. Paste wallet_new_offer.scala 
