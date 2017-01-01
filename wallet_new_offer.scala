case class WalletItem(ownedbywallet: String, id: String, effectiveenddate: String, createdtime: String, deleted: String, effectivebegindate: String, itemid: String, promocode: String, shareable: String, supcused: String, otype: String, updatedtime: String)

var ids = sc.cassandraTable("wallet","wallet_by_id").select("id").as((i:String) => (i)).collect()

var offer = sc.cassandraTable("wallet","wallet_newoffers").where("id = '1'").as((id:String,ed:String,cd:String,d:String,ebd:String,ii:String,p:String,s:String,sup:String,t:String,ut:String) => (id,ed,cd,d,ebd,ii,p,s,sup,t,ut)).collect()

for (x <- ids) yield offer.map{ case (a,b,c,d,e,f,g,h,i,j,k) => sc.parallelize(Seq((x,a,b,c,d,e,f,g,h,i,j,k))).saveToCassandra("wallet","wallet_offers")}
