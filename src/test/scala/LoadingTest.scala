import com.datastax.driver.core._
import util.Random
val cluster=Cluster.builder().addContactPoint("127.0.0.1").build()
val session=cluster.connect("prova")
val insert=session.prepare("INSERT INTO prova.data_d8tree (key , rand , value ) VALUES (?,?,?);")
1.to(1000000).map{
  case i=> 
    println(s"inserting $i")
    session.execute(insert.bind(1.to(10).map(_=>Random.nextInt(7)).mkString(""),Random.nextInt():java.lang.Integer,s"insert $i"))
}

