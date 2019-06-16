package foo

import java.util.Date;
import hello.MyJavaClass;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String



final class ScalaClass(aString: String, val anInteger: Int) {
    def this(aBool: Boolean) {
        this("defaultString", -1)
    }

    val theString = "theString"

    var someString = "some"

    def plus(x: Int, y: Int): Int = x + y
    
    def getRow(x: String, y: String):InternalRow = {
      val cls = new MyJavaClass
      cls.doIt("ooooops");
      println("Hello World");
//    print(i2);      
//    x + y + new Date
      val i1 = InternalRow.empty
      print(i1);
      InternalRow(0, "mystring", (1, "mypair"));
    }
    
    def getSeq(x: String, y: String):Seq[Int] = {
//      Seq(0, "string", (0, "pair"))
      Seq(3, 4, 5)
    }

    def getInternalRow(x: String, y: String):InternalRow = {
//      Seq(0, "string", (0, "pair"))
      val seq = Seq(UTF8String.fromString("1"),UTF8String.fromString("2"));
//      val seq = Seq("1","2")
      InternalRow.fromSeq(seq);
    }

    def toInternalRow(x: String, y: String):InternalRow = {
      val seq = Seq(UTF8String.fromString(x),5);
//      val seq = Seq(UTF8String.fromString("7"),UTF8String.fromString("8"));
      InternalRow.fromSeq(seq);
    }
    
    

}