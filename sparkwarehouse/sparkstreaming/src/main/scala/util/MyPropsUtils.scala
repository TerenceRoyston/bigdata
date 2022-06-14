package util

import java.util.ResourceBundle

/**
 * @author XuBowen
 * @date 2022/6/11 12:26
 */
object MyPropsUtils {
    private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

    def apply(propsKey: String) : String ={
        bundle.getString(propsKey)
    }

    def main(args: Array[String]): Unit = {
        println(MyPropsUtils("es.host"))
    }
}
