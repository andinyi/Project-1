import java.io.File
import java.nio.file.FileAlreadyExistsException

class apiJsonGet(apiUrl:String){
  def getJson(jsonName:String):Unit = {
    val data = requests.get(apiUrl)
    val text = data.text()
    val json = ujson.read(text)
    try {
      os.write(os.pwd/jsonName, json)
    } catch {
      case e: FileAlreadyExistsException => {
        println("File Already Exists! Overwriting...")
        os.write.over(os.pwd/jsonName, json)
      }
    }
  }
}
