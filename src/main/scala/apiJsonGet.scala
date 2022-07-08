import java.io.File
import java.nio.file.FileAlreadyExistsException

class apiJsonGet(apiUrl:String){
  def getJson():Unit = {
    val data = requests.get(apiUrl)
    val text = data.text()
    val json = ujson.read(text)
    try {
      os.write(os.pwd/"tmp.json", json)
    } catch {
      case e: FileAlreadyExistsException => println("File Already Exists!")
    }
  }
}
