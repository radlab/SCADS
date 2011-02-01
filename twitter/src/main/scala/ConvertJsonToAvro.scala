case class TwitterspamRecord(x: java.util.HashMap[String, Double], y: Double);

object ConvertJsonToAvro {
  def convertJsontoAvro(line: String): TwitterspamRecord = {
    val tok = line.split(" ", 2)
    val label = (if (tok(0).toInt == 0) { 1 } else { -1 })
    val path = tok(1)
    var json = GetJson.getJson(path) // Fetches the text of the file sitting in 'path'
                                     // This is eminently replacable
    val vec = ParseData.parseData(json, Array("skip_kestrel", "skip_email", "skip_individual_ips", "skip_tweet"))

    new TwitterspamRecord(vec.elements, label)
  }
}
