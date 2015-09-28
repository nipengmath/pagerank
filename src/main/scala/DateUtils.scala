import java.text.SimpleDateFormat
import java.util.Date

object DateUtils {

  val defaultFormat = new SimpleDateFormat("yyyyMMdd_HHmmss")

  def format(date: Date = new Date(), 
       format: SimpleDateFormat = null) = {
    val f = if (format == null) defaultFormat else format
    f.format(date)
  }
  
}