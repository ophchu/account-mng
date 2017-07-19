// Databricks notebook source
import com.dgn.core.utils._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql._
import org.apache.hadoop.fs.Path
import java.io.{File, IOException, StringWriter}
import java.nio.file._
import collection.mutable.HashMap
import scala.collection.JavaConverters._
import java.util.Properties
import java.util.Date
import javax.mail._
import javax.mail.internet._

// COMMAND ----------

// MAGIC %run "production-grid/config/main-config-values-root" $env="lts-prod"

// COMMAND ----------

val dateMinus1 = DateTimeUtils.getLocalDate(dt).minusDays(1).toString

// COMMAND ----------

val recipientsMap = Map(
  "test-team" -> "tamir.mayblat@dgngames.com, Ophir.Cohen@dgngames.com, Shay.Cohen@dgngames.com",
  "tamir" -> "tamir.mayblat@dgngames.com",
  "ophchu" -> "Ophir.Cohen@dgngames.com",
  "all" ->  "new_mail_report_Daily_LTS@dgngames.com"
)
val sendTo = recipientsMap("ophchu")

println(s"Going to send the daily mail to: [$sendTo]")

// COMMAND ----------

val dt = dateMinus1
val templateDir = s"$avroSchemaRoot/ftls"
val ftlFileName = "summary.ftl"

//email params
val env = "LTS"
val username = "AKIAJOBJEX5BCN4XUIMA"
val password = "AgCX2EVT5E0kqx5E5ERoOJL2hIeVxcee1w+3xqC9uF5z"
val host = "email-smtp.us-east-1.amazonaws.com"
val from = "bi_reports@dgngames.com"
val contentType = "text/html"
val dateMinus31 = DateTimeUtils.getLocalDate(dt).minusDays(31).toString

//trends params
val trendPeriod = 14
val dateTrendFrom = DateTimeUtils.getLocalDate(dt).minusDays(trendPeriod).toString

// COMMAND ----------

val summaryTotalSqlDaily = s"""
  with strtSession as (
  select 
  dt, 
  count(distinct user_id) as dau, 
  count(distinct case when payer=true and purchase_count>0 then user_id end) as payers
  from lts_event_start_session
  group by dt
  ),

  mau as (
  select 
  date_add(max(dt),-1) as dt,
  count(distinct user_id) as mau
  from lts_event_start_session_daily_rng
  ),

  spin as (
  select 
  dt, 
  count(distinct user_id) as spinners,
  count(*) as spins
  from lts_event_spin
  group by dt),

  trns as (
  select 
  dt, 
  count (distinct user_id) as depositors,
  sum(price) as deposit_amount,
  count(*) as deposits,
  count(distinct case when purchase_count=0 then user_id end) as FTD
  from lts_event_transaction
  group by dt
  )

  select 
  ss.dt as baseDate, 
  ss.dau,
  m.mau,
  s.spinners,
  s.spins,
  t.depositors,
  t.deposit_amount as depositsAmount,
  t.deposits,
  ss.payers,
  t.ftd,
  s.spins/s.spinners as spinRatio,
  t.deposit_amount/ss.dau as darpu,
  t.deposit_amount/t.depositors as arppu
  from strtSession ss
  ,mau m
  ,spin s
  ,trns t
"""

// COMMAND ----------

val summarySqlDaily = s"""
    WITH m 
       AS (SELECT platform_id, 
                  Count(DISTINCT( user_id )) AS mau 
           FROM   lts_event_start_session_daily_rng 
           GROUP  BY platform_id), 
       d 
       AS (SELECT platform_id, 
                  Count(DISTINCT( user_id )) AS dau 
           FROM   lts_event_start_session 
           GROUP  BY platform_id), 
       sh 
       AS (SELECT platform_id, 
                  Count(*) AS shares 
           FROM   lts_event_share 
           GROUP  BY platform_id), 
       inv 
       AS (SELECT platform_id, 
                  Count(*) AS invites 
           FROM   lts_event_invite 
           GROUP  BY platform_id), 
       rg 
       AS (SELECT platform_id, 
                  Count(*) AS request_gifts 
           FROM   lts_event_request_gift 
           GROUP  BY platform_id), 
       sg 
       AS (SELECT platform_id, 
                  Count(*) AS send_gifts 
           FROM   lts_event_send_gift 
           GROUP  BY platform_id), 
       s 
       AS (SELECT platform_id, 
                  Count(DISTINCT( user_id )) AS spinners, 
                  Count(*)                   AS spins 
           FROM   lts_event_spin 
           GROUP  BY platform_id), 
       t 
       AS (SELECT platform_id, 
                  Count(DISTINCT( user_id )) AS depositors, 
                  Sum(price)                 AS deposits_amount, 
                  Count(*)                   AS deposits 
           FROM   lts_event_transaction 
           GROUP  BY platform_id), 
       p 
       AS (SELECT platform_id, 
                  Count(DISTINCT( user_id )) AS payers 
           FROM   lts_event_start_session 
           WHERE  payer = true 
                  AND purchase_count > 0 
           GROUP  BY platform_id), 
       f 
       AS (SELECT platform_id, 
                  Count(DISTINCT( user_id )) AS ftd 
           FROM   lts_event_transaction 
           WHERE  purchase_count = 0 
           GROUP  BY platform_id) 
  SELECT '$dt' as baseDate,
         m.platform_id                        AS platformId, 
         mau, 
         dau, 
         Nvl(spinners, 0)                     AS spinners, 
         Nvl(spins, 0)                        AS spins, 
         Nvl(depositors, 0)                   AS depositors, 
         Nvl(deposits_amount, 0)              AS depositsAmount, 
         Nvl(deposits, 0)                     AS deposits, 
         Nvl(payers, 0)                       AS payers, 
         Nvl(ftd, 0)                          AS ftd, 
         Nvl(spins / spinners, 0)             AS spinRatio, 
         Nvl(deposits_amount / dau, 0)        AS darpu, 
         Nvl(deposits_amount / depositors, 0) AS arppu, 
         Nvl(shares, 0)                       AS shares, 
         Nvl(invites, 0)                      AS invites, 
         Nvl(request_gifts, 0)                AS requestGifts, 
         Nvl(send_gifts, 0)                   AS sendGifts 
  FROM   m 
         LEFT JOIN d 
                ON m.platform_id = d.platform_id 
         LEFT JOIN s 
                ON m.platform_id = s.platform_id 
         LEFT JOIN t 
                ON m.platform_id = t.platform_id 
         LEFT JOIN p 
                ON m.platform_id = p.platform_id 
         LEFT JOIN f 
                ON m.platform_id = f.platform_id 
         LEFT JOIN sh 
                ON m.platform_id = sh.platform_id 
         LEFT JOIN inv 
                ON m.platform_id = inv.platform_id 
         LEFT JOIN rg 
                ON m.platform_id = rg.platform_id 
         LEFT JOIN sg 
                ON m.platform_id = sg.platform_id 
  ORDER  BY platformId DESC 
"""

// COMMAND ----------

val transactionSqlDaily = s"""
  SELECT '$dt' as baseDate, 
       platform_id                AS platformId, 
       purchase_origin            AS purchaseOrigin, 
       Count(*)                   AS deposits, 
       Sum(price)                 AS depositsAmount, 
       Count(DISTINCT( user_id )) AS depositors, 
       Sum(coins_amount)          AS coinsAmount 
  FROM   lts_event_transaction
  GROUP  BY dt, 
          platform_id, 
          purchase_origin 
  ORDER  BY dt, 
          platform_id, 
          purchase_origin 
"""

// COMMAND ----------

val slotSqlDaily = s"""
  SELECT '$dt' as baseDate , 
       platform_id                AS platformId, 
       slot_id                    AS slotId, 
       nvl(Sum(win_amount),0)     AS winAmount, 
       Sum(bet_amount)            AS betAmount, 
       nvl(Sum(feature_win_amount),0) AS featureWinAmount, 
       nvl(Sum(jackpot_win_amount),0) AS jackpotWinAmount, 
       nvl(Sum(mission_win_amount),0) AS missionWinAmount, 
       Count(*)                   AS spins, 
       Count(DISTINCT( user_id )) AS spinners 
  FROM   lts_event_spin 
  GROUP  BY dt, 
          platform_id, 
          slot_id 
  ORDER  BY dt, 
          platform_id, 
          slot_id 
"""

// COMMAND ----------

val collectBonusSqlDaily = s"""
  SELECT '$dt' as baseDate, 
       platform_id                AS platformId, 
       bonus_source               AS bonusSource, 
       nvl(Sum(coins_amount),0)          AS coins, 
       Count(DISTINCT( user_id )) AS collectors 
  FROM   lts_event_collect_bonus 
  GROUP  BY dt, 
          platform_id, 
          bonus_source 
  ORDER  BY dt, 
          platform_id, 
          bonus_source 
"""

// COMMAND ----------

val dauTrendSqlDaily = s"""
  SELECT dt as baseDate, 
       platform_id                AS series, 
       Count(DISTINCT( user_id )) AS data 
  FROM   lts_event_start_session_daily_rng 
  where dt>='$dateTrendFrom' and dt<='$dt'
  GROUP  BY dt, 
          platform_id 
  ORDER  BY dt, 
          platform_id 
"""

// COMMAND ----------

val depositTrendSqlDaily = s"""
  SELECT dt as baseDate, 
       platform_id AS series, 
       Count(*)    AS data 
  FROM   lts_event_transaction_daily_rng 
  where dt>='$dateTrendFrom' and dt<='$dt'
  GROUP  BY dt, 
          platform_id 
  ORDER  BY dt, 
          platform_id 
"""

// COMMAND ----------

val depositAmountTrendSqlDaily = s"""
  SELECT dt as baseDate, 
       platform_id AS series, 
       sum(price)  AS data
  FROM   lts_event_transaction_daily_rng 
  where  dt>='$dateTrendFrom' and dt<='$dt'
  GROUP  BY dt, 
          platform_id 
  ORDER  BY dt, 
          platform_id 
"""

// COMMAND ----------

val installCanvasSqlDaily = s"""
  SELECT   '$dt' as baseDate, platform_id as platformId,
           Count(*) AS installs, 
           Sum( 
           CASE 
                    WHEN register_campaign_source IS NOT NULL THEN 1 
                    ELSE 0 
           END) AS paidInstalls, 
           Sum( 
           CASE 
                    WHEN register_organic_source IS NOT NULL THEN 1 
                    ELSE 0 
           END) AS organicInstalls 
  FROM     lts_event_install 
  WHERE    platform_id='WEB_CANVAS' 
  GROUP BY 2
"""

// COMMAND ----------

val installMobileSqlDaily = s"""
  SELECT '$dt' as baseDate,
       platform_id AS platformId, 
       Count(*)    AS installs, 
       Sum(CASE 
             WHEN ko_network IS NOT NULL THEN 1 
             ELSE 0 
           END)    AS paidinstalls, 
       Sum(CASE 
             WHEN ko_network IS NULL THEN 1 
             ELSE 0 
           END)    AS organicInstalls 
  FROM   lts_event_kochava 
  GROUP  BY 2  
"""

// COMMAND ----------

def createTables(event: String, tableName: String) = {
  val baseEventPath = s"""$eventPath/eventType=$event/"""
  spark.read.option("basePath", baseEventPath).parquet(s"""$baseEventPath/dt=$dt""").createOrReplaceTempView(tableName)
}

def createTableRng31(event : String, tableName : String) {
  val baseEventPath = s"""$eventPath/eventType=$event"""
  SparkUtils.readDtDF(baseEventPath, dateMinus31, dt).createOrReplaceTempView(tableName)
}

// COMMAND ----------

case class RetentionElement (
                              val baseDate : String,
                              val retentionDate : String,
                              val platformId : String,
                              val du : Long,
                              val retentionCount : Long,
                              val retentionRatio : Double
                            )

case class TransactionReportElement (
                                      val baseDate : String,
                                      val platformId : String,
                                      val purchaseOrigin : String,
                                      val depositors : Long,
                                      val deposits : Long,
                                      val depositsAmount : Double,
                                      val coinsAmount : Long
                                    )

case class SlotReportElement (
                               val baseDate : String,
                               val platformId : String,
                               val slotId : String,
                               val winAmount : Long,
                               val featureWinAmount : Long,
                               val jackpotWinAmount : Long,
                               val missionWinAmount : Long,
                               val betAmount : Long,
                               val spins : Long,
                               val spinners : Long
                             )

case class SummaryReportElement (
                                  val baseDate : String,
                                  val platformId : String,
                                  val mau : Long,
                                  val dau : Long,
                                  val spinners : Long,
                                  val spins : Long,
                                  val depositors : Long,
                                  val depositsAmount : Double,
                                  val deposits : Long,
                                  val payers : Long,
                                  val ftd : Long ,
                                  val spinRatio : Double,
                                  val darpu : Double,
                                  val arppu : Double,
                                  val shares : Long,
                                  val invites : Long,
                                  val requestGifts : Long,
                                  val sendGifts : Long
                                )

case class SummaryTotalReportElement (
                                       val baseDate : String,
                                       val mau : Long,
                                       val dau : Long,
                                       val spinners : Long,
                                       val spins : Long,
                                       val depositors : Long,
                                       val depositsAmount : Double,
                                       val deposits : Long,
                                       val payers : Long,
                                       val ftd : Long ,
                                       val spinRatio : Double,
                                       val darpu : Double,
                                       val arppu : Double
                                     )

case class CollectBonusReportElement (
                                       val baseDate : String,
                                       val platformId : String,
                                       val bonusSource : String,
                                       val coins : Long,
                                       val collectors : Long
                                     )

case class TrendDataPoint (
                            val series : String,
                            val baseDate : String,
                            val data : Double
                          )

case class InstallReportElement (
                                  val baseDate : String,
                                  val platformId : String,
                                  val organicInstalls : Long,
                                  val paidInstalls : Long,
                                  val installs : Long
                                )

case class SummaryReportWrapper (
                                  val summaryTotalReportElements : Array[SummaryTotalReportElement],
                                  val summaryReportElements : Array[SummaryReportElement],
                                  val retentionD1Elements: Array[RetentionElement],
                                  val retentionD7Elements : Array[RetentionElement],
                                  val retentionD30Elements : Array[RetentionElement],
                                  val transactionReportElements : Array[TransactionReportElement],
                                  val slotReportElements : Array[SlotReportElement],
                                  val collectBonusReportElements : Array[CollectBonusReportElement],
                                  val installCanvasReportElements : Array[InstallReportElement],
                                  val installMobileReportElements : Array[InstallReportElement],
                                  val dauTrend : Array[TrendDataPoint],
                                  val depositTrend : Array[TrendDataPoint],
                                  val depositAmountTrend : Array[TrendDataPoint]
                                )

// COMMAND ----------

def initRetention(baseDate : String, retentionD : Int) = {
  val retentionBaseDate = DateTimeUtils.getLocalDate(dt).minusDays(retentionD+1).toString
  val windowStartDate = DateTimeUtils.getLocalDate(dt).minusDays(1).toString
  val windowEndDate = baseDate

  val sql = s"""
        WITH d AS 
            ( 
               SELECT   platform_id, 
                         Count(DISTINCT(platform_user_id)) AS du 
               FROM     lts_event_start_session_daily_rng 
               WHERE    dt = '$retentionBaseDate'
               AND      session_count=0 
               GROUP BY 1 ), f AS 
            ( 
             SELECT   platform_id, 
                      count(DISTINCT(platform_user_id)) AS fu 
             FROM     lts_event_start_session_daily_rng 
             WHERE    ( 
                               dt='$windowStartDate'
                      OR       dt='$windowEndDate') 
             AND      ( 
                               ts - first_play_ts) >= 1000*60*60*24*$retentionD
             AND      ( 
                               ts - first_play_ts) <= 1000*60*60*24*($retentionD + 1.0)
             AND      platform_user_id IN 
                      ( 
                             SELECT platform_user_id 
                             FROM   lts_event_start_session_daily_rng 
                             WHERE  dt = '$retentionBaseDate'
                             AND    session_count=0) 
             GROUP BY 1 ) 
    SELECT    '$dt' as baseDate, 
              '$retentionBaseDate' as retentionDate,
              d.platform_id as platformId, 
              du, 
              nvl(fu,0) as retentionCount, 
              nvl((fu/du) * 100 ,0) AS retentionRatio 
    FROM      d 
    LEFT JOIN f 
    ON        d.platform_id=f.platform_id 
    ORDER BY  1
  """
  spark.sql(sql)
}

// COMMAND ----------

import freemarker.cache.FileTemplateLoader
import freemarker.template.Configuration
import freemarker.template.Version
import freemarker.template.TemplateExceptionHandler
import freemarker.template._
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;
import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;

val dataset : DefaultCategoryDataset = new DefaultCategoryDataset();

def getFreemarkerConfiguration: Configuration = {
  val cfg : Configuration = new Configuration(new Version(2, 3, 22))
  cfg.setDefaultEncoding("UTF-8")
  cfg.setLocale(java.util.Locale.US)
  cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER)
  cfg.setTemplateLoader(new FileTemplateLoader(new File(s"""$templateDir/""")))
  cfg
}

def getTrendDataFile(cid: String, trendPoints: Array[TrendDataPoint]): File = {
  for (trendPoint <- trendPoints) {
    dataset.addValue(trendPoint.data, trendPoint.series, trendPoint.baseDate)
  }
  val chart = ChartFactory.createLineChart(cid, null, null, dataset, PlotOrientation.VERTICAL, true, true, false)
  val plot = chart.getCategoryPlot
  val domainAxis = plot.getDomainAxis
  domainAxis.setCategoryLabelPositions(CategoryLabelPositions.UP_90)
  val path = FileSystems.getDefault.getPath(s"""/tmp/$cid-$dt.jpeg""")

  Files.deleteIfExists(path)
  ChartUtilities.saveChartAsJPEG(path.toFile, chart, 700, 360)
  path.toFile
}

def getTrendBodyPart(cid: String, trendPoints: Array[TrendDataPoint]): BodyPart = {
  val messageBodyPart = new MimeBodyPart
  val fds = new FileDataSource(getTrendDataFile(cid, trendPoints))
  messageBodyPart.setDataHandler(new DataHandler(fds))
  messageBodyPart.setHeader("Content-ID", s"""<$cid>""")
  messageBodyPart
}

def processTemplate(templateFile: String, data: SummaryReportWrapper): String = {
  val cfg = getFreemarkerConfiguration
  cfg.setTemplateLoader(new FileTemplateLoader(new File(s"""$templateDir/""")))
  val template = cfg.getTemplate(templateFile)
  val dataMap = scala.collection.mutable.Map[String, Object]()
  dataMap += ("summaryTotalReportElements" -> data.summaryTotalReportElements)
  dataMap += ("summaryReportElements" -> data.summaryReportElements)
  dataMap += ("installCanvasReportElements" -> data.installCanvasReportElements)
  dataMap += ("installMobileReportElements" -> data.installMobileReportElements)
  dataMap += ("retentionD1Elements" -> data.retentionD1Elements)
  dataMap += ("retentionD7Elements" -> data.retentionD7Elements)
  dataMap += ("retentionD30Elements" -> data.retentionD30Elements)

  val out = new StringWriter
  template.process(dataMap.asJava, out)
  out.toString
}

// COMMAND ----------

def send (env: String, host: String, username: String, password: String, from: String, to: String, subject: String, body: String, contentType: String, parts : List[BodyPart]) {
  val prop:Properties = new Properties()
  prop.put("mail.smtp.host", host)
  prop.put("mail.smtp.user", username)
  prop.put("mail.smtp.password", password)
  prop.put("mail.smtp.auth", "true")
  prop.put("mail.smtp.port", "587")
  prop.put("mail.smtp.starttls.enable", "true")

  val auth:Authenticator = new Authenticator() {
    override def getPasswordAuthentication = new PasswordAuthentication(username, password)
  }
  var session:Session = Session.getInstance(prop, auth)

  var msg:MimeMessage = new MimeMessage(session)
  msg.setFrom(new InternetAddress(from))
  msg.setRecipients(Message.RecipientType.TO, to)
  msg.setHeader("Content-Type", "text/html")
  msg.setSubject(subject)

  var multipart : MimeMultipart = new MimeMultipart
  var messageBodyPart : BodyPart = new MimeBodyPart
  messageBodyPart.setContent(body, contentType);
  multipart.addBodyPart(messageBodyPart)

  for (part <- parts) {
    multipart.addBodyPart(part)
  }
  msg.setContent(multipart)
  Transport.send(msg)
}

// COMMAND ----------

def initSummaryReportWrapper(summaryTotalReportElements : Array[SummaryTotalReportElement],
                             summaryReportElements : Array[SummaryReportElement],
                             retentionD1Elements : Array[RetentionElement],
                             retentionD7Elements : Array[RetentionElement],
                             retentionD30Elements : Array[RetentionElement],
                             transactionReportElements : Array[TransactionReportElement],
                             slotReportElements : Array[SlotReportElement],
                             collectBonusReportElements : Array[CollectBonusReportElement],
                             installCanvasReportElements : Array[InstallReportElement],
                             installMobileReportElements : Array[InstallReportElement],
                             dauTrend : Array[TrendDataPoint],
                             depositTrend : Array[TrendDataPoint],
                             depositAmountTrend : Array[TrendDataPoint]
                            ) = {
  SummaryReportWrapper(summaryTotalReportElements,
    summaryReportElements,
    retentionD1Elements,
    retentionD7Elements,
    retentionD30Elements,
    transactionReportElements,
    slotReportElements,
    collectBonusReportElements,
    installCanvasReportElements,
    installMobileReportElements,
    dauTrend,
    depositTrend,
    depositAmountTrend
  )
}

def processAndSendEmail() {
  //create report elements
  val summaryTotalReportElements = spark.sql(summaryTotalSqlDaily).as[SummaryTotalReportElement].collect
  val summaryReportElements = spark.sql(summarySqlDaily).as[SummaryReportElement].collect
  val installCanvasReportElements = spark.sql(installCanvasSqlDaily).as[InstallReportElement].collect
  val installMobileReportElements = spark.sql(installMobileSqlDaily).as[InstallReportElement].collect
  val retentionD1ReportElements = initRetention(dt, 1).as[RetentionElement].collect
  val retentionD7ReportElements = initRetention(dt, 7).as[RetentionElement].collect
  val retentionD30ReportElements = initRetention(dt, 30).as[RetentionElement].collect
  val transactionReportElements = spark.sql(transactionSqlDaily).as[TransactionReportElement].collect
  val slotReportElements = spark.sql(slotSqlDaily).as[SlotReportElement].collect
  val collectBonusReportElements = spark.sql(collectBonusSqlDaily).as[CollectBonusReportElement].collect
  val dauTrend = spark.sql(dauTrendSqlDaily).as[TrendDataPoint].collect
  val depositTrend = spark.sql(dauTrendSqlDaily).as[TrendDataPoint].collect
  val depositAmountTrend = spark.sql(depositAmountTrendSqlDaily).as[TrendDataPoint].collect

  //init wrapper
  val summaryReportWrapper = initSummaryReportWrapper(summaryTotalReportElements,
    summaryReportElements,
    retentionD1ReportElements,
    retentionD7ReportElements,
    retentionD30ReportElements,
    transactionReportElements,
    slotReportElements,
    collectBonusReportElements ,
    installCanvasReportElements,
    installMobileReportElements,
    dauTrend,
    depositTrend,
    depositAmountTrend
  )
  //   val to = testRecipients
  //   val to = allRecipientsAddress
  val to = sendTo
  val subject = s"""$env - Summary Mail: date = $dt"""
  //apply ftl template to generate mail body
  val body = processTemplate(ftlFileName, summaryReportWrapper);
  val dauBodyPart = getTrendBodyPart("dau_trend", summaryReportWrapper.dauTrend)
  val depositTrendBodyPart = getTrendBodyPart("deposit_trend", summaryReportWrapper.depositTrend)
  val depositAmountTrendBodyPart = getTrendBodyPart("deposit_amount_trend", summaryReportWrapper.depositAmountTrend)

  //apply list of trends for graphs
  val parts : List[BodyPart] = List(dauBodyPart, depositTrendBodyPart, depositAmountTrendBodyPart)

  send(env, host, username, password, from, to, subject, body, contentType, parts)

  println(s"""Email sent: $subject""")
}

// COMMAND ----------

createTableRng31("start_session", "lts_event_start_session_daily_rng")
createTableRng31("transaction", "lts_event_transaction_daily_rng")
createTables("start_session", "lts_event_start_session")
createTables("transaction", "lts_event_transaction")
createTables("spin", "lts_event_spin")
createTables("share", "lts_event_share")
createTables("invite", "lts_event_invite")
createTables("request_gift", "lts_event_request_gift")
createTables("send_gift", "lts_event_send_gift")
createTables("kochava", "lts_event_kochava")
createTables("collect_bonus", "lts_event_collect_bonus")
createTables("install", "lts_event_install")

// COMMAND ----------

processAndSendEmail