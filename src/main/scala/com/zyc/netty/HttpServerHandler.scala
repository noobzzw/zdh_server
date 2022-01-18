package com.zyc.netty

import java.net.URLDecoder
import java.util.Date
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import com.zyc.base.util.JsonUtil
import com.zyc.common.{MariadbCommon, SparkBuilder}
import com.zyc.zdh.DataSources
import com.zyc.zdh.datasources.{DataWareHouseSources, FlumeDataSources, KafKaDataSources}
import io.netty.channel.{ChannelFutureListener, ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.codec.http._
import io.netty.util.CharsetUtil
import org.apache.log4j.MDC
import org.slf4j.{Logger, LoggerFactory}


/**
 * 处理Http请求的handler
 */
class HttpServerHandler extends ChannelInboundHandlerAdapter with HttpBaseHandler {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val spark = SparkBuilder.getSparkSession()
  //单线程线程池，同一时间只会有一个线程在运行,保证加载顺序
  private val Deadpool = new ThreadPoolExecutor(
    1, // core pool size
    1, // max pool size
    500, // keep alive time
    TimeUnit.MILLISECONDS, // unit of time
    new LinkedBlockingQueue[Runnable]() // "链表组成的队列，最大为Integer.MAX_VALUE"
  )

  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
    logger.info("Spark(netty)端接受到http消息")
    val request = msg.asInstanceOf[FullHttpRequest]
    val keepAlive = HttpUtil.isKeepAlive(request)
    val response = dispatcher(request)
    if (keepAlive) {
      response.headers().set(Connection, KeepAlive)
      ctx.writeAndFlush(response)
    } else {
      ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE)
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    ctx.writeAndFlush(defaultResponse(serverErr)).addListener(ChannelFutureListener.CLOSE)
    logger.error(cause.getMessage)
    logger.error("error:", cause)
  }


  /**
    * 分发请求
    *
    * @param request 后端发来的请求
    * @return response
    */
  def dispatcher(request: FullHttpRequest): HttpResponse = {
    val uri = request.uri()
    //数据采集请求
    val param = getReqContent(request)

    // kill任务请求
    if (uri.contains("/api/v1/kill")) {
      killJobs(param)
    }

    // 展示数据库表
    if (uri.contains("/api/v1/zdh/show_databases")) {
      showDataBases()
    } else if (uri.contains("/api/v1/zdh/show_tables")) {
      val databaseName = param.getOrElse("databaseName", "default").toString
      showTables(databaseName)
    } else if (uri.contains("/api/v1/zdh/desc_table")) {
      val tableName = param.getOrElse("table", "").toString
      descTable(tableName)
    }

    val dispatchOptions = param.getOrElse("tli", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    val dispatch_task_id = dispatchOptions.getOrElse("job_id", "001").toString
    val task_logs_id=param.getOrElse("task_logs_id", "001").toString
    // jsonToMap 无法处理""，增加判断逻辑
    var etl_date: String = ""
    if (dispatchOptions.contains("params")) {
      etl_date = JsonUtil.jsonToMap(dispatchOptions.getOrElse("params", "").toString).getOrElse("ETL_DATE", "").toString
    }
    try {
      MDC.put("job_id", dispatch_task_id)
      MDC.put("task_logs_id",task_logs_id)
      logger.info(s"接收到请求uri: ${uri}")
      MariadbCommon.updateTaskStatus(task_logs_id, dispatch_task_id, "etl", etl_date, "22")
      logger.info(s"接收到请求uri:$uri,参数:${param.mkString(",").replaceAll("\"", "")}")
      if (uri.contains("/api/v1/zdh/more")) {
        moreEtl(param)
      } else if (uri.contains("/api/v1/zdh/sql")) {
        sqlEtl(param)
      } else if(uri.contains("/api/v1/zdh/drools")){
        droolsEtl(param)
      }else if (uri.contains("/api/v1/zdh/show_databases")) {
        showDataBases()
      } else if (uri.contains("/api/v1/zdh/show_tables")) {
        val databaseName = param.getOrElse("databaseName", "default").toString
        showTables(databaseName)
      } else if (uri.contains("/api/v1/zdh/desc_table")) {
        val table = param.getOrElse("table", "").toString
        val result = DataWareHouseSources.desc_table(spark, table)
        defaultResponse(result)
      } else if (uri.contains("/api/v1/zdh/keeplive")) {
        defaultResponse(cmdOk)
      } else if (uri.contains("/api/v1/zdh")) {
        etl(param)
      } else if (uri.contains("/api/v1/del")) {
        val param = getReqContent(request)
        val key = param.getOrElse("job_id", "")
        logger.info("删除实时任务:" + key)
        if (param.getOrElse("del_type", "").equals("kafka")) {
          if (KafKaDataSources.kafkaInstance.containsKey(key)) {
            KafKaDataSources.kafkaInstance.get(key).stop(false)
            KafKaDataSources.kafkaInstance.remove(key)
          }
        } else {
          if (FlumeDataSources.flumeInstance.containsKey(key)) {
            FlumeDataSources.flumeInstance.get(key).stop(false)
            FlumeDataSources.flumeInstance.remove(key)
          }
        }
        defaultResponse(cmdOk)
      } else {
        defaultResponse(noUri)
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        MariadbCommon.updateTaskStatus2(task_logs_id,dispatch_task_id,dispatchOptions,etl_date)
        defaultResponse(noUri)
      }
    } finally {
      MDC.remove("job_id")
      MDC.remove("task_logs_id")
    }

  }

  private def getBody(content: String): Map[String, Any] = {
    JsonUtil.jsonToMap(content)
  }

  private def getParam(uri: String): Map[String, Any] = {
    val path = URLDecoder.decode(uri, chartSet)
    val cont = uri.substring(path.lastIndexOf("?") + 1)
    if (cont.contains("="))
      cont.split("&").map(f => (f.split("=")(0), f.split("=")(1))).toMap[String, Any]
    else
      Map.empty[String, Any]
  }

  private def getReqContent(request: FullHttpRequest): Map[String, Any] = {
    request.method() match {
      case HttpMethod.GET => getParam(request.uri())
      case HttpMethod.POST => getBody(request.content.toString(CharsetUtil.UTF_8))
    }
  }

  private def killJobs(param: Map[String,Any]): DefaultFullHttpResponse = {
    val task_logs_id = param.getOrElse("task_logs_id", "001").toString
    val dispatch_task_id = param.getOrElse("job_id", "001").toString
    // mdc 为轻量的日志，这里将日志写入zdh_log这个表里
    MDC.put("job_id", dispatch_task_id)
    MDC.put("task_logs_id",task_logs_id)
    val result = kill(param)
    MDC.remove("job_id")
    MDC.remove("task_logs_id")
    result
  }

  private def kill(param: Map[String, Any]): DefaultFullHttpResponse={
//    val task_logs_id = param.getOrElse("task_logs_id", "001").toString
    // 需要kill的任务集
    val jobGroups = param.getOrElse("jobGroups",List.empty[String]).asInstanceOf[List[String]]
    // 启动spark job
    logger.info(s"开始杀死任务: ${jobGroups.mkString(",")}")
    jobGroups.foreach(jobGroup=>{
      spark.sparkContext.cancelJobGroup(jobGroup)
      logger.info(s"杀死任务:$jobGroup")
    })
    logger.info(s"完成杀死任务:${jobGroups.mkString(",")}")
    // response ok
    defaultResponse(cmdOk)
  }

  /**
   * 展示数据库
   * @return
   */
  private def showDataBases(): DefaultFullHttpResponse = {
    defaultResponse(DataWareHouseSources.show_databases(spark))
  }

  private def showTables(databaseName:String): DefaultFullHttpResponse = {
    defaultResponse(DataWareHouseSources.show_tables(spark, databaseName))
  }

  private def descTable(tableName:String): Unit = {
    defaultResponse(DataWareHouseSources.desc_table(spark, tableName))
  }


  private def moreEtl(param: Map[String, Any]): DefaultFullHttpResponse = {

    //此处任务task_logs_id
    val task_logs_id = param.getOrElse("task_logs_id", "001").toString

    //输入数据源信息
    val dsi_EtlInfo = param.getOrElse("dsi_EtlInfo", List.empty[Map[String, Map[String, Any]]]).asInstanceOf[List[Map[String, Map[String, Any]]]]

    //输出数据源信息
    val dsi_Output = param.getOrElse("dsi_Output", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //etl任务信息
    val etlMoreTaskInfo = param.getOrElse("etlMoreTaskInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //调度任务信息
    val dispatchOptions = param.getOrElse("tli", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //输出数据源类型
    val outPut = dsi_Output.getOrElse("data_source_type", "").toString

    //输出数据源基础信息
    val outPutBaseOptions = dsi_Output


    //输出数据源其他信息k:v,k1:v1 格式
    val outputOptions: Map[String, Any] = etlMoreTaskInfo.getOrElse("data_sources_params_output", "").toString match {
      case "" => Map.empty[String, Any]
      case a => JsonUtil.jsonToMap(a)
    }

    val outPutCols = null

    //清空语句
    val clear = etlMoreTaskInfo.getOrElse("data_sources_clear_output", "").toString

    Deadpool.execute(new Runnable() {
      override def run() = {
        try {
          DataSources.DataHandlerMore(spark, task_logs_id, dispatchOptions, dsi_EtlInfo, etlMoreTaskInfo, outPut,
            outPutBaseOptions ++ outputOptions, outPutCols, clear)
        } catch {
          case ex: Exception => {
            ex.printStackTrace()
          }
        }
      }
    })
    defaultResponse(cmdOk)

  }

  private def sqlEtl(param: Map[String, Any]): DefaultFullHttpResponse = {
    //此处任务task_logs_id
    val task_logs_id = param.getOrElse("task_logs_id", "001").toString

    //输入数据源信息
    val dsi_Input = param.getOrElse("dsi_Input", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    //输出数据源信息
    val dsi_Output = param.getOrElse("dsi_Output", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    //etl任务信息
    val sqlTaskInfo = param.getOrElse("sqlTaskInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    //调度任务信息
    val dispatchOptions = param.getOrElse("tli", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //输入数据源类型
    val inPut = dsi_Input.getOrElse("data_source_type", "").toString

    //输入数据源基础信息
    val inPutBaseOptions = dsi_Input

    //输入数据源其他信息k:v,k1:v1 格式
    val inputOptions: Map[String, Any] = sqlTaskInfo.getOrElse("data_sources_params_input", "").toString.trim match {
      case "" => Map.empty[String, Any]
      case a => JsonUtil.jsonToMap(a)
    }

    val inputCols: Array[String] = sqlTaskInfo.getOrElse("data_sources_file_columns", "").toString.split(",")

    //输出数据源类型
    val outPut = dsi_Output.getOrElse("data_source_type", "").toString

    //输出数据源基础信息
    val outPutBaseOptions = dsi_Output

    //过滤条件
    val filter = sqlTaskInfo.getOrElse("data_sources_filter_input", "").toString


    //输出数据源其他信息k:v,k1:v1 格式
    val outputOptions: Map[String, Any] = sqlTaskInfo.getOrElse("data_sources_params_output", "").toString match {
      case "" => Map.empty[String, Any]
      case a => JsonUtil.jsonToMap(a)
    }


    //清空语句
    val clear = sqlTaskInfo.getOrElse("data_sources_clear_output", "").toString


    Deadpool.execute(new Runnable() {
      override def run() = {
        try {
          DataSources.DataHandlerSql(spark, task_logs_id, dispatchOptions, sqlTaskInfo, inPut, inPutBaseOptions ++ inputOptions, outPut,
            outPutBaseOptions ++ outputOptions, null, clear)
        } catch {
          case ex: Exception => {
            ex.printStackTrace()
          }
        }
      }
    })
    defaultResponse(cmdOk)
  }

  private def etl(param: Map[String, Any]): DefaultFullHttpResponse = {
    //此处任务task_logs_id
    val task_logs_id = param.getOrElse("task_logs_id", "001").toString
    //输入数据源信息
    val dsi_Input = param.getOrElse("dsi_Input", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    //输出数据源信息
    val dsi_Output = param.getOrElse("dsi_Output", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    //etl任务信息
    val etlTaskInfo = param.getOrElse("etlTaskInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    //调度任务信息
    val dispatchOptions = param.getOrElse("tli", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //输入数据源类型
    val inPut = dsi_Input.getOrElse("data_source_type", "").toString

    //输入数据源基础信息
    val inPutBaseOptions = dsi_Input

    //输入数据源其他信息k:v,k1:v1 格式
    val inputOptions: Map[String, Any] = etlTaskInfo.getOrElse("data_sources_params_input", "").toString.trim match {
      case "" => Map.empty[String, Any]
      case a => JsonUtil.jsonToMap(a)
    }

    val inputCols: Array[String] = etlTaskInfo.getOrElse("data_sources_file_columns", "").toString.split(",")

    //输出数据源类型
    val outPut = dsi_Output.getOrElse("data_source_type", "").toString

    //输出数据源基础信息
    val outPutBaseOptions = dsi_Output

    //过滤条件
    val filter = etlTaskInfo.getOrElse("data_sources_filter_input", "").toString


    //输出数据源其他信息k:v,k1:v1 格式
    val outputOptions: Map[String, Any] = etlTaskInfo.getOrElse("data_sources_params_output", "").toString match {
      case "" => Map.empty[String, Any]
      case a => JsonUtil.jsonToMap(a)
    }

    //字段映射
    //List(Map(column_alias -> id, column_expr -> id))
    val list_map = etlTaskInfo.getOrElse("column_data_list", null).asInstanceOf[List[Map[String, String]]]
    val outPutCols = list_map.toArray
    //val outPutCols=list_map.map(f=> expr(f.getOrElse("column_expr","")).as(f.getOrElse("column_alias",""))).toArray

    //清空语句
    val clear = etlTaskInfo.getOrElse("data_sources_clear_output", "").toString


    Deadpool.execute(new Runnable() {
      override def run() = {
        try {
          DataSources.DataHandler(spark, task_logs_id, dispatchOptions, etlTaskInfo, inPut, inPutBaseOptions ++ inputOptions, filter, inputCols, outPut,
            outPutBaseOptions ++ outputOptions, outPutCols, clear)
        } catch {
          case ex: Exception => {
            ex.printStackTrace()
          }
        }
      }
    })
    defaultResponse(cmdOk)
  }


  private def droolsEtl(param: Map[String, Any]): DefaultFullHttpResponse ={

    //此处任务task_logs_id
    val task_logs_id = param.getOrElse("task_logs_id", "001").toString

    //输入数据源信息
    val dsi_EtlInfo = param.getOrElse("dsi_EtlInfo",List.empty[Map[String, Map[String, Any]]]).asInstanceOf[List[Map[String, Map[String, Any]]]]

    //输出数据源信息
    val dsi_Output = param.getOrElse("dsi_Output", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //etl任务信息
    val etlDroolsTaskInfo = param.getOrElse("etlDroolsTaskInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //etl-多源任务信息
    val etlMoreTaskInfo = param.getOrElse("etlMoreTaskInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //etl-sql任务信息
    val sqlTaskInfo = param.getOrElse("sqlTaskInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //调度任务信息
    val dispatchOptions = param.getOrElse("tli", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //输出数据源类型
    val outPut = dsi_Output.getOrElse("data_source_type", "").toString

    //输出数据源基础信息
    val outPutBaseOptions = dsi_Output


    //输出数据源其他信息k:v,k1:v1 格式
    val outputOptions: Map[String, Any] = etlDroolsTaskInfo.getOrElse("data_sources_params_output", "").toString match {
      case "" => Map.empty[String, Any]
      case a => JsonUtil.jsonToMap(a)
    }

    val outPutCols = null

    //清空语句
    val clear = etlDroolsTaskInfo.getOrElse("data_sources_clear_output", "").toString

    Deadpool.execute(new Runnable() {
      override def run() = {
        try {
          DataSources.DataHandlerDrools(spark, task_logs_id, dispatchOptions, dsi_EtlInfo, etlDroolsTaskInfo,etlMoreTaskInfo,sqlTaskInfo, outPut,
            outPutBaseOptions ++ outputOptions, outPutCols, clear)
        } catch {
          case ex: Exception => {
            ex.printStackTrace()
          }
        }
      }
    })
    defaultResponse(cmdOk)

  }
}
