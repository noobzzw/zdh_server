package com.zyc

import com.typesafe.config.ConfigFactory
import com.zyc.base.util.HttpUtil
import com.zyc.common.{HACommon, MariadbCommon, ServerSparkListener, SparkBuilder}
import com.zyc.exception.NoFindOtherServerException
import com.zyc.netty.NettyServer
import org.apache.log4j.MDC
import org.slf4j.LoggerFactory

object SystemInit {

  val logger = LoggerFactory.getLogger(this.getClass)
  //心跳检测路径
  val keepalive_url = "/api/v1/zdh/keeplive"

  def main(args: Array[String]): Unit = {
    MDC.put("job_id", "001")
    val configLoader = ConfigFactory.load("application.conf")
    val port = Some[String](configLoader.getConfig("server").getString("port")).getOrElse("60001")
    val zdh_instance = Some[String](configLoader.getString("instance")).getOrElse("zdh_server")
    val time_interval = Some[Long](configLoader.getString("time_interval").toLong).getOrElse(10L)
    val spark_history_server = Some[String](configLoader.getString("spark_history_server")).getOrElse("http://127.0.0.1:18080/api/v1")
    val online = Some[String](configLoader.getString("online")).getOrElse("1")

    logger.info("开始初始化SparkSession")
    val spark = SparkBuilder.getSparkSession()
    val uiWebUrl = spark.sparkContext.uiWebUrl.get
    val host=uiWebUrl.split(":")(1).substring(2)
    val applicationId = spark.sparkContext.applicationId
    val master = spark.sparkContext.master
    // todo 这句到底是干啥的？？
    spark.sparkContext.master
    try{
      MariadbCommon.insertZdhHaInfo(zdh_instance,host , port, uiWebUrl.split(":")(2),applicationId,spark_history_server,master,online)
      logger.info("开始初始化netty server")
      new Thread(new Runnable {
        override def run(): Unit = new NettyServer().start()
      }).start()

      // zdh HA相关
//      while (true){
//        val list = MariadbCommon.getZdhHaInfo()
//        // 找出其他服务并发送探活信息
//        list.filter(map => !map.getOrElse("zdh_host","").equals(host) || !map.getOrElse("zdh_port","").equals(port))
//          .foreach(map=>{
//            val remote_host = map.getOrElse("zdh_host","")
//            val remote_port = map.getOrElse("zdh_port","")
//            val remote_url = "http://"+remote_host+":"+remote_port+keeplive_url
//            try{
//              val rs = HttpUtil.get(remote_url,Seq.empty[(String,String)])
//              if (!rs.contains("200")) {
//                throw new Exception(s"未找到对应服务:${remote_host}:${remote_host}")
//              }
//              logger.info(s"找到其他服务:${remote_host}:${remote_port}")
//            }catch {
//              case _:Exception => MariadbCommon.delZdhHaInfo(map.getOrElse("id","-1"))
//            }
//          })
//
//        if(list.filter(map=> map.getOrElse("zdh_host","").equals(host) && map.getOrElse("zdh_port","").equals(port)).size<1){
//          logger.debug("当前节点丢失,重新注册当前节点")
//          MariadbCommon.insertZdhHaInfo(zdh_instance,host , port, uiWebUrl.split(":")(2),applicationId,spark_history_server,master,online)
//        }else{
//          logger.debug("当前节点存在,更新当前节点")
//          val instance=list.filter(map=> map.getOrElse("zdh_host","").equals(host) && map.getOrElse("zdh_port","").equals(port))(0)
//          val id=instance.getOrElse("id","-1")
//          MariadbCommon.updateZdhHaInfoUpdateTime(id)
//          if(instance.getOrElse("online","0").equalsIgnoreCase("2")){
//            if(ServerSparkListener.jobs.size()<=0){
//              logger.info("当前节点物理下线成功")
//              MariadbCommon.delZdhHaInfo("enabled",host,port)
//              System.exit(0)
//            }else{
//              logger.info("当前节点存在正在执行的任务,任务执行完成,自动物理下线")
//            }
//          }else if(instance.getOrElse("online","0").equalsIgnoreCase("0")){
//            if(ServerSparkListener.jobs.size()<=0){
//              logger.info("当前节点逻辑下线成功")
//            }else{
//              logger.info("当前节点存在正在执行的任务,任务执行完成,自动逻辑下线")
//            }
//          }
//
//        }
//        Thread.sleep(time_interval*1000)
//      }
    } catch {
      case ex:Exception => logger.error(ex.getMessage)
    } finally {
      MariadbCommon.delZdhHaInfo("enabled",host, port)
    }
  }


}
