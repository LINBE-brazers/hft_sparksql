package hftDataWarehouse.load

case class SparkTaskSchedulingInfo(
                                    project_name:String,
                                    task_name:String,
                                    database_name:String,
                                    table_name:String,
                                    create_time:String,
                                    beging_time:String,
                                    end_time:String,
                                    durationtime:String,
                                    begin_count:String,
                                    end_count:String,
                                    difference:String,
                                    issuccess:String
                                  )
