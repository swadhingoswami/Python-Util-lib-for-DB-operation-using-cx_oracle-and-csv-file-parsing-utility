"""
Author      : Swadhin Goswami
Gmail       : gsmswadhin@gmail.com
Description : Example program for DB insert operation in python using cx_oracle 
"""

#!/usr/bin/python
import PyLibUtil
import exceptions
import re
import time
import datetime


CS_CLUSTER_TBL_VAL = {
'CLUSTER_NAME'          : Cluster_Name,
'MART_NAME'             : Mart,
'NODE_NAME'             : l[0],
'PARTITION_ID'          : '', # To DO
'PRODCON_CODE_LINK'     : Prodcon[ 1 ],
'IQS_CODE_LINK'         : IqsCodeLink[ 1 ],
'DATA_LOAD_START_TIME'  : DataLoadTime,
'DATA_LOAD_END_TIME'    : HandShakeFinished,
'ITEM_LOAD_START_TIME'  : ItemLoadStartTime,
'ITEM_LOAD_END_TIME'    : ItemLoadEndtTime,
'VENUE_LOAD_START_TIME' : VenueLoadStartTime,
'VENUE_LOAD_END_TIME'   : VenueLoadEndtTime,
'HAND_SHAKE_FINISHED'   : HandShakeFinished
}
TableName = "CS_CLUSTER_TBL";
PyLibUtil.InsertIntoDB( TableName, CS_CLUSTER_TBL_VAL );

CS_MART_VAL = {
'MARTNAME'           : Mart,
'CLUSTER_NAME'       : Cluster_Name,
'MART_TYPE'          : DataMartType[ 1 ],
'NO_OF_SIDES'        : d[ "NumOfClusters" ],
'HEAD1'              : l[0],
'HEAD2'              : '',
'NO_OF_PARTITIONS'   : d[ "NumOfPartition" ],
'START_TIME_PERIOD'  : MartStartYearVal,
'END_TIME_PERIOD'    : MartEndYearVal
}
TableName = "CS_MART";
PyLibUtil.InsertIntoDB( TableName, CS_MART_VAL );
