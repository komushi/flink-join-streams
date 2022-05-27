#!/bin/bash

current_account=$(aws sts get-caller-identity --query Account --output text)

export CF_STACKName=flinkStack
export CF_FlinkJarS3Bucket=vsco-module-bucket
export CF_FlinkJarName=flink-join-streams-0.0.0.jar

#Flink input Resources
export CF_InputKdsStreamFirst=VsocStreamCan
export CF_InputKdsStreamSecond=VsocStreamIis
export CF_InputKdsStreamThird=VsocStreamSms
export CF_KdsInitialPosition=LATEST

# Flink output Resources

# Flink application
export CF_KinesisAnalyticsName=vsocAnalytics
export CF_WindowSize=100
export CF_WindowSizeUOM=MILLISECONDS
export CF_WatermarkDelay=60100
export CF_WatermarkDelayUOM=MILLIS
export CF_ThreaWatermarking=32
export CF_ThreadWindowing=64
export CF_ThreadLogging=1


aws cloudformation deploy  \
 --template-file cfn_flinkDeployment.yaml \
 --stack-name ${CF_STACKName} \
 --capabilities CAPABILITY_NAMED_IAM \
 --no-fail-on-empty-changeset \
 --parameter-overrides \
 FlinkJarS3Bucket=${CF_FlinkJarS3Bucket}  \
 FlinkJarName=${CF_FlinkJarName}  \
 InputKdsStreamFirst=${CF_InputKdsStreamFirst}  \
 InputKdsStreamSecond=${CF_InputKdsStreamSecond}  \
 InputKdsStreamThird=${CF_InputKdsStreamThird}  \
 KdsInitialPosition=${CF_KdsInitialPosition}  \
 KinesisAnalyticsName=${CF_KinesisAnalyticsName}  \
 WindowSize=${CF_WindowSize}  \
 WindowSizeUOM=${CF_WindowSizeUOM}  \
 WatermarkDelay=${CF_WatermarkDelay}  \
 WatermarkDelayUOM=${CF_WatermarkDelayUOM}  \
 ThreaWatermarking=${CF_ThreaWatermarking}  \
 ThreadWindowing=${CF_ThreadWindowing}  \
 ThreadLogging=${CF_ThreadLogging}
 