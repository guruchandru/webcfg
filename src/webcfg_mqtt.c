/*
 * If not stated otherwise in this file or this component's Licenses.txt file the
 * following copyright and licenses apply:
 *
 * Copyright 2023 RDK Management
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <ctype.h>
#include <pthread.h>
#include <errno.h>
#include "webcfg_generic.h"
#include "webcfg_multipart.h"
#include "webcfg_mqtt.h"
#include "webcfg_rbus.h"
#include "webcfg_notify.h"

pthread_cond_t mqtt_sync_condition=PTHREAD_COND_INITIALIZER;
pthread_mutex_t mqtt_sync_mutex=PTHREAD_MUTEX_INITIALIZER;
static pthread_t mqttThreadId = 0;
static int systemStatus = 0;
static char g_deviceId[64]={'\0'};
//global flag to do bootupsync only once.
static int bootupsync = 0;
static int subscribeFlag = 0;
static int webcfg_onconnect_flag = 0;

static char g_systemReadyTime[64]={'\0'};
static char g_FirmwareVersion[64]={'\0'};
static char g_bootTime[64]={'\0'};
static char g_productClass[64]={'\0'};
static char g_ModelName[64]={'\0'};
static char g_PartnerID[64]={'\0'};
static char g_AccountID[64]={'\0'};
static char g_deviceWanMac[64]={'\0'};
static char *supportedVersion_header=NULL;
static char *supportedDocs_header=NULL;
static char *supplementaryDocs_header=NULL;

static void webcfgSubscribeCallbackHandler(
    rbusHandle_t handle,
    rbusEvent_t const* event,
    rbusEventSubscription_t* subscription);
static void webcfgOnMessageCallbackHandler(
    rbusHandle_t handle,
    rbusEvent_t const* event,
    rbusEventSubscription_t* subscription);
static void webcfgOnPublishCallbackHandler(
    rbusHandle_t handle,
    rbusEvent_t const* event,
    rbusEventSubscription_t* subscription);
void * processPayloadTask(void *tmp);

void initWebconfigMqttTask(unsigned long status)
{
	int err = 0;
	err = pthread_create(&mqttThreadId, NULL, WebconfigMqttTask, (void *) status);
	if (err != 0)
	{
		WebcfgError("Error creating WebconfigMqttTask thread :[%s]\n", strerror(err));
	}
	else
	{
		WebcfgInfo("WebconfigMqttTask thread created Successfully\n");
	}
}

void* WebconfigMqttTask(void *status)
{
	pthread_detach(pthread_self());
	pthread_mutex_lock (&mqtt_sync_mutex);
	int rc = -1;
	rbusHandle_t rbus_handle;

	systemStatus = (unsigned long)status;
	WebcfgInfo("WebconfigMqttTask\n");
	rbus_handle = get_global_rbus_handle();

	if(!rbus_handle)
	{
		WebcfgError("WebconfigMqttTask failed as rbus_handle is empty\n");
		return NULL;
	}

	checkMqttConnStatus();

	WebcfgInfo("MQTTCM broker is connected, proceed to subscribe\n");

	WebcfgInfo("rbus event subscribe to mqtt subscribe callback\n");
	rc = rbusEvent_Subscribe(rbus_handle, WEBCFG_SUBSCRIBE_CALLBACK, webcfgSubscribeCallbackHandler, NULL, 0);
	if(rc != RBUS_ERROR_SUCCESS)
	{
		WebcfgError("consumer: rbusEvent_Subscribe for %s failed: %d\n", WEBCFG_SUBSCRIBE_CALLBACK, rc);
		return NULL;
	}

	WebcfgInfo("rbus event subscribe to mqtt message callback\n");
	rc = rbusEvent_Subscribe(rbus_handle, WEBCFG_ONMESSAGE_CALLBACK, webcfgOnMessageCallbackHandler, NULL, 0);
	if(rc != RBUS_ERROR_SUCCESS)
	{
		WebcfgError("consumer: rbusEvent_Subscribe for %s failed: %d\n", WEBCFG_ONMESSAGE_CALLBACK, rc);
		return NULL;
	}

	WebcfgInfo("rbus event subscribe to mqtt publish callback\n");
	rc = rbusEvent_Subscribe(rbus_handle, WEBCFG_ONPUBLISH_CALLBACK, webcfgOnPublishCallbackHandler, NULL, 0);
	if(rc != RBUS_ERROR_SUCCESS)
	{
		WebcfgError("consumer: rbusEvent_Subscribe for %s failed: %d\n", WEBCFG_ONPUBLISH_CALLBACK, rc);
		return NULL;
	}
	webcfg_onconnect_flag = 1;
	if(!subscribeFlag)
	{
		WebcfgInfo("set mqtt subscribe request to mqtt CM\n");
		mqttSubscribeInit();
	}
	
	pthread_cond_wait(&mqtt_sync_condition, &mqtt_sync_mutex);
	webcfg_onconnect_flag = 0;
	
	WebcfgInfo("rbus event Unsubscribe to mqtt subscribed callback\n");
	rc = rbusEvent_Unsubscribe(rbus_handle, WEBCFG_SUBSCRIBE_CALLBACK);
	if(rc != RBUS_ERROR_SUCCESS)
	{
		WebcfgError("consumer: rbusEvent_Unsubscribe for %s failed: %d\n", WEBCFG_SUBSCRIBE_CALLBACK, rc);
	}

	WebcfgInfo("rbus event Unsubscribe to mqtt message callback\n");
	rc = rbusEvent_Unsubscribe(rbus_handle, WEBCFG_ONMESSAGE_CALLBACK);
	if(rc != RBUS_ERROR_SUCCESS)
	{
		WebcfgError("consumer: rbusEvent_Unsubscribe for %s failed: %d\n", WEBCFG_ONMESSAGE_CALLBACK, rc);
	}

	WebcfgInfo("rbus event Unsubscribe to mqtt publish callback\n");
	rc = rbusEvent_Unsubscribe(rbus_handle, WEBCFG_ONPUBLISH_CALLBACK);
	if(rc != RBUS_ERROR_SUCCESS)
	{
		WebcfgError("consumer: rbusEvent_Unsubscribe for %s failed: %d\n", WEBCFG_ONPUBLISH_CALLBACK, rc);
	}
	
	pthread_mutex_unlock (&mqtt_sync_mutex);	
	return NULL;
}

//To fetch mqttcm broker connection status at bootup.
int getMqttCMConnStatus()
{
	rbusValue_t value = NULL;
	char *status = NULL;
	int ret = 0, rc = 0;
        rc = rbus_get(get_global_rbus_handle(), MQTT_CONNSTATUS_PARAM, &value);

        if(rc == RBUS_ERROR_SUCCESS)
        {
		status = (char *)rbusValue_GetString(value, NULL);
		if(status !=NULL)
		{
			WebcfgInfo("mqttcm connection status fetched is %s\n", status);
			if(strncmp(status,  "Up", 2) == 0)
			{
				ret = 1;
			}
		}
		else
		{
			WebcfgError("mqttcm connect status is NULL\n");
		}
		rbusValue_Release(value);
        }
        else
        {
            WebcfgError("mqttcm connect status rbus_get failed, rc %d\n", rc);
        }
        return ret;
}

//This function checks the mqtt connection status and if it is "down" it will wait in back off retry
void checkMqttConnStatus()
{
	int connStatus = 0;
	int backoffRetryTime = 0;
	int backoff_max_time = 5;
	int max_retry_sleep;
	//Retry Backoff count shall start at c=2 & calculate 2^c - 1.
	int c =2;
        max_retry_sleep = (int) pow(2, backoff_max_time) -1;
        WebcfgInfo("max_retry_sleep is %d\n", max_retry_sleep );

	while(1)
	{
		if(backoffRetryTime < max_retry_sleep)
		{
			backoffRetryTime = (int) pow(2, c) -1;
		}

		WebcfgInfo("New backoffRetryTime value calculated as %d seconds\n", backoffRetryTime);
		connStatus = getMqttCMConnStatus();
		if(connStatus)
		{
			WebcfgDebug("MQTTCM broker is connected, proceed further\n");
			break;
		}
		else
		{
			WebcfgError("MQTTCM broker is not connected, waiting..\n");
			sleep(backoffRetryTime);
			c++;

			if(backoffRetryTime == max_retry_sleep)
			{
				c = 2;
				backoffRetryTime = 0;
				WebcfgInfo("backoffRetryTime reached max value, reseting to initial value\n");
			}
		}
	}
}

rbusError_t mqttSubscribeInit()
{
	rbusError_t ret = RBUS_ERROR_BUS_ERROR;
	rbusValue_t value;
	rbusObject_t inParams;
	rbusObject_t outParams;
	char subscribe_topic[256] = { 0 };
	static char clientID[64] = { 0 };

	rbusHandle_t rbus_handle = get_global_rbus_handle();

	if(!rbus_handle)
	{
		WebcfgError("mqttSubscribeInit failed as rbus_handle is empty\n");
		return ret;
	}

	char* temp_clientID = strdup(Get_Mqtt_ClientId());
	if( temp_clientID != NULL && strlen(temp_clientID) !=0 )
	{
	      strncpy(clientID, temp_clientID, sizeof(clientID)-1);
	      WebcfgInfo("clientID fetched from Get_Mqtt_ClientId is %s\n", clientID);
              WEBCFG_FREE(temp_clientID);
	}
	else
	{
		WebcfgError("clientID fetched from Get_Mqtt_ClientId is Invalid\n");
		return RBUS_ERROR_BUS_ERROR;
	}

	snprintf(subscribe_topic, MAX_MQTT_LEN, "%s%s/%s", MQTT_SUBSCRIBE_TOPIC, clientID, WEBCFG_MODULE_NAME);

	rbusObject_Init(&inParams, NULL);

	rbusValue_Init(&value);
	rbusValue_SetString(value, WEBCFG_MODULE_NAME);
	rbusObject_SetValue(inParams, "compname", value);
	rbusValue_Release(value);

	rbusValue_Init(&value);
	rbusValue_SetString(value, subscribe_topic);
	rbusObject_SetValue(inParams, "topic", value);
	rbusValue_Release(value);

	if(webcfg_onconnect_flag)
	{
		ret = rbusMethod_Invoke(rbus_handle, MQTT_SUBSCRIBE_PARAM, inParams, &outParams);

		if (ret)
		{
			WebcfgError("rbusMethod_Invoke for subscribe to topic failed: %s\n", rbusError_ToString(ret));
		}
		else
		{
			WebcfgInfo("rbusMethod_Invoke for subscribe to topic success\n");
			rbusObject_Release(outParams);
		}
	}

	rbusObject_Release(inParams);

	return ret;
}

static void webcfgSubscribeCallbackHandler(
    rbusHandle_t handle,
    rbusEvent_t const* event,
    rbusEventSubscription_t* subscription)
{
    rbusValue_t incoming_value;

    incoming_value = rbusObject_GetValue(event->data, "value");

    WebcfgInfo("Received on subscribe callback event %s\n", event->name);

    if(incoming_value)
    {
        char * inVal = (char *)rbusValue_GetString(incoming_value, NULL);
        WebcfgDebug("subscribe callback incoming_value: %s\n", inVal);
        if(strncmp(inVal, "success", 7) == 0)
        {
		subscribeFlag = 1;
		WebcfgInfo("Mqtt on subscribe is success\n");
		if(!bootupsync)
		{
			WebcfgInfo("mqtt is connected and subscribed to topic, trigger bootup sync to cloud.\n");
			int ret = triggerMqttSync();
			if(ret)
			{
				WebcfgInfo("Triggered bootup sync via mqtt\n");
			}
			else
			{
				WebcfgError("Failed to trigger bootup sync via mqtt\n");
			}
			bootupsync = 1;
		}
	}
    }
    (void)handle;
}

int triggerMqttSync()
{
	char *mqttheaderList = NULL;
	mqttheaderList = (char *) malloc(sizeof(char) * 1024);

	if(mqttheaderList != NULL)
	{
		createMqttHeader(&mqttheaderList);
		if(mqttheaderList !=NULL)
		{
			WebcfgInfo("mqttheaderList generated is \n%s len %zu\n", mqttheaderList, strlen(mqttheaderList));
			setBootupSyncHeader(mqttheaderList);
			WebcfgInfo("setBootupSyncHeader done\n");
			WEBCFG_FREE(mqttheaderList);
		}
		else
		{
			WebcfgError("Failed to generate mqttheaderList\n");
			WEBCFG_FREE(mqttheaderList);
			return 0;
		}
	}
	else
	{
		WebcfgError("Failed to allocate mqttheaderList\n");
		return 0;
	}
	return 1;
}

rbusError_t setBootupSyncHeader(char *publishGetVal)
{
	rbusError_t ret = RBUS_ERROR_BUS_ERROR;
	rbusValue_t value;
	rbusObject_t inParams;
	rbusObject_t outParams;
	char locationID[256] = { 0 };
	char publish_get_topic[256] = { 0 };
	Get_Mqtt_LocationId(locationID);
	static char g_ClientID[64] = { 0 };

	rbusHandle_t rbus_handle = get_global_rbus_handle();

	if(!rbus_handle)
	{
		WebcfgError("setBootupSyncHeader failed as rbus_handle is empty\n");
		return ret;
	}

	if( Get_Mqtt_ClientId() != NULL && strlen(Get_Mqtt_ClientId()) !=0 )
	{
	      strncpy(g_ClientID, Get_Mqtt_ClientId(), sizeof(g_ClientID)-1);
	      WebcfgInfo("g_ClientID fetched from Get_Mqtt_ClientId is %s\n", g_ClientID);
	}
	
	snprintf(publish_get_topic, MAX_MQTT_LEN, "%s%s/%s/%s/get", MQTT_PUBLISH_TOPIC_PREFIX, g_ClientID, locationID, WEBCFG_MODULE_NAME);

	rbusObject_Init(&inParams, NULL);
	
	rbusValue_Init(&value);
	rbusValue_SetString(value, publishGetVal);
	rbusObject_SetValue(inParams, "payload", value);
	rbusValue_Release(value);

	rbusValue_Init(&value);
	rbusValue_SetString(value, publish_get_topic);
	rbusObject_SetValue(inParams, "topic", value);
	rbusValue_Release(value);

	rbusValue_Init(&value);
	rbusValue_SetString(value, "0");
	rbusObject_SetValue(inParams, "qos", value);
	rbusValue_Release(value);

	ret = rbusMethod_Invoke(rbus_handle, WEBCFG_MQTT_PUBLISH_PARAM, inParams, &outParams);

	rbusObject_Release(inParams);

	if (ret)
	{
		WebcfgError("rbusMethod_Invoke for setBootupSyncHeader failed:%s\n", rbusError_ToString(ret));
	}
	else
	{
		WebcfgInfo("rbusMethod_Invoke for setBootupSyncHeader success\n");
		rbusObject_Release(outParams);
	}
	bootupsync = 1;
	return ret;
}


void convertToUppercase(char* deviceId)
{
	int j =0;
	while (deviceId[j])
	{
		deviceId[j] = toupper(deviceId[j]);
		j++;
	}
}

int createMqttHeader(char **header_list)
{
	char *version_header = NULL;
	char *accept_header = NULL;
	char *status_header=NULL;
	char *schema_header=NULL;
	char *bootTime = NULL, *bootTime_header = NULL;
	char *FwVersion = NULL, *FwVersion_header=NULL;
	char *supportedDocs = NULL;
	char *supportedVersion = NULL;
	char *supplementaryDocs = NULL;
        char *productClass = NULL, *productClass_header = NULL;
	char *ModelName = NULL, *ModelName_header = NULL;
	char *systemReadyTime = NULL, *systemReadyTime_header=NULL;
	char *telemetryVersion_header = NULL;
	char *PartnerID = NULL, *PartnerID_header = NULL;
	char *AccountID = NULL, *AccountID_header = NULL;
	char *deviceId_header = NULL, *doc_header = NULL;
	char *contenttype_header = NULL, *contentlen_header = NULL;
	char *DeviceWanMac = NULL, *DeviceWanMac_header = NULL;
	char *deviceMac = NULL;
	struct timespec cTime;
	char currentTime[32];
	char *currentTime_header=NULL;
	char *uuid_header = NULL;
	char *transaction_uuid = NULL;
	char version[512]={'\0'};
	size_t supported_doc_size = 0;
	size_t supported_version_size = 0;
	size_t supplementary_docs_size = 0;
	char docList[512] = {'\0'};

	WebcfgInfo("Start of createMqttHeader\n");

	if( strlen(g_deviceId) ==0 )
	{
	       deviceMac = get_deviceMAC();
	       if(deviceMac != NULL)
	       {
	              strncpy(g_deviceId, get_deviceMAC(), sizeof(g_deviceId)-1);
	              WebcfgInfo("g_deviceId fetched is %s\n", g_deviceId);
	       }
	}

	if(strlen(g_deviceId))
	{
		deviceId_header = (char *) malloc(sizeof(char)*MAX_BUF_SIZE);
		if(deviceId_header !=NULL)
		{
			convertToUppercase(g_deviceId);
			snprintf(deviceId_header, MAX_BUF_SIZE, "Device-Id: %s", g_deviceId);
			WebcfgInfo("deviceId_header formed %s\n", deviceId_header);
		}
		else
		{
			WebcfgError("Failed in memory allocation for deviceId_header\n");
		}
	}
	else
	{
		WebcfgError("Failed to get deviceId\n");
	}

	if(!get_global_supplementarySync())
	{
		version_header = (char *) malloc(sizeof(char)*MAX_BUF_SIZE);
		if(version_header !=NULL)
		{
			refreshConfigVersionList(version, 0, docList);
			snprintf(version_header, MAX_BUF_SIZE, "\r\nIF-NONE-MATCH:%s", ((strlen(version)!=0) ? version : "0"));
			WebcfgInfo("version_header formed %s\n", version_header);
		}
		else
		{
			WebcfgError("Failed in memory allocation for version_header\n");
		}
	}

	if(!get_global_supplementarySync())
	{
		WebcfgInfo("docList is %s\n", docList);

		doc_header = (char *) malloc(sizeof(char)*MAX_BUF_SIZE);
		if(doc_header !=NULL)
		{
			snprintf(doc_header, MAX_BUF_SIZE, "\r\nDoc-Name:%s", ((strlen(docList)!=0) ? docList : "NULL"));
			WebcfgInfo("doc_header formed %s\n", doc_header);
		}
		else
		{
			WebcfgError("Failed in memory allocation for doc_header\n");
		}
	}

	accept_header = (char *) malloc(sizeof(char)*MAX_BUF_SIZE);
	if(accept_header !=NULL)
	{
		snprintf(accept_header, MAX_BUF_SIZE, "\r\nAccept: application/msgpack");
	}
	else
	{
		WebcfgError("Failed in memory allocation for accept_header\n");		
	}

	schema_header = (char *) malloc(sizeof(char)*MAX_BUF_SIZE);
	if(schema_header !=NULL)
	{
		snprintf(schema_header, MAX_BUF_SIZE, "\r\nSchema-Version: %s", "v2.0");
		WebcfgInfo("schema_header formed %s\n", schema_header);
	}
	else
	{
		WebcfgError("Failed in memory allocation for schema_header\n");	
	}

	if(!get_global_supplementarySync())
	{
		if(supportedVersion_header == NULL)
		{
			supportedVersion = getsupportedVersion();

			if(supportedVersion !=NULL)
			{
				supported_version_size = strlen(supportedVersion)+strlen("X-System-Schema-Version: ");
				supportedVersion_header = (char *) malloc(supported_version_size+1);
				if(supportedVersion_header != NULL)
				{
					memset(supportedVersion_header,0,supported_version_size+1);
					WebcfgDebug("supportedVersion fetched is %s\n", supportedVersion);
					snprintf(supportedVersion_header, supported_version_size+1, "\r\nX-System-Schema-Version: %s", supportedVersion);
					WebcfgInfo("supportedVersion_header formed %s\n", supportedVersion_header);
				}
				else
				{
					WebcfgError("Failed in memory allocation for supportedVersion_header\n");	
				}
			}
			else
			{
				WebcfgInfo("supportedVersion fetched is NULL\n");
			}
		}
		else
		{
			WebcfgInfo("supportedVersion_header formed %s\n", supportedVersion_header);
		}

		if(supportedDocs_header == NULL)
		{
			supportedDocs = getsupportedDocs();

			if(supportedDocs !=NULL)
			{
				supported_doc_size = strlen(supportedDocs)+strlen("X-System-Supported-Docs: ");
				supportedDocs_header = (char *) malloc(supported_doc_size+1);
				if(supportedDocs_header != NULL)
				{
					memset(supportedDocs_header,0,supported_doc_size+1);
					WebcfgDebug("supportedDocs fetched is %s\n", supportedDocs);
					snprintf(supportedDocs_header, supported_doc_size+1, "\r\nX-System-Supported-Docs: %s", supportedDocs);
					WebcfgInfo("supportedDocs_header formed %s\n", supportedDocs_header);
				}
				else
				{
					WebcfgError("Failed in memory allocation for supportedDocs_header\n");	
				}
			}
			else
			{
				WebcfgInfo("SupportedDocs fetched is NULL\n");
			}
		}
		else
		{
			WebcfgInfo("supportedDocs_header formed %s\n", supportedDocs_header);
		}
	}
	else
	{
		if(supplementaryDocs_header == NULL)
		{
			supplementaryDocs = getsupplementaryDocs();

			if(supplementaryDocs !=NULL)
			{
				supplementary_docs_size = strlen(supplementaryDocs)+strlen("X-System-SupplementaryService-Sync: ");
				supplementaryDocs_header = (char *) malloc(supplementary_docs_size+1);
				if(supplementaryDocs_header != NULL)
				{
					memset(supplementaryDocs_header,0,supplementary_docs_size+1);
					WebcfgDebug("supplementaryDocs fetched is %s\n", supplementaryDocs);
					snprintf(supplementaryDocs_header, supplementary_docs_size+1, "\r\nX-System-SupplementaryService-Sync: %s", supplementaryDocs);
					WebcfgInfo("supplementaryDocs_header formed %s\n", supplementaryDocs_header);
				}
				else
				{
					WebcfgError("Failed in memory allocation for supplementaryDocs_header\n");	
				}	
			}
			else
			{
				WebcfgInfo("supplementaryDocs fetched is NULL\n");
			}
		}
		else
		{
			WebcfgInfo("supplementaryDocs_header formed %s\n", supplementaryDocs_header);
		}
	}

	if(strlen(g_bootTime) ==0)
	{
		bootTime = getDeviceBootTime();
		if(bootTime !=NULL)
		{
		       strncpy(g_bootTime, bootTime, sizeof(g_bootTime)-1);
		       WebcfgDebug("g_bootTime fetched is %s\n", g_bootTime);
		       WEBCFG_FREE(bootTime);
		}
	}

	if(strlen(g_bootTime))
	{
		bootTime_header = (char *) malloc(sizeof(char)*MAX_BUF_SIZE);
		if(bootTime_header !=NULL)
		{
			snprintf(bootTime_header, MAX_BUF_SIZE, "\r\nX-System-Boot-Time: %s", g_bootTime);
			WebcfgInfo("bootTime_header formed %s\n", bootTime_header);
		}
		else
		{
			WebcfgError("Failed in memory allocation for bootTime_header\n");
		}
	}
	else
	{
		WebcfgError("Failed to get bootTime\n");
	}

	if(strlen(g_FirmwareVersion) ==0)
	{
		FwVersion = getFirmwareVersion();
		if(FwVersion !=NULL)
		{
		       strncpy(g_FirmwareVersion, FwVersion, sizeof(g_FirmwareVersion)-1);
		       WebcfgDebug("g_FirmwareVersion fetched is %s\n", g_FirmwareVersion);
		       WEBCFG_FREE(FwVersion);
		}
	}

	if(strlen(g_FirmwareVersion))
	{
		FwVersion_header = (char *) malloc(sizeof(char)*MAX_BUF_SIZE);
		if(FwVersion_header !=NULL)
		{
			snprintf(FwVersion_header, MAX_BUF_SIZE, "\r\nX-System-Firmware-Version: %s", g_FirmwareVersion);
			WebcfgInfo("FwVersion_header formed %s\n", FwVersion_header);
		}
		else
		{
			WebcfgError("Failed in memory allocation for FwVersion_header\n");
		}
	}
	else
	{
		WebcfgError("Failed to get FwVersion\n");
	}

	status_header = (char *) malloc(sizeof(char)*MAX_BUF_SIZE);
	if(status_header !=NULL)
	{
		if(systemStatus !=0)
		{
			snprintf(status_header, MAX_BUF_SIZE, "\r\nX-System-Status: %s", "Non-Operational");
		}
		else
		{
			snprintf(status_header, MAX_BUF_SIZE, "\r\nX-System-Status: %s", "Operational");
		}
		WebcfgInfo("status_header formed %s\n", status_header);
	}
	else
	{
		WebcfgError("Failed in memory allocation for status_header\n");
	}

	memset(currentTime, 0, sizeof(currentTime));
	getCurrent_Time(&cTime);
	snprintf(currentTime,sizeof(currentTime),"%d",(int)cTime.tv_sec);
	currentTime_header = (char *) malloc(sizeof(char)*MAX_BUF_SIZE);
	if(currentTime_header !=NULL)
	{
		snprintf(currentTime_header, MAX_BUF_SIZE, "\r\nX-System-Current-Time: %s", currentTime);
		WebcfgInfo("currentTime_header formed %s\n", currentTime_header);
	}
	else
	{
		WebcfgError("Failed in memory allocation for currentTime_header\n");	
	}

        if(strlen(g_systemReadyTime) ==0)
        {
		WebcfgInfo("get_global_systemReadyTime\n");
                systemReadyTime = get_global_systemReadyTime();
                if(systemReadyTime !=NULL)
                {
                       strncpy(g_systemReadyTime, systemReadyTime, sizeof(g_systemReadyTime)-1);
                       WebcfgInfo("g_systemReadyTime fetched is %s\n", g_systemReadyTime);
                }
        }

        if(strlen(g_systemReadyTime))
        {
                systemReadyTime_header = (char *) malloc(sizeof(char)*MAX_BUF_SIZE);
                if(systemReadyTime_header !=NULL)
                {
			snprintf(systemReadyTime_header, MAX_BUF_SIZE, "\r\nX-System-Ready-Time: %s", g_systemReadyTime);
	                WebcfgInfo("systemReadyTime_header formed %s\n", systemReadyTime_header);
                }
		else
		{
			WebcfgError("Failed in memory allocation for systemReadyTime_header\n");
		}
        }
        else
        {
                WebcfgDebug("Failed to get systemReadyTime\n");
        }

	if(transaction_uuid == NULL)
	{
		transaction_uuid = generate_trans_uuid();
	}

	if(transaction_uuid !=NULL)
	{
		uuid_header = (char *) malloc(sizeof(char)*MAX_BUF_SIZE);
		if(uuid_header !=NULL)
		{
			snprintf(uuid_header, MAX_BUF_SIZE, "\r\nTransaction-ID: %s", transaction_uuid);
			WebcfgInfo("uuid_header formed %s\n", uuid_header);
		}
		else
		{
			WebcfgError("Failed in memory allocation for uuid_header\n");
		}
	}
	else
	{
		WebcfgError("Failed to generate transaction_uuid\n");
	}

	if(strlen(g_productClass) ==0)
	{
		productClass = getProductClass();
		if(productClass !=NULL)
		{
		       strncpy(g_productClass, productClass, sizeof(g_productClass)-1);
		       WebcfgDebug("g_productClass fetched is %s\n", g_productClass);
		       WEBCFG_FREE(productClass);
		}
	}

	if(strlen(g_productClass))
	{
		productClass_header = (char *) malloc(sizeof(char)*MAX_BUF_SIZE);
		if(productClass_header !=NULL)
		{
			snprintf(productClass_header, MAX_BUF_SIZE, "\r\nX-System-Product-Class: %s", g_productClass);
			WebcfgInfo("productClass_header formed %s\n", productClass_header);
		}
		else
		{
			WebcfgError("Failed in memory allocation for productClass_header\n");
		}
	}
	else
	{
		WebcfgError("Failed to get productClass\n");
	}

	if(strlen(g_ModelName) ==0)
	{
		ModelName = getModelName();
		if(ModelName !=NULL)
		{
		       strncpy(g_ModelName, ModelName, sizeof(g_ModelName)-1);
		       WebcfgDebug("g_ModelName fetched is %s\n", g_ModelName);
		       WEBCFG_FREE(ModelName);
		}
	}

	if(strlen(g_ModelName))
	{
		ModelName_header = (char *) malloc(sizeof(char)*MAX_BUF_SIZE);
		if(ModelName_header !=NULL)
		{
			snprintf(ModelName_header, MAX_BUF_SIZE, "\r\nX-System-Model-Name: %s", g_ModelName);
			WebcfgInfo("ModelName_header formed %s\n", ModelName_header);
		}
		else
		{
			WebcfgError("Failed in memory allocation for ModelName_header\n");
		}
	}
	else
	{
		WebcfgError("Failed to get ModelName\n");
	}

	if(strlen(g_PartnerID) ==0)
	{
		PartnerID = getPartnerID();
		if(PartnerID !=NULL)
		{
		       strncpy(g_PartnerID, PartnerID, sizeof(g_PartnerID)-1);
		       WebcfgDebug("g_PartnerID fetched is %s\n", g_PartnerID);
		       WEBCFG_FREE(PartnerID);
		}
	}

	if(strlen(g_PartnerID))
	{
		PartnerID_header = (char *) malloc(sizeof(char)*MAX_BUF_SIZE);
		if(PartnerID_header !=NULL)
		{
			snprintf(PartnerID_header, MAX_BUF_SIZE, "\r\nX-System-PartnerID: %s", g_PartnerID);
			WebcfgInfo("PartnerID_header formed %s\n", PartnerID_header);
		}
		else
		{
			WebcfgError("Failed in memory allocation for PartnerID_header\n");
		}
	}
	else
	{
		WebcfgError("Failed to get PartnerID\n");
	}

	contenttype_header = (char *) malloc(sizeof(char)*MAX_BUF_SIZE);
	if(contenttype_header !=NULL)
	{
		snprintf(contenttype_header, MAX_BUF_SIZE, "\r\nContent-type: application/json");
		WebcfgInfo("contenttype_header formed %s\n", contenttype_header);
	}
	else
	{
		WebcfgError("Failed in memory allocation for contenttype_header\n");
	}
	contentlen_header = (char *) malloc(sizeof(char)*MAX_BUF_SIZE);
	if(contentlen_header !=NULL)
	{
		snprintf(contentlen_header, MAX_BUF_SIZE, "\r\nContent-length: 0");
		WebcfgInfo("contentlen_header formed %s\n", contentlen_header);
	}
	else
	{
		WebcfgError("Failed in memory allocation for contentlen_header\n");
	}

	if(strlen(g_deviceWanMac) ==0)
	{
		DeviceWanMac = get_deviceWanMAC();
		if(DeviceWanMac !=NULL)
		{
		       strncpy(g_deviceWanMac, DeviceWanMac, sizeof(g_deviceWanMac)-1);
		       WebcfgDebug("g_deviceWanMac fetched is %s\n", g_deviceWanMac);
		}
	}

	if(strlen(g_deviceWanMac))
	{
		DeviceWanMac_header = (char *) malloc(sizeof(char)*MAX_BUF_SIZE);
		if(DeviceWanMac_header !=NULL)
		{
			snprintf(DeviceWanMac_header, MAX_BUF_SIZE, "\r\nX-System-Wan-Mac: %s", g_deviceWanMac);
			WebcfgInfo("DeviceWanMac_header formed %s\n", DeviceWanMac_header);
		}
		else
		{
			WebcfgError("Failed in memory allocation for DeviceWanMac_header\n");
		}
	}
	else
	{
		WebcfgError("Failed to get DeviceWanMac_header\n");
	}

	//Addtional headers for telemetry sync
	if(get_global_supplementarySync())
	{
		telemetryVersion_header = (char *) malloc(sizeof(char)*MAX_BUF_SIZE);
		if(telemetryVersion_header !=NULL)
		{
			snprintf(telemetryVersion_header, MAX_BUF_SIZE, "\r\nX-System-Telemetry-Profile-Version: %s", "2.0");
			WebcfgInfo("telemetryVersion_header formed %s\n", telemetryVersion_header);
		}
		else
		{
			WebcfgError("Failed in memory allocation for telemetryVersion_header\n");
		}		

		if(strlen(g_AccountID) ==0)
		{
			AccountID = getAccountID();
			if(AccountID !=NULL)
			{
			       strncpy(g_AccountID, AccountID, sizeof(g_AccountID)-1);
			       WebcfgDebug("g_AccountID fetched is %s\n", g_AccountID);
			       WEBCFG_FREE(AccountID);
			}
		}

		if(strlen(g_AccountID))
		{
			AccountID_header = (char *) malloc(sizeof(char)*MAX_BUF_SIZE);
			if(AccountID_header !=NULL)
			{
				snprintf(AccountID_header, MAX_BUF_SIZE, "\r\nX-System-AccountID: %s", g_AccountID);
				WebcfgInfo("AccountID_header formed %s\n", AccountID_header);
			}
			else
			{
				WebcfgError("Failed in memory allocation for AccountID_header\n");
			}				
		}
		else
		{
			WebcfgError("Failed to get AccountID\n");
		}

		/*if(strlen(g_deviceWanMac) ==0)
		{
			DeviceWanMac = get_deviceWanMAC();
			if(DeviceWanMac !=NULL)
			{
			       strncpy(g_deviceWanMac, DeviceWanMac, sizeof(g_deviceWanMac)-1);
			       WebcfgDebug("g_deviceWanMac fetched is %s\n", g_deviceWanMac);
			}
		}

		if(strlen(g_deviceWanMac))
		{
			DeviceWanMac_header = (char *) malloc(sizeof(char)*MAX_BUF_SIZE);
			if(DeviceWanMac_header !=NULL)
			{
				snprintf(DeviceWanMac_header, MAX_BUF_SIZE, "\r\nX-System-Wan-Mac: %s", g_deviceWanMac);
				WebcfgInfo("DeviceWanMac_header formed %s\n", DeviceWanMac_header);
			}
			else
			{
				WebcfgError("Failed in memory allocation for DeviceWanMac_header\n");
			}
		}
		else
		{
			WebcfgError("Failed to get DeviceWanMac_header\n");
		}*/

	}

	if(!get_global_supplementarySync())
	{
		WebcfgInfo("Framing primary sync header\n");
		snprintf(*header_list, 1024, "%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s\r\n\r\n", (deviceId_header!=NULL)?deviceId_header:"",(doc_header!=NULL)?doc_header:"", (version_header!=NULL)?version_header:"", (accept_header!=NULL)?accept_header:"", (schema_header!=NULL)?schema_header:"", (supportedVersion_header!=NULL)?supportedVersion_header:"", (supportedDocs_header!=NULL)?supportedDocs_header:"", (bootTime_header!=NULL)?bootTime_header:"", (FwVersion_header!=NULL)?FwVersion_header:"", (status_header!=NULL)?status_header:"", (currentTime_header!=NULL)?currentTime_header:"", (systemReadyTime_header!=NULL)?systemReadyTime_header:"", (uuid_header!=NULL)?uuid_header:"", (productClass_header!=NULL)?productClass_header:"", (ModelName_header!=NULL)?ModelName_header:"", (contenttype_header!=NULL)?contenttype_header:"", (contentlen_header!=NULL)?contentlen_header:"",(PartnerID_header!=NULL)?PartnerID_header:"", (DeviceWanMac_header!=NULL)?DeviceWanMac_header:"");
		WEBCFG_FREE(DeviceWanMac_header);
	}
	else
	{
		WebcfgInfo("Framing supplementary sync header\n");
		snprintf(*header_list, 1024, "%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s\r\n\r\n", (deviceId_header!=NULL)?deviceId_header:"",(doc_header!=NULL)?doc_header:"", (version_header!=NULL)?version_header:"", (accept_header!=NULL)?accept_header:"", (schema_header!=NULL)?schema_header:"", (supportedVersion_header!=NULL)?supportedVersion_header:"", (supportedDocs_header!=NULL)?supportedDocs_header:"", (bootTime_header)?bootTime_header:"", (FwVersion_header)?FwVersion_header:"", (status_header)?status_header:"", (currentTime_header)?currentTime_header:"", (systemReadyTime_header!=NULL)?systemReadyTime_header:"", (uuid_header!=NULL)?uuid_header:"", (productClass_header!=NULL)?productClass_header:"", (ModelName_header!=NULL)?ModelName_header:"",(contenttype_header!=NULL)?contenttype_header:"", (contentlen_header!=NULL)?contentlen_header:"", (PartnerID_header!=NULL)?PartnerID_header:"",(supplementaryDocs_header!=NULL)?supplementaryDocs_header:"", (telemetryVersion_header!=NULL)?telemetryVersion_header:"", (AccountID_header!=NULL)?AccountID_header:"", (DeviceWanMac_header!=NULL)?DeviceWanMac_header:"");
		WEBCFG_FREE(DeviceWanMac_header);
	}
	writeToDBFile("/tmp/header_list.txt", *header_list, strlen(*header_list));
	WebcfgDebug("mqtt header_list is \n%s\n", *header_list);
	freeMqttHeaders(contentlen_header,contenttype_header,PartnerID_header,ModelName_header,productClass_header,uuid_header,systemReadyTime_header,currentTime_header,status_header,FwVersion_header,bootTime_header,schema_header,accept_header,version_header,doc_header,deviceId_header,transaction_uuid);
	return 0;
}

void freeMqttHeaders(char *contentlen_header, char *contenttype_header, char *PartnerID_header, char *ModelName_header, char *productClass_header, char *uuid_header, char *systemReadyTime_header, char *currentTime_header, char *status_header, char *FwVersion_header, char *bootTime_header, char *schema_header, char *accept_header, char *version_header, char *doc_header, char *deviceId_header, char *transaction_uuid)
{
	WEBCFG_FREE(contentlen_header);
	WEBCFG_FREE(contenttype_header);
	WEBCFG_FREE(PartnerID_header);
	WEBCFG_FREE(ModelName_header);
	WEBCFG_FREE(productClass_header);
	WEBCFG_FREE(uuid_header);
	WEBCFG_FREE(systemReadyTime_header);
	WEBCFG_FREE(currentTime_header);
	WEBCFG_FREE(status_header);
	WEBCFG_FREE(FwVersion_header);
	WEBCFG_FREE(bootTime_header);
	WEBCFG_FREE(schema_header);
	WEBCFG_FREE(accept_header);
	WEBCFG_FREE(version_header);
	WEBCFG_FREE(doc_header);
	WEBCFG_FREE(deviceId_header);
	WEBCFG_FREE(transaction_uuid);
}

//new thread created for processPayload 
void * processPayloadTask(void *tmp)
{
	pthread_detach(pthread_self());
	if(tmp != NULL)
	{
		msg_t *msg = NULL;
		msg = (msg_t *) tmp;
		int len=0;
		void * data = NULL;
		data = msg->data;
		len = msg->len;

		processPayload((char *)data, len);
		WEBCFG_FREE(tmp);
	}
	else
	{
		WebcfgError("tmp data is NULL\n");
	}

	return NULL;
}

static void webcfgOnMessageCallbackHandler(
    rbusHandle_t handle,
    rbusEvent_t const* event,
    rbusEventSubscription_t* subscription)
{
    msg_t *msg=NULL;
    rbusValue_t incoming_value;

    incoming_value = rbusObject_GetValue(event->data, "value");

    WebcfgInfo("Received on message callback event %s\n", event->name);

    if(incoming_value != NULL)
    {	
	int err = 0;
	pthread_t threadId;
	char *data = NULL;
	int len = 0;
	char *temp_data = NULL;
	
	WebcfgDebug("on message incoming_value: %s\n", rbusValue_GetBytes(incoming_value, NULL));

	data = (char *)rbusValue_GetBytes(incoming_value, &len);
	temp_data = malloc(sizeof(char) * len + 1);
	if(temp_data != NULL)
	{
		temp_data = memcpy(temp_data, data, len + 1);
	}
	else
	{
		WebcfgError("Failed in memory allocation for temp_data\n");	
	}
	msg = (msg_t *)malloc(sizeof(msg_t));
	if(msg != NULL)
	{
		memset(msg,0,sizeof(msg_t));
		msg->data = temp_data;
		msg->len = len;

		WebcfgDebug("data is %s\n", temp_data);
		WebcfgDebug("Received msg len is %d\n", len);

		err = pthread_create(&threadId, NULL, processPayloadTask, (void *) msg);
		if (err != 0) 
		{
			WebcfgError("Error creating WebConfig processPayload thread :[%s]\n", strerror(err));
		}
		else
		{
			WebcfgInfo("WebConfig processPayload Thread created Successfully.\n");
		}
	}
	else
	{
		WebcfgError("Failed in memory allocation for msg\n");
	}

    }
    (void)handle;
}

static void webcfgOnPublishCallbackHandler(
    rbusHandle_t handle,
    rbusEvent_t const* event,
    rbusEventSubscription_t* subscription)
{
    rbusValue_t incoming_value;

    incoming_value = rbusObject_GetValue(event->data, "value");

    WebcfgInfo("Received on publish callback event %s\n", event->name);

    if(incoming_value)
    {
        char * inVal = (char *)rbusValue_GetString(incoming_value, NULL);
        WebcfgDebug("subscribe callback incoming_value: %s\n", inVal);
        WebcfgInfo("%s\n", inVal);
    }
    (void)handle;
}

int handleMqttResponse(int response_code, char *contentLength, char* transaction_uuid)
{
	int first_digit=0;
	char version[512]={'\0'};
	char docList[512]={'\0'};
	uint32_t db_root_version = 0;
	char *db_root_string = NULL;
	int subdocList = 0;

	if(response_code == 304)
	{
		WebcfgInfo("webConfig is in sync with cloud. response_code:%d\n", response_code);
		WebcfgDebug("db_root_version is %d and db_root_string is %s\n", db_root_version, db_root_string);
		getRootDocVersionFromDBCache(&db_root_version, &db_root_string, &subdocList);
		addWebConfgNotifyMsg(NULL, db_root_version, NULL, NULL, transaction_uuid, 0, "status", 0, db_root_string, response_code);
		if(db_root_string !=NULL)
		{
			WEBCFG_FREE(db_root_string);
		}
		WEBCFG_FREE(transaction_uuid);
		return 1;
	}
	else if(response_code == 200)
	{
		//After factory reset when server sends 200 with empty config, set POST-NONE root version
		if((contentLength !=NULL) && (strcmp(contentLength, "0") == 0))
		{
			WebcfgInfo("webConfigData content length is %s\n", contentLength);
			WebcfgDebug("version is %s \n", version);
			refreshConfigVersionList(version, response_code, docList);
			set_global_contentLen(NULL);
			WEBCFG_FREE(transaction_uuid);
			return 1;
		}
		else
		{
			WebcfgDebug("webConfig is not in sync with cloud. response_code:%d\n", response_code);
			return 0;
		}
	}
	else if(response_code == 204)
	{
		WebcfgInfo("No configuration available for this device. response_code:%d\n", response_code);
		WebcfgDebug("db_root_version is %d and db_root_string is %s\n", db_root_version, db_root_string);
		getRootDocVersionFromDBCache(&db_root_version, &db_root_string, &subdocList);
		addWebConfgNotifyMsg(NULL, db_root_version, NULL, NULL, transaction_uuid, 0, "status", 0, db_root_string, response_code);
		if(db_root_string !=NULL)
		{
			WEBCFG_FREE(db_root_string);
		}
		WEBCFG_FREE(transaction_uuid);
		return 1;
	}
	else if(response_code == 429)
	{
		WebcfgInfo("No action required from client. response_code:%d\n", response_code);
		WebcfgDebug("db_root_version is %d and db_root_string is %s\n", db_root_version, db_root_string);
		getRootDocVersionFromDBCache(&db_root_version, &db_root_string, &subdocList);
		addWebConfgNotifyMsg(NULL, db_root_version, NULL, NULL, transaction_uuid, 0, "status", 0, db_root_string, response_code);
		if(db_root_string !=NULL)
		{
			WEBCFG_FREE(db_root_string);
		}
		WEBCFG_FREE(transaction_uuid);
		return 1;
	}
	first_digit = (int)(response_code / pow(10, (int)log10(response_code)));
	if((response_code !=403) && (first_digit == 4)) //4xx
	{
		WebcfgInfo("Action not supported. response_code:%d\n", response_code);
		if (response_code == 404)
		{
			//To set POST-NONE root version when 404
			refreshConfigVersionList(version, response_code, docList);
		}
		WebcfgDebug("db_root_version is %d and db_root_string is %s\n", db_root_version, db_root_string);
		getRootDocVersionFromDBCache(&db_root_version, &db_root_string, &subdocList);
		addWebConfgNotifyMsg(NULL, db_root_version, NULL, NULL, transaction_uuid, 0, "status", 0, db_root_string, response_code);
		if(db_root_string !=NULL)
		{
			WEBCFG_FREE(db_root_string);
		}
		WEBCFG_FREE(transaction_uuid);
		return 1;
	}
	else //5xx & all other errors
	{
		WebcfgError("Error code returned, need to do curl retry to server. response_code:%d\n", response_code);
		WebcfgDebug("db_root_version is %d and db_root_string is %s\n", db_root_version, db_root_string);
		getRootDocVersionFromDBCache(&db_root_version, &db_root_string, &subdocList);
		addWebConfgNotifyMsg(NULL, db_root_version, NULL, NULL, transaction_uuid, 0, "status", 0, db_root_string, response_code);
		if(db_root_string !=NULL)
		{
			WEBCFG_FREE(db_root_string);
		}
		WEBCFG_FREE(transaction_uuid);
	}
	return 1;
}

int processPayload(char * data, int dataSize)
{
	int mstatus = 0;

	char *transaction_uuid =NULL;
	char ct[256] = {0};
	char * data_body = malloc(sizeof(char) * dataSize+1);

	if(data_body == NULL)
	{
		WebcfgError("Failed in memory allocation for data_body\n");
		return 1;
	}

	memset(data_body, 0, sizeof(char) * dataSize+1);
	data_body = memcpy(data_body, data, dataSize+1);
	data_body[dataSize] = '\0';
	char *ptr_count = data_body;
	char *ptr1_count = data_body;
	char *temp = NULL;
	char *etag_header = NULL;
	char* version = NULL;
	char* res_header = NULL;
	char* res_code = NULL;
	char* contentLengthString = NULL;
	char* contentlength_header = NULL;
	int response_code = 0;
	WebcfgDebug("ptr_count is %s\n", ptr_count);

	while((ptr_count - data_body) < dataSize )
	{
		ptr_count = memchr(ptr_count, 'H', dataSize - (ptr_count - data_body));
		if(ptr_count == NULL)
		{
			WebcfgError("HTTP response code not found\n");
			break;
		}
		if(0 == memcmp(ptr_count, "HTTP", strlen("HTTP")))
		{
			ptr1_count = memchr(ptr_count+1, '\r', dataSize - (ptr_count - data_body));
			res_header = strndup(ptr_count, (ptr1_count-ptr_count));
			if(res_header != NULL)
			{
				//Extract HTTP response code using space as delimiter
				res_code = strtok(res_header, " ");
				if(res_code !=NULL)
				{
					res_code = strtok(NULL, " ");
					WebcfgInfo("response code extracted is %s\n", res_code);
					if(res_code != NULL)
					{
						response_code = atoi(res_code);
					}
					res_code++;
					break;
				}
			}
		}
		ptr_count++;
	}

	ptr_count = data_body;
	ptr1_count = data_body;
	while((ptr_count - data_body) < dataSize )
	{
		ptr_count = memchr(ptr_count, 'C', dataSize - (ptr_count - data_body));
		if(ptr_count == NULL)
		{
			WebcfgError("Content-type header not found\n");
			break;
		}
/*since key name is Content-type in test blob using this for original code base use Content-Type*/
		if(0 == memcmp(ptr_count, "Content-Type:", strlen("Content-Type:")))
		{
			ptr1_count = memchr(ptr_count+1, '\r', dataSize - (ptr_count - data_body));
			temp = strndup(ptr_count, (ptr1_count-ptr_count));
			strncpy(ct,temp,(sizeof(ct)-1));
			break;
		}
		ptr_count++;
	}

	ptr_count = data_body;
	ptr1_count = data_body;
	while((ptr_count - data_body) < dataSize )
	{
		ptr_count = memchr(ptr_count, 'E', dataSize - (ptr_count - data_body));
		if(ptr_count == NULL)
		{
			WebcfgError("etag_header not found\n");
			break;
		}
		if(0 == memcmp(ptr_count, "Etag:", strlen("Etag:")))
		{
			ptr1_count = memchr(ptr_count+1, '\r', dataSize - (ptr_count - data_body));
			etag_header = strndup(ptr_count, (ptr1_count-ptr_count));
			if(etag_header !=NULL)
			{
				WebcfgInfo("etag header extracted is %s\n", etag_header);
				//Extract root version from Etag: <value> header.
				version = strtok(etag_header, ":");
				if(version !=NULL)
				{
					version = strtok(NULL, ":");
					version++;
					//g_ETAG should be updated only for primary sync.
					if(!get_global_supplementarySync())
					{
						set_global_ETAG(version);
						WebcfgInfo("g_ETAG updated in processPayload %s\n", get_global_ETAG());
					}
					break;
				}
			}
		}
		ptr_count++;
	}

	ptr_count = data_body;
	ptr1_count = data_body;
	while((ptr_count - data_body) < dataSize )
	{
		ptr_count = memchr(ptr_count, 'C', dataSize - (ptr_count - data_body));
		if(ptr_count == NULL)
		{
			WebcfgError("Content-Length not found\n");
			break;
		}
		if(0 == memcmp(ptr_count, "Content-Length:", strlen("Content-Length:")))
		{
			ptr1_count = memchr(ptr_count+1, '\r', dataSize - (ptr_count - data_body));
			contentlength_header = strndup(ptr_count, (ptr1_count-ptr_count));
			if(contentlength_header !=NULL)
			{
				//Extract content length from Content-Length: <value> header.
				contentLengthString = strtok(contentlength_header, ":");
				if(contentLengthString !=NULL)
				{
					contentLengthString = strtok(NULL, ":");
					contentLengthString++;
					WebcfgInfo("contentlength extracted is %s\n", contentLengthString);
					break;
				}
			}
		}
		ptr_count++;
	}

	WEBCFG_FREE(data_body);
	WEBCFG_FREE(temp);
	WEBCFG_FREE(etag_header);
	transaction_uuid = generate_trans_uuid();

	WebcfgInfo("contentlength extracted is %s\n", contentLengthString);
	if(handleMqttResponse(response_code, contentLengthString, transaction_uuid) == 1)
	{
		WEBCFG_FREE(data);
		return 1;
	}

	if(data !=NULL)
	{
		WebcfgInfo("webConfigData fetched successfully\n");
		WebcfgInfo("webConfig is not in sync with cloud. response_code:%d\n", response_code);
		WebcfgDebug("parseMultipartDocument\n");
		mstatus = parseMultipartDocument(data, ct, (size_t)dataSize, transaction_uuid);

		if(mstatus == WEBCFG_SUCCESS)
		{
			WebcfgInfo("webConfigData applied successfully\n");
		}
		else
		{
			WebcfgDebug("Failed to apply root webConfigData received from server\n");
		}
	}
	else
	{
		WEBCFG_FREE(transaction_uuid);
		WebcfgError("webConfigData is empty, need to retry\n");
	}
	return 1;
}

char * createMqttPubHeader(char * payload, char * dest, ssize_t * payload_len)
{
	char * destination = NULL;
	char * content_type = NULL;
	char * content_length = NULL;
	char *pub_headerlist = NULL;

	pub_headerlist = (char *) malloc(sizeof(char) * 1024);

	if(pub_headerlist != NULL)
	{
		if(payload != NULL)
		{
			if(dest != NULL)
			{
				destination = (char *) malloc(sizeof(char)*MAX_BUF_SIZE);
				if(destination !=NULL)
				{
					snprintf(destination, MAX_BUF_SIZE, "Destination: %s", dest);
					WebcfgInfo("destination formed %s\n", destination);
				}
				else
				{
					WebcfgError("Failed in memory allocation for destination\n");					
				}
			}

			content_type = (char *) malloc(sizeof(char)*MAX_BUF_SIZE);
			if(content_type !=NULL)
			{
				snprintf(content_type, MAX_BUF_SIZE, "\r\nContent-type: application/json");
				WebcfgInfo("content_type formed %s\n", content_type);
			}
			else
			{
				WebcfgError("Failed in memory allocation for content_type\n");				
			}

			content_length = (char *) malloc(sizeof(char)*MAX_BUF_SIZE);
			if(content_length !=NULL)
			{
				snprintf(content_length, MAX_BUF_SIZE, "\r\nContent-length: %zu", strlen(payload));
				WebcfgInfo("content_length formed %s\n", content_length);
			}
			else
			{
				WebcfgError("Failed in memory allocation for content_length\n");
			}

			WebcfgInfo("Framing publish notification header\n");
			snprintf(pub_headerlist, 1024, "%s%s%s\r\n\r\n%s\r\n", (destination!=NULL)?destination:"", (content_type!=NULL)?content_type:"", (content_length!=NULL)?content_length:"",(payload!=NULL)?payload:"");
	    }
		WebcfgInfo("mqtt pub_headerlist is \n%s", pub_headerlist);
		*payload_len = strlen(pub_headerlist);
		WEBCFG_FREE(content_length);
		WEBCFG_FREE(content_type);
		WEBCFG_FREE(destination);
	}
	else
	{
		WebcfgError("Failed in memory allocation for pub_headerlist\n");
	}	
	return pub_headerlist;
}

rbusError_t setPublishNotification(char *publishNotifyVal)
{
	rbusError_t ret = RBUS_ERROR_BUS_ERROR;
	rbusValue_t value;
	rbusObject_t inParams;
	rbusObject_t outParams;
	char locationID[256] = { 0 };
	char publish_get_topic[256] = { 0 };
	Get_Mqtt_LocationId(locationID);
	static char g_ClientID[64] = { 0 };

	rbusHandle_t rbus_handle = get_global_rbus_handle();

	if(!rbus_handle)
	{
		WebcfgError("setPublishNotification failed as rbus_handle is empty\n");
		return ret;
	}

	if( Get_Mqtt_ClientId() != NULL && strlen(Get_Mqtt_ClientId()) !=0 )
	{
		strncpy(g_ClientID, Get_Mqtt_ClientId(), sizeof(g_ClientID)-1);
		WebcfgInfo("g_ClientID fetched from Get_Mqtt_ClientId is %s\n", g_ClientID);
	}

	snprintf(publish_get_topic, MAX_MQTT_LEN, "%s%s/%s/%s/poke", MQTT_PUBLISH_TOPIC_PREFIX, g_ClientID, locationID, WEBCFG_MODULE_NAME);

	rbusObject_Init(&inParams, NULL);

	rbusValue_Init(&value);
	rbusValue_SetString(value, publishNotifyVal);
	rbusObject_SetValue(inParams, "payload", value);
	rbusValue_Release(value);

	rbusValue_Init(&value);
	rbusValue_SetString(value, publish_get_topic);
	rbusObject_SetValue(inParams, "topic", value);
	rbusValue_Release(value);

	rbusValue_Init(&value);
	rbusValue_SetString(value, "0");
	rbusObject_SetValue(inParams, "qos", value);
	rbusValue_Release(value);

	ret = rbusMethod_Invoke(rbus_handle, WEBCFG_MQTT_PUBLISH_PARAM, inParams, &outParams);
	rbusObject_Release(inParams);

	if (ret)
	{
		WebcfgError("rbusMethod_Invoke for setPublishNotification failed:%s\n", rbusError_ToString(ret));
	}
	else
	{
		WebcfgInfo("rbusMethod_Invoke for setPublishNotification success\n");
		rbusObject_Release(outParams);
	}
	return ret;
}

void publish_notify_mqtt(void *payload, ssize_t len, char * dest)
{
    int rc;
	if(dest != NULL)
	{
		ssize_t payload_len = 0;
		char * pub_payload = createMqttPubHeader(payload, dest, &payload_len);
		if(pub_payload != NULL)
		{
			rc = setPublishNotification(pub_payload);
			if(rc != 0)
			{
					WebcfgError("setPublishNotification failed, rc %d\n", rc);
			}
			else
			{
				WebcfgInfo("Publish payload success %d\n", rc);
			}
			WEBCFG_FREE(pub_payload);
		}
	}
}

int sendNotification_mqtt(char *payload, char *destination, wrp_msg_t *notif_wrp_msg, void *msg_bytes)
{
	if(webcfg_onconnect_flag)
	{
		WebcfgInfo("publish_notify_mqtt with json string payload\n");
		char *payload_str = strdup(payload);
		WebcfgInfo("payload_str %s len %zu\n", payload_str, strlen(payload_str));
		publish_notify_mqtt(payload_str, strlen(payload_str), destination);
		WEBCFG_FREE(payload_str);
		WebcfgInfo("publish_notify_mqtt done\n");
		return 1;
	}
	else
	{
		WebcfgError("Failed to publish notification as mqtt broker is not connected\n");
	}
	return 0;
}
pthread_cond_t *get_global_mqtt_sync_condition(void)
{
    return &mqtt_sync_condition;
}
