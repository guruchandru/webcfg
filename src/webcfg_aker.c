/*
 * Copyright 2020 Comcast Cable Communications Management, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>
#include <math.h>
#include <string.h>
#include "webcfg.h"
#include "webcfg_log.h"
#include "webcfg_aker.h"
#include "webcfg_generic.h"
#include "webcfg_event.h"
#include "webcfg_blob.h"
#include "webcfg_param.h"
#include <wrp-c.h>
#include <wdmp-c.h>
#include <msgpack.h>
#include <libparodus.h>
#include <cJSON.h>
/*----------------------------------------------------------------------------*/
/*                                   Macros                                   */
/*----------------------------------------------------------------------------*/
#define SERVICE_STATUS 		"service-status"
#define AKER_STATUS_ONLINE      "online"
#define WAIT_TIME_IN_SEC        30
#define CONTENT_TYPE_JSON       "application/json"
#define AKER_UPDATE_PARAM       "Device.DeviceInfo.X_RDKCENTRAL-COM_Aker.Update"
#define AKER_DELETE_PARAM       "Device.DeviceInfo.X_RDKCENTRAL-COM_Aker.Delete"
/*----------------------------------------------------------------------------*/
/*                               Data Structures                              */
/*----------------------------------------------------------------------------*/
/* none */

/*----------------------------------------------------------------------------*/
/*                            File Scoped Variables                           */
/*----------------------------------------------------------------------------*/
int akerDocVersion=0;
uint16_t akerTransId=0;
int wakeFlag = 0;
static int akerTestFlag = 0;
char *aker_status = NULL;
bool send_aker_flag = false;
pthread_mutex_t client_mut=PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t client_con=PTHREAD_COND_INITIALIZER;
/*----------------------------------------------------------------------------*/
/*                             Function Prototypes                            */
/*----------------------------------------------------------------------------*/
static char *decodePayload(char *payload);
static void handleAkerStatus(int status, char *payload);
static void free_crud_message(wrp_msg_t *msg);
static char* parsePayloadForAkerStatus(char *payload);
static char *get_global_status();
static void set_global_status(char *status);
/*----------------------------------------------------------------------------*/
/*                             External Functions                             */
/*----------------------------------------------------------------------------*/

pthread_cond_t *get_global_client_con(void)
{
    return &client_con;
}

pthread_mutex_t *get_global_client_mut(void)
{
    return &client_mut;
}

bool get_send_aker_flag()
{
    return send_aker_flag;
}

void set_send_aker_flag(bool flag)
{
    send_aker_flag = flag;
}

int akerwait__ (unsigned int secs)
{
  int shutdown_flag;
  struct timespec svc_timer;

  clock_gettime(CLOCK_REALTIME, &svc_timer);
  svc_timer.tv_sec += secs;
  pthread_mutex_lock(get_global_sync_mutex());
  pthread_cond_timedwait (get_global_sync_condition(), get_global_sync_mutex(), &svc_timer);
  shutdown_flag = get_global_shutdown();
  pthread_mutex_unlock (get_global_sync_mutex());
  return shutdown_flag;
}

int send_aker_blob(char *paramName, char *blob, uint32_t blobSize, uint16_t docTransId, int version)
{
	WebcfgDebug("Aker blob is %s\n",blob);
	wrp_msg_t *msg = NULL;
	int ret = WDMP_FAILURE;
	char source[MAX_BUF_SIZE] = {'\0'};
	char destination[MAX_BUF_SIZE] = {'\0'};
	char trans_uuid[MAX_BUF_SIZE/4] = {'\0'};
	int sendStatus = -1;
	int retry_count = 0;
	int backoffRetryTime = 0;
	int c=2;

	if(paramName == NULL)
	{
		WebcfgError("aker paramName is NULL\n");
		return ret;
	}
	akerDocVersion = version;
	akerTransId = docTransId;

	msg = (wrp_msg_t *)malloc(sizeof(wrp_msg_t));
	if(msg != NULL)
	{
		memset(msg, 0, sizeof(wrp_msg_t));
		WebcfgDebug("Aker paramName is %s size %ld\n", paramName, sizeof(AKER_UPDATE_PARAM));
		if((strncmp(paramName, AKER_UPDATE_PARAM, sizeof(AKER_UPDATE_PARAM)) ==0) && (blobSize > 0))
		{
			msg->msg_type = WRP_MSG_TYPE__UPDATE;
			msg->u.crud.payload = blob;
			msg->u.crud.payload_size = blobSize;
		}
		else if(strncmp(paramName, AKER_DELETE_PARAM, sizeof(AKER_DELETE_PARAM)) ==0)
		{
			msg->msg_type = WRP_MSG_TYPE__DELETE;
			msg->u.crud.payload = NULL;
			msg->u.crud.payload_size = 0;
		}
		else //aker schedule RETRIEVE is not supported through webconfig.
		{
			WebcfgError("Invalid aker request\n");
			WEBCFG_FREE(msg);
			return ret;
		}
		snprintf(source, sizeof(source), "mac:%s/webcfg",get_deviceMAC());
		WebcfgDebug("source: %s\n",source);
		msg->u.crud.source = strdup(source);
		snprintf(destination, sizeof(destination), "mac:%s/aker/schedule",get_deviceMAC());
		WebcfgDebug("destination: %s\n",destination);
		msg->u.crud.dest = strdup(destination);
		snprintf(trans_uuid, sizeof(trans_uuid), "%hu", docTransId);
		msg->u.crud.transaction_uuid = strdup(trans_uuid);
		msg->u.crud.content_type = strdup(CONTENT_TYPE_JSON);

		while(retry_count<=3)
		{
		        backoffRetryTime = (int) pow(2, c) -1;

			sendStatus = libparodus_send(get_webcfg_instance(), msg);
			if(sendStatus == 0)
			{
				WebcfgInfo("Sent blob successfully to parodus\n");
				ret = WDMP_SUCCESS;
				retry_count = 0;
				break;
			}
			else
			{
				WebcfgError("Failed to send blob: '%s', retrying ...\n",libparodus_strerror(sendStatus));
				WebcfgInfo("send_aker_blob backoffRetryTime %d seconds\n", backoffRetryTime);
				if (akerwait__ (backoffRetryTime))
				{
					WebcfgInfo("g_shutdown true, break send_aker_blob failure\n");
					break;
				}
				c++;
				retry_count++;
			}
		}
		free_crud_message(msg);
	}
	return ret;
}

//send aker-status upstream RETRIEVE request to parodus to check aker registration
WEBCFG_STATUS checkAkerStatus()
{
	wrp_msg_t *msg = NULL;
	WEBCFG_STATUS rv = WEBCFG_FAILURE;
	int sendStatus = -1;
	char *status_val = NULL;
	char source[MAX_BUF_SIZE/4] = {'\0'};
	char dest[MAX_BUF_SIZE/4] = {'\0'};
	char *transaction_uuid = NULL;

	msg = (wrp_msg_t *)malloc(sizeof(wrp_msg_t));
	if(msg != NULL)
	{
		memset(msg, 0, sizeof(wrp_msg_t));
		msg->msg_type = WRP_MSG_TYPE__RETREIVE;

		snprintf(source, sizeof(source), "mac:%s/webcfg",get_deviceMAC());
		WebcfgDebug("source: %s\n",source);
		msg->u.crud.source = strdup(source);
		snprintf(dest, sizeof(dest), "mac:%s/parodus/service-status/aker",get_deviceMAC());
		WebcfgDebug("dest: %s\n",dest);
		msg->u.crud.dest = strdup(dest);
		transaction_uuid = generate_trans_uuid();
		if(transaction_uuid !=NULL)
		{
			msg->u.crud.transaction_uuid = transaction_uuid;
			WebcfgInfo("transaction_uuid generated is %s\n", msg->u.crud.transaction_uuid);
		}
		msg->u.crud.content_type = strdup(CONTENT_TYPE_JSON);

		sendStatus = libparodus_send(get_webcfg_instance(), msg);
		if(sendStatus == 0)
		{
			WebcfgInfo("Sent aker retrieve request to parodus\n");
			//waiting to get response from parodus. add lock here while reading
			status_val = get_global_status();
			if(status_val !=NULL)
			{
				if (strcmp(status_val, AKER_STATUS_ONLINE) == 0)
				{
					WebcfgDebug("Received aker status as %s\n", status_val);
					rv = WEBCFG_SUCCESS;
				}
				else
				{
					WebcfgError("Received aker status as %s\n", status_val);
				}
				WEBCFG_FREE(status_val);
			}
			else
			{
				WebcfgError("Failed to get aker status\n");
			}
		}
		else
		{
			WebcfgError("Failed to send aker retrieve req: '%s'\n",libparodus_strerror(sendStatus));
		}

		wrp_free_struct (msg);
	}
	return rv;
}

void updateAkerMaxRetry(webconfig_tmp_data_t *temp, char *docname)
{
	if (NULL != temp)
	{
		WebcfgDebug("updateAkerMaxRetry: temp->name %s, temp->version %lu, temp->retry_count %d\n",temp->name, (long)temp->version, temp->retry_count);
		if( strcmp(docname, temp->name) == 0)
		{
			updateTmpList(temp, temp->name, temp->version, "failed", "aker_service_unavailable", 0, 0, 0);
			addWebConfgNotifyMsg(temp->name, temp->version, "failed", "aker_service_unavailable", temp->cloud_trans_id, 0, "status", 0, NULL, 200);
			return;
		}
	}
	WebcfgError("updateAkerMaxRetry failed for doc %s\n", docname);
}

AKER_STATUS processAkerSubdoc(webconfig_tmp_data_t *docNode, multipartdocs_t *akerIndex)
{
	int i =0;
	AKER_STATUS rv = AKER_FAILURE;
	param_t *reqParam = NULL;
	WDMP_STATUS ret = WDMP_FAILURE;
	int paramCount = 0;
	uint16_t doc_transId = 0;
	webcfgparam_t *pm = NULL;
	multipartdocs_t *gmp = NULL;
	uint16_t err = 0;
	char* result = NULL;
	char* errMsg = NULL;
	char value[MAX_VALUE_LEN]={0};

	gmp = akerIndex;

	if(akerTestFlag == 0)
	{
		gmp = NULL;
		akerTestFlag++;
	}
	if(gmp ==NULL)
	{
		WebcfgError("processAkerSubdoc failed as mp cache is NULL\n");
		err = getStatusErrorCodeAndMessage(AKER_SUBDOC_PROCESSING_FAILED, &result);
		WebcfgDebug("The error_details is %s and err_code is %d\n", result, err);
		if(docNode!=NULL)
		{
			updateTmpList(docNode, "aker", docNode->version, "failed", result, err, 0, 0);
			if(docNode->cloud_trans_id !=NULL)
			{
				addWebConfgNotifyMsg("aker", docNode->version, "failed", result, docNode->cloud_trans_id ,0, "status", err, NULL, 200);
			}
		}
		WEBCFG_FREE(result);

		return AKER_FAILURE;
	}

	if(strcmp(gmp->name_space, "aker") == 0)
	{
		WebcfgDebug("gmp->name_space %s\n", gmp->name_space);
		WebcfgDebug("gmp->etag %lu\n" , (long)gmp->etag);
		WebcfgDebug("gmp->data %s\n" , gmp->data);
		WebcfgDebug("gmp->data_size is %zu\n", gmp->data_size);

		WebcfgDebug("--------------decode root doc-------------\n");
		pm = webcfgparam_convert( gmp->data, gmp->data_size+1 );
		err = errno;
		if ( NULL != pm)
		{
			paramCount = (int)pm->entries_count;

			reqParam = (param_t *) malloc(sizeof(param_t) * paramCount);
			memset(reqParam,0,(sizeof(param_t) * paramCount));

			WebcfgDebug("paramCount is %d\n", paramCount);
			for (i = 0; i < paramCount; i++)
			{
			        if(pm->entries[i].value != NULL)
			        {
					if(pm->entries[i].type == WDMP_BLOB)
					{
						char *appended_doc = NULL;
						appended_doc = webcfg_appendeddoc( gmp->name_space, gmp->etag, pm->entries[i].value, pm->entries[i].value_size, &doc_transId);
						if(appended_doc != NULL)
						{
							WebcfgDebug("webcfg_appendeddoc doc_transId : %hu\n", doc_transId);
							if(pm->entries[i].name !=NULL)
							{
								reqParam[i].name = strdup(pm->entries[i].name);
							}
							WebcfgDebug("appended_doc length: %zu\n", strlen(appended_doc));
							reqParam[i].value = strdup(appended_doc);
							reqParam[i].type = WDMP_BASE64;
							WEBCFG_FREE(appended_doc);
						}
						updateTmpList(docNode, gmp->name_space, gmp->etag, "pending", "none", 0, doc_transId, 0);

						//Start event handler thread to process aker events if it is not started already.
						WebcfgDebug("get_global_eventFlag is %d\n", get_global_eventFlag());
						if(get_global_eventFlag() == 0)
						{
							WebcfgInfo("starting initEventHandlingTask during aker processing\n");
							initEventHandlingTask();
							processWebcfgEvents();
							set_global_eventFlag();
						}
					}
					else
					{
						WebcfgError("blob type is incorrect\n");
						err = getStatusErrorCodeAndMessage(INCORRECT_BLOB_TYPE, &result);
						WebcfgDebug("The error_details is %s and err_code is %d\n", result, err);
						updateTmpList(docNode, gmp->name_space, gmp->etag, "failed", result, err, 0, 0);
						if(docNode != NULL && docNode->cloud_trans_id != NULL )
						{
							addWebConfgNotifyMsg(gmp->name_space, gmp->etag, "failed", result, docNode->cloud_trans_id ,0, "status", err, NULL, 200);
						}
						WEBCFG_FREE(result);

						WEBCFG_FREE(reqParam);
						webcfgparam_destroy( pm );
						return rv;
					}
			        }
				WebcfgInfo("Request:> param[%d].name = %s, type = %d\n",i,reqParam[i].name,reqParam[i].type);
				WebcfgDebug("Request:> param[%d].value = %s\n",i,reqParam[i].value);
			}

			if(reqParam !=NULL && validate_request_param(reqParam, paramCount) == WEBCFG_SUCCESS)
			{
				ret = send_aker_blob(pm->entries[0].name, pm->entries[0].value,pm->entries[0].value_size, doc_transId, (int)gmp->etag);

				if(ret == WDMP_SUCCESS)
				{
					WebcfgDebug("aker doc send success\n");
					set_send_aker_flag(true);
					rv = AKER_SUCCESS;
				}
				else
				{

					err = getStatusErrorCodeAndMessage(AKER_SUBDOC_PROCESSING_FAILED, &result);

					if(docNode != NULL)
					{
						WebcfgInfo("subdoc_name and err_code : %s %lu\n",docNode->name, (long)docNode->error_code);
						WebcfgInfo("failure_reason %s\n", docNode->error_details);

						//Invalid aker request
						updateTmpList(docNode, gmp->name_space, gmp->etag, "failed", "doc_rejected", err, 0, 0);
						if(docNode->cloud_trans_id !=NULL)
						{
							addWebConfgNotifyMsg(gmp->name_space, gmp->etag, "failed", "doc_rejected", docNode->cloud_trans_id, 0, "status", err, NULL, 200);

						}
					}
					WEBCFG_FREE(result);

					rv = AKER_FAILURE;
				}
				reqParam_destroy(paramCount, reqParam);
			}
			webcfgparam_destroy( pm );
		}
		else
		{
			WebcfgError("--------------decode root doc failed-------------\n");
			char * msg = NULL;
			msg = (char *)webcfgparam_strerror(err);
			err = getStatusErrorCodeAndMessage(DECODE_ROOT_FAILURE, &errMsg);
			snprintf(value,MAX_VALUE_LEN,"%s:%s", errMsg, msg);
			updateTmpList(docNode, gmp->name_space, gmp->etag, "failed", value, err, 0, 0);
			if(docNode !=NULL && docNode->cloud_trans_id !=NULL)
			{
				addWebConfgNotifyMsg(gmp->name_space, gmp->etag, "failed", value, docNode->cloud_trans_id,0, "status", err, NULL, 200);
			}
			WEBCFG_FREE(errMsg);
		}
	}
	else
	{
		WebcfgDebug("aker is not found in mp list\n");
	}
	return rv;
}
/*----------------------------------------------------------------------------*/
/*                             Internal functions                             */
/*----------------------------------------------------------------------------*/
//set global aker status and to awake waiting getter threads
static void set_global_status(char *status)
{
	pthread_mutex_lock (&client_mut);
	WebcfgDebug("mutex lock in producer thread\n");
	wakeFlag = 1;
	aker_status = status;
	pthread_cond_signal(&client_con);
	pthread_mutex_unlock (&client_mut);
	WebcfgDebug("mutex unlock in producer thread\n");
}

//Combining getter func with pthread wait.
static char *get_global_status()
{
	char *temp = NULL;
	int  rv;
	struct timespec ts;
	pthread_mutex_lock (&client_mut);
	WebcfgDebug("mutex lock in consumer thread\n");
	WebcfgDebug("Before pthread cond wait in consumer thread\n");

	clock_gettime(CLOCK_REALTIME, &ts);
	ts.tv_sec += WAIT_TIME_IN_SEC;

	while (!wakeFlag)
	{
		if (get_global_shutdown())
		{
			WebcfgDebug("g_shutdown in client consumer thread\n");
			break;
		}
		rv = pthread_cond_timedwait(&client_con, &client_mut, &ts);
		WebcfgDebug("After pthread_cond_timedwait\n");
		if (rv == ETIMEDOUT)
		{
			WebcfgError("Timeout Error. Unable to get service_status even after %d seconds\n", WAIT_TIME_IN_SEC);
			pthread_mutex_unlock(&client_mut);
			return NULL;
		}
	}
	temp = aker_status;
	wakeFlag = 0;
	pthread_mutex_unlock (&client_mut);
	WebcfgDebug("mutex unlock in consumer thread after cond wait\n");
	return temp;
}

static char *decodePayload(char *payload)
{
	msgpack_unpacked result;
	size_t off = 0;
	char *decodedPayload = NULL;
	uint16_t err = 0;
	char* errmsg = NULL;
	msgpack_unpack_return ret;
	msgpack_unpacked_init(&result);
	ret = msgpack_unpack_next(&result, payload, strlen(payload), &off);
	if(ret == MSGPACK_UNPACK_SUCCESS && akerTestFlag >1)
	{
		msgpack_object obj = result.data;
		//msgpack_object_print(stdout, obj);
		//puts("");
		if (obj.type == MSGPACK_OBJECT_MAP) 
		{
			msgpack_object_map *map = &obj.via.map;
			msgpack_object_kv* p = map->ptr;
			msgpack_object *key = &p->key;
			msgpack_object *val = &p->val;
			if (0 == strncmp(key->via.str.ptr, "message", key->via.str.size))
			{
				if(val->via.str.ptr != NULL)
				{
					decodedPayload = strdup(val->via.str.ptr);
				}
			}
		}
	}
	else
	{
		WebcfgError("Failed to decode msgpack data\n");
		webconfig_tmp_data_t * docNode = NULL;
		err = getStatusErrorCodeAndMessage(AKER_RESPONSE_PARSE_FAILURE, &errmsg);
		WebcfgDebug("The error_details is %s and err_code is %d\n", errmsg, err);
		docNode = getTmpNode("aker");
		if(docNode !=NULL)
		{
			updateTmpList(docNode, "aker", docNode->version, "failed", errmsg, err, 0, 0);
			if(docNode->cloud_trans_id !=NULL)
			{
				addWebConfgNotifyMsg("aker", docNode->version, "failed", errmsg, docNode->cloud_trans_id,0, "status", err, NULL, 200);
			}
		}
		WEBCFG_FREE(errmsg);
	}
	akerTestFlag++;
	msgpack_unpacked_destroy(&result);
	return decodedPayload;
}

static char* parsePayloadForAkerStatus(char *payload)
{
	cJSON *json = NULL;
	cJSON *akerStatusObj = NULL;
	char *aker_status_str = NULL;
	char *akerStatus = NULL;

	json = cJSON_Parse( payload );
	if( !json )
	{
		WebcfgError( "json parse error: [%s]\n", cJSON_GetErrorPtr() );
	}
	else
	{
		akerStatusObj = cJSON_GetObjectItem( json, SERVICE_STATUS );
		if( akerStatusObj != NULL)
		{
			aker_status_str = cJSON_GetObjectItem( json, SERVICE_STATUS )->valuestring;
			if ((aker_status_str != NULL) && strlen(aker_status_str) > 0)
			{
				akerStatus = strdup(aker_status_str);
				WebcfgDebug("akerStatus value parsed from payload is %s\n", akerStatus);
			}
			else
			{
				WebcfgError("aker status string is empty\n");
			}
		}
		else
		{
			WebcfgError("Failed to get akerStatus from payload\n");
		}
		cJSON_Delete(json);
	}
	return akerStatus;
}

static void handleAkerStatus(int status, char *payload)
{
	char data[MAX_BUF_SIZE] = {0};
	char *eventData = NULL;
	uint16_t err = 0;
	char* result = NULL;
	webconfig_tmp_data_t * docNode = NULL;
	status = 0;
	switch(status)
	{
		case 201:
		case 200:
			snprintf(data,sizeof(data),"aker,%hu,%u,ACK,%u",akerTransId,akerDocVersion,0);
		break;
		case 534:
		case 535:
			snprintf(data,sizeof(data),"aker,%hu,%u,NACK,%u,aker,%d,%s",akerTransId,akerDocVersion,0,status,payload);
		break;
		default:
			WebcfgError("Invalid status code %d\n",status);
			err = getStatusErrorCodeAndMessage(INVALID_AKER_RESPONSE, &result);
			WebcfgDebug("The error_details is %s and err_code is %d\n", result, err);
			docNode = getTmpNode("aker");
			if(docNode !=NULL)
			{
				updateTmpList(docNode, "aker", docNode->version, "failed", result, err, 0, 0);
				if(docNode->cloud_trans_id !=NULL)
				{
					addWebConfgNotifyMsg("aker", docNode->version, "failed", result, docNode->cloud_trans_id,0, "status", err, NULL, 200);
				}
			}
			WEBCFG_FREE(result);
			return;
	}
	WebcfgDebug("data: %s\n",data);
	eventData = data;
	webcfgCallback(eventData, NULL);
}

static void free_crud_message(wrp_msg_t *msg)
{
	if(msg)
	{
		if(msg->u.crud.source)
		{
			free(msg->u.crud.source);
		}
		if(msg->u.crud.dest)
		{
			free(msg->u.crud.dest);
		}
		if(msg->u.crud.transaction_uuid)
		{
			free(msg->u.crud.transaction_uuid);
		}
		if(msg->u.crud.content_type)
		{
			free(msg->u.crud.content_type);
		}
		free(msg);
	}
}

void processAkerUpdateDelete(wrp_msg_t *wrpMsg)
{
	char *sourceService, *sourceApplication =NULL;
	char *payload = NULL;

	sourceService = wrp_get_msg_element(WRP_ID_ELEMENT__SERVICE, wrpMsg, SOURCE);
	WebcfgDebug("sourceService: %s\n",sourceService);
	sourceApplication = wrp_get_msg_element(WRP_ID_ELEMENT__APPLICATION, wrpMsg, SOURCE);
	WebcfgDebug("sourceApplication: %s\n",sourceApplication);
	if(sourceService != NULL && sourceApplication != NULL && strcmp(sourceService,"aker")== 0 && strcmp(sourceApplication,"schedule")== 0)
	{
		WebcfgInfo("Response received from %s\n",sourceService);
		WEBCFG_FREE(sourceService);
		WEBCFG_FREE(sourceApplication);
		payload = decodePayload(wrpMsg->u.crud.payload);
		if(payload !=NULL)
		{
			WebcfgDebug("payload = %s\n",payload);
			WebcfgDebug("status: %d\n",wrpMsg->u.crud.status);
			handleAkerStatus(wrpMsg->u.crud.status, payload);
			WEBCFG_FREE(payload);
		}
		else
		{
			WebcfgError("decodePayload is NULL\n");
		}
	}
	wrp_free_struct (wrpMsg);

}

void processAkerRetrieve(wrp_msg_t *wrpMsg)
{
	char *sourceService, *sourceApplication =NULL;
	char *status=NULL;

	sourceService = wrp_get_msg_element(WRP_ID_ELEMENT__SERVICE, wrpMsg, SOURCE);
	sourceApplication = wrp_get_msg_element(WRP_ID_ELEMENT__APPLICATION, wrpMsg, SOURCE);
	WebcfgDebug("sourceService %s sourceApplication %s\n", sourceService, sourceApplication);
	if(sourceService != NULL && sourceApplication != NULL && strcmp(sourceService,"parodus")== 0 && strcmp(sourceApplication,"service-status/aker")== 0)
	{
		WebcfgDebug("Retrieve response received from parodus : %s transaction_uuid %s\n",(char *)wrpMsg->u.crud.payload, wrpMsg->u.crud.transaction_uuid );
		status = parsePayloadForAkerStatus(wrpMsg->u.crud.payload);
		if(status !=NULL)
		{
			//set this as global status. add lock before update it.
			set_global_status(status);
			WebcfgDebug("set aker-status value as %s\n", status);
		}
		WEBCFG_FREE(sourceService);
		WEBCFG_FREE(sourceApplication);
	}
	wrp_free_struct (wrpMsg);
}
