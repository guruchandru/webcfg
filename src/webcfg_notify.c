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

#include "webcfg_multipart.h"
#include "webcfg_notify.h"
#include "webcfg_generic.h"
#include "webcfg.h"
#include <pthread.h>
#include <cJSON.h>
#include <unistd.h>
/*----------------------------------------------------------------------------*/
/*                                   Macros                                   */
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
/*                               Data Structures                              */
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
/*                            File Scoped Variables                           */
/*----------------------------------------------------------------------------*/
static pthread_t NotificationThreadId=0;
pthread_mutex_t notify_mut=PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t notify_con=PTHREAD_COND_INITIALIZER;
notify_params_t *notifyMsgQ = NULL;
/*----------------------------------------------------------------------------*/
/*                             Function Prototypes                            */
/*----------------------------------------------------------------------------*/
void* processWebConfgNotification();
void free_notify_params_struct(notify_params_t *param);
/*----------------------------------------------------------------------------*/
/*                             External Functions                             */
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
/*                             External Functions                             */
/*----------------------------------------------------------------------------*/
pthread_t get_global_notify_threadid()
{
    return NotificationThreadId;
}

pthread_cond_t *get_global_notify_con(void)
{
    return &notify_con;
}

pthread_mutex_t *get_global_notify_mut(void)
{
    return &notify_mut;
}

//To handle webconfig notification tasks
void initWebConfigNotifyTask()
{
	int err = 0;
	err = pthread_create(&NotificationThreadId, NULL, processWebConfgNotification, NULL);
	if (err != 0)
	{
		WebcfgError("Error creating Webconfig Notification thread :[%s]\n", strerror(err));
	}
	else
	{
		WebcfgInfo("Webconfig Notification thread created Successfully\n");
	}

}

void addWebConfgNotifyMsg(char *docname, uint32_t version, char *status, char *error_details, char *transaction_uuid, uint32_t timeout, char *type, uint16_t error_code, char *root_string, long response_code)
{
	WebcfgInfo("Inside addWebConfgNotifyMsg\n");
	notify_params_t *args = NULL;

	args = (notify_params_t *)malloc(sizeof(notify_params_t));
	char versionStr[32] = {'\0'};

	if(args)
	{
		memset(args, 0, sizeof(notify_params_t));
		if(docname != NULL)
		{
			args->name = strdup(docname);
		}

		if(status != NULL)
		{
			args->application_status = strdup(status);
		}

		args->timeout = timeout;

		if(error_details != NULL)
		{
			args->error_details = strdup(error_details);
		}

		if(version ==0 && root_string !=NULL)
		{
			snprintf(versionStr, sizeof(versionStr), "%s", root_string);
		}
		else
		{
			snprintf(versionStr, sizeof(versionStr), "%lu", (long)version);
		}

		if(strlen(versionStr) > 0)
		{
			args->version = strdup(versionStr);
		}

		if(transaction_uuid != NULL)
		{
			args->transaction_uuid = strdup(transaction_uuid);
		}

		if(type != NULL)
		{
			args->type = strdup(type);
		}

		args->error_code = error_code;

		args->response_code = response_code;

		WebcfgInfo("args->name:%s,args->application_status:%s,args->timeout:%lu,args->error_details:%s,args->version:%s,args->transaction_uuid:%s,args->type:%s,args->error_code:%lu,args->response_code:%lu\n",args->name,args->application_status, (long)args->timeout, args->error_details, args->version, args->transaction_uuid, args->type, (long)args->error_code,args->response_code );

		args->next=NULL;

		pthread_mutex_lock (&notify_mut);
		//Producer adds the notifyMsg into queue
		if(notifyMsgQ == NULL)
		{
			notifyMsgQ = args;
			WebcfgInfo("Producer added notify message\n");
			pthread_cond_signal(&notify_con);
			pthread_mutex_unlock (&notify_mut);
			WebcfgInfo("mutex unlock in notify producer thread\n");
		}
		else
		{
			notify_params_t *temp = notifyMsgQ;
			while(temp->next)
			{
			    temp = temp->next;
			}
			temp->next = args;
			pthread_mutex_unlock (&notify_mut);
		}
	}
	else
	{
	    WebcfgError("failure in allocation for notify message\n");
	}
}

//Notify thread function waiting for notify msgs
void* processWebConfgNotification()
{
	char device_id[32] = { '\0' };
	cJSON *notifyPayload = NULL;
	char  * stringifiedNotifyPayload = NULL;
	char dest[512] = {'\0'};
	char *source = NULL;

	WebcfgInfo("Inside processWebConfgNotification\n");
	while(1)
	{
		pthread_mutex_lock (&notify_mut);
		WebcfgInfo("mutex lock in notify consumer thread\n");
		if(notifyMsgQ != NULL)
		{
			notify_params_t *msg = notifyMsgQ;
			notifyMsgQ = notifyMsgQ->next;
			pthread_mutex_unlock (&notify_mut);
			WebcfgInfo("mutex unlock in notify consumer thread\n");

			if((get_deviceMAC() !=NULL) && (strlen(get_deviceMAC()) == 0))
			{
				WebcfgError("deviceMAC is NULL, failed to send Webconfig Notification\n");
			}
			else
			{
				snprintf(device_id, sizeof(device_id), "mac:%s", get_deviceMAC());
				WebcfgInfo("webconfig Device_id %s\n", device_id);

				notifyPayload = cJSON_CreateObject();

				if(notifyPayload != NULL)
				{
					cJSON_AddStringToObject(notifyPayload,"device_id", device_id);

					if(msg)
					{
						if(msg->name !=NULL)
						{
							cJSON_AddStringToObject(notifyPayload,"namespace", (NULL != msg->name && (strlen(msg->name)!=0)) ? msg->name : "unknown");
						}
						if(msg->application_status !=NULL)
						{
							cJSON_AddStringToObject(notifyPayload,"application_status", msg->application_status);
						}
						WebcfgDebug("msg->timeout is %lu\n", (long)msg->timeout);
						if(msg->timeout !=0)
						{
							cJSON_AddNumberToObject(notifyPayload,"timeout", msg->timeout);
						}
						WebcfgDebug("msg->error_code is %lu\n", (long)msg->error_code);
						if(msg->error_code !=0)
						{
							cJSON_AddNumberToObject(notifyPayload,"error_code", msg->error_code);
						}
						if((msg->error_details !=NULL) && (strcmp(msg->error_details, "none")!=0))
						{
							cJSON_AddStringToObject(notifyPayload,"error_details", (NULL != msg->error_details) ? msg->error_details : "unknown");
						}
						if( msg->response_code != 200 )
						{
							cJSON_AddNumberToObject(notifyPayload,"http_status_code", msg->response_code);
						}
						cJSON_AddStringToObject(notifyPayload,"transaction_uuid", (NULL != msg->transaction_uuid && (strlen(msg->transaction_uuid)!=0)) ? msg->transaction_uuid : "unknown");
						cJSON_AddStringToObject(notifyPayload,"version", (NULL != msg->version && (strlen(msg->version)!=0)) ? msg->version : "0");
					
					}
					stringifiedNotifyPayload = cJSON_PrintUnformatted(notifyPayload);
					cJSON_Delete(notifyPayload);
				}

				if(msg->response_code == 200)
				{
					snprintf(dest,sizeof(dest),"event:subdoc-report/%s/%s/%s",msg->name,device_id,msg->type);
				}
				else
				{
					snprintf(dest,sizeof(dest),"event:rootdoc-report/%s/%s",device_id,msg->type);
				}
				WebcfgInfo("dest is %s\n", dest);

				WebcfgInfo("After dest value print\n");

				if (stringifiedNotifyPayload != NULL && strlen(device_id) != 0)
				{
					source = (char*) malloc(sizeof(char) * sizeof(device_id));
					strncpy(source, device_id, sizeof(device_id));
					WebcfgInfo("source is %s\n", source);
					WebcfgInfo("stringifiedNotifyPayload is %s\n", stringifiedNotifyPayload);
					WebcfgInfo("Before sendNotification\n");
					//stringifiedNotifyPayload, source to be freed by sendNotification
					sendNotification(stringifiedNotifyPayload, source, dest);
					WebcfgInfo("After sendNotification\n");
				}
					free_notify_params_struct(msg);
					msg = NULL;
			}			
		}
		else
		{
			if (get_global_shutdown())
			{
				WebcfgInfo("g_shutdown in notify consumer thread\n");
				pthread_mutex_unlock (&notify_mut);
				break;
			}
			WebcfgInfo("Before pthread cond wait in notify thread\n");
			pthread_cond_wait(&notify_con, &notify_mut);
			pthread_mutex_unlock (&notify_mut);
			WebcfgInfo("mutex unlock in notify thread after cond wait\n");
		}
	}
	return NULL;
}

void free_notify_params_struct(notify_params_t *param)
{
    if(param != NULL)
    {
        if(param->name != NULL)
        {
            WEBCFG_FREE(param->name);
        }
        if(param->application_status != NULL)
        {
            WEBCFG_FREE(param->application_status);
        }
	if(param->error_details != NULL)
        {
            WEBCFG_FREE(param->error_details);
        }
        if(param->version != NULL)
        {
            WEBCFG_FREE(param->version);
        }
	if(param->transaction_uuid != NULL)
        {
	    WEBCFG_FREE(param->transaction_uuid);
        }
	if(param->type != NULL)
        {
            WEBCFG_FREE(param->type);
        }
        WEBCFG_FREE(param);
    }
}

uint16_t getStatusErrorCodeAndMessage(WEBCFG_ERROR_CODE status, char** result)
{
	uint16_t ret = 0;

	if (status == DECODE_ROOT_FAILURE)
	{
		*result = strdup("decode_root_failure");
		ret = 111;
	}
	else if (status == INCORRECT_BLOB_TYPE)
	{
		*result = strdup("incorrect_blob_type");
		ret = 211;
	}
	else if (status == BLOB_PARAM_VALIDATION_FAILURE) 
	{
		*result = strdup("blob_param_validation_failure");
		ret = 211;
	}
	else if (status == WEBCONFIG_DATA_EMPTY) 
	{
		*result = strdup("webconfig_data_empty");
		ret = 211;
	}
	else if (status == MULTIPART_BOUNDARY_NULL) 
	{
		*result = strdup("multipart_boundary_NULL");
		ret = 211;
	}
	else if (status == INVALID_CONTENT_TYPE)
	{
		*result = strdup("invalid_content_type");
		ret = 211;
	}
	else if (status == ADD_TO_CACHE_LIST_FAILURE)
	{
		*result = strdup("add_to_cache_list_failure");
		ret = 311;
	}
	else if (status == FAILED_TO_SET_BLOB)
	{
		*result = strdup("failed_to_set_blob");
		ret = 311;
	}
	else if (status == MULTIPART_CACHE_NULL)
	{
		*result = strdup("multipart_cache_NULL");
		ret = 311;
	}
	else if (status == AKER_SUBDOC_PROCESSING_FAILED)
	{
		*result = strdup("aker_subdoc_processing_failed");
		ret = 411;
	}
	else if (status == AKER_RESPONSE_PARSE_FAILURE)
	{
		*result = strdup("aker_response_parse_failure");
		ret = 411;
	}
	else if (status == INVALID_AKER_RESPONSE)
	{
		*result = strdup("invalid_aker_response");
		ret = 411;
	}
	else if (status == LIBPARODUS_RECEIVE_FAILURE)
	{
		*result = strdup("libparodus_receive_failure");
		ret = 411;
	}
	else if (status == COMPONENT_EVENT_PARSE_FAILURE)
	{
		*result = strdup("component_event_parse_failure");
		ret = 511;
	}
	else if (status == SUBDOC_RETRY_FAILED)
	{
		*result = strdup("subdoc_retry_failed");
		ret = 611;
	}
	else
	{
		WebcfgError("Error detected is unknown\n");
		*result = strdup("Unknown Error");
	}

	return ret;
}
