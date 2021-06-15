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
#include <wrp-c.h>
#include <wdmp-c.h>
#include <msgpack.h>
#include <libparodus.h>
/*----------------------------------------------------------------------------*/
/*                                   Macros                                   */
/*----------------------------------------------------------------------------*/
#define PARODUS_URL_DEFAULT     "tcp://127.0.0.1:6666"
#define CLIENT_URL_DEFAULT      "tcp://127.0.0.1:6659"
/*----------------------------------------------------------------------------*/
/*                               Data Structures                              */
/*----------------------------------------------------------------------------*/
/* none */

/*----------------------------------------------------------------------------*/
/*                            File Scoped Variables                           */
/*----------------------------------------------------------------------------*/
libpd_instance_t webcfg_instance;
static pthread_t clientthreadId=0;
/*----------------------------------------------------------------------------*/
/*                             Function Prototypes                            */
/*----------------------------------------------------------------------------*/
static void webConfigClientReceive();
static void connect_parodus();
static void *parodus_receive();
static void get_parodus_url(char **parodus_url, char **client_url);
/*----------------------------------------------------------------------------*/
/*                             External Functions                             */
/*----------------------------------------------------------------------------*/
void initWebConfigClient()
{
	connect_parodus();
	webConfigClientReceive();
}

libpd_instance_t get_webcfg_instance(void)
{
    return webcfg_instance;
}

pthread_t get_global_client_threadid()
{
    return clientthreadId;
}

/*----------------------------------------------------------------------------*/
/*                             Internal functions                             */
/*----------------------------------------------------------------------------*/
static void webConfigClientReceive()
{
	int err = 0;

	err = pthread_create(&clientthreadId, NULL, parodus_receive, NULL);
	if (err != 0) 
	{
		WebcfgError("Error creating webConfigClientReceive thread :[%s]\n", strerror(err));
	}
	else
	{
		WebcfgInfo("webConfigClientReceive Thread created Successfully.\n");
	}
}

static void connect_parodus()
{
	int backoffRetryTime = 0;
	int backoff_max_time = 5;
	int max_retry_sleep;
	//Retry Backoff count shall start at c=2 & calculate 2^c - 1.
	int c =2;
	int retval=-1;
	char *parodus_url = NULL;
	char *client_url = NULL;

	max_retry_sleep = (int) pow(2, backoff_max_time) -1;
	WebcfgInfo("max_retry_sleep is %d\n", max_retry_sleep );

	get_parodus_url(&parodus_url, &client_url);

	if(parodus_url != NULL && client_url != NULL)
	{	
		libpd_cfg_t cfg1 = {.service_name = "webcfg",
							.receive = true, .keepalive_timeout_secs = 64,
							.parodus_url = parodus_url,
							.client_url = client_url
						   };

		WebcfgInfo("libparodus_init with parodus url %s and client url %s\n",cfg1.parodus_url,cfg1.client_url);

		while(1)
		{
			if(backoffRetryTime < max_retry_sleep)
			{
				backoffRetryTime = (int) pow(2, c) -1;
			}
			WebcfgInfo("New backoffRetryTime value calculated as %d seconds\n", backoffRetryTime);
			int ret =libparodus_init (&webcfg_instance, &cfg1);
			WebcfgDebug("ret is %d\n",ret);
			if(ret ==0)
			{
				WebcfgInfo("Init for parodus Success..!!\n");
				break;
			}
			else
			{
				WebcfgError("Init for parodus failed: '%s'\n",libparodus_strerror(ret));
				if (akerwait__ (backoffRetryTime))
				{
					WebcfgInfo("g_shutdown true, break connect_parodus wait\n");
					break;
				}
				c++;

				if(backoffRetryTime == max_retry_sleep)
				{
					c = 2;
					backoffRetryTime = 0;
					WebcfgInfo("backoffRetryTime reached max value, reseting to initial value\n");
				}
			}
			retval = libparodus_shutdown(&webcfg_instance);
			WebcfgInfo("libparodus_shutdown retval %d\n", retval);
		}
	}
}

void* parodus_receive()
{
	int rtn;
	wrp_msg_t *wrp_msg = NULL;
	uint16_t err = 0;
	char * errmsg = NULL;
	static int sleep_counter = 0;

	while(1)
	{

		if (get_global_shutdown())
		{
			WebcfgDebug("g_shutdown true, break libparodus_receive\n");
			sleep_counter = 0;
			break;
		}
		rtn = libparodus_receive (webcfg_instance, &wrp_msg, 2000);

		if(get_send_aker_flag())
		{
			rtn = 2;
		}

		if (rtn == 1)
		{
			sleep_counter = 0;
			continue;
		}

		if (rtn != 0)
		{
			WebcfgError ("Libparodus failed to receive message: '%s'\n",libparodus_strerror(rtn));
			sleep(5);
			sleep_counter++;
			//waiting 70s (14*5sec) to get response from aker and then to send receive failure notification
			if(get_send_aker_flag() && (sleep_counter == 14))
			{
				webconfig_tmp_data_t * docNode = NULL;
				err = getStatusErrorCodeAndMessage(LIBPARODUS_RECEIVE_FAILURE, &errmsg);
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
				sleep_counter = 0;
				set_send_aker_flag(false);
			}
			continue;
		}

		if(wrp_msg != NULL)
		{
			set_send_aker_flag(false);
			sleep_counter = 0;
			WebcfgDebug("Message received with type %d\n",wrp_msg->msg_type);
			WebcfgDebug("transaction_uuid %s\n", wrp_msg->u.crud.transaction_uuid );
			if ((wrp_msg->msg_type == WRP_MSG_TYPE__UPDATE) ||(wrp_msg->msg_type == WRP_MSG_TYPE__DELETE))
			{
				processAkerUpdateDelete(wrp_msg);
			}
			else if (wrp_msg->msg_type == WRP_MSG_TYPE__RETREIVE)
			{
				processAkerRetrieve(wrp_msg);
			}
		}
	}
	libparodus_shutdown(&webcfg_instance);
	return 0;
}

static void get_parodus_url(char **parodus_url, char **client_url)
{
	FILE *fp = fopen(DEVICE_PROPS_FILE, "r");
	if (NULL != fp)
	{
		char str[255] = {'\0'};
		//TODO:: Use Fgets to fix coverity issue "fscanf" assumes an arbitrarily long string.
		while(fscanf(fp,"%s", str) != EOF)
		{
			char *value = NULL;
			if(NULL != (value = strstr(str, "PARODUS_URL=")))
			{
				value = value + strlen("PARODUS_URL=");
				*parodus_url = strdup(value);
			}

			if(NULL != (value = strstr(str, "WEBCFG_CLIENT_URL=")))
			{
				value = value + strlen("WEBCFG_CLIENT_URL=");
				*client_url = strdup(value);
			}
		}
		fclose(fp);
	}
	else
	{
		WebcfgError("Failed to open device.properties file:%s\n", DEVICE_PROPS_FILE);
		WebcfgInfo("Adding default values for parodus_url and client_url\n");
		*parodus_url = strdup(PARODUS_URL_DEFAULT);
		*client_url = strdup(CLIENT_URL_DEFAULT);
	}

	if (NULL == *parodus_url)
	{
		WebcfgError("parodus_url is not present in device.properties, adding default parodus_url\n");
		*parodus_url = strdup(PARODUS_URL_DEFAULT);
	}
	else
	{
		WebcfgInfo("parodus_url formed is %s\n", *parodus_url);
	}

	if (NULL == *client_url)
	{
		WebcfgError("client_url is not present in device.properties, adding default client_url\n");
		*client_url = strdup(CLIENT_URL_DEFAULT);
	}
	else
	{
		WebcfgInfo("client_url formed is %s\n", *client_url);
	}
}

