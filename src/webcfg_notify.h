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
#ifndef WEBCFGNOTIFY_H
#define WEBCFGNOTIFY_H

#include <stdint.h>
#include "cJSON.h"
#include "webcfg_log.h"
#include "webcfg.h"
#include "webcfg_db.h"

typedef enum
{
	DECODE_ROOT_FAILURE,
	INCORRECT_BLOB_TYPE,
	BLOB_PARAM_VALIDATION_FAILURE,
	WEBCONFIG_DATA_EMPTY,
	MULTIPART_BOUNDARY_NULL,
	INVALID_CONTENT_TYPE,
	ADD_TO_CACHE_LIST_FAILURE,
	FAILED_TO_SET_BLOB,
	MULTIPART_CACHE_NULL,
	AKER_SUBDOC_PROCESSING_FAILED,
	AKER_RESPONSE_PARSE_FAILURE,
	INVALID_AKER_RESPONSE,
	LIBPARODUS_RECEIVE_FAILURE,
	COMPONENT_EVENT_PARSE_FAILURE,
	SUBDOC_RETRY_FAILED
} WEBCFG_ERROR_CODE;

typedef struct _notify_params
{
	char * name;
	char * application_status;
	char * version;
	char * error_details;
	char * transaction_uuid;
	char * type;
	uint32_t timeout;
	uint16_t error_code;
	long response_code;
	struct _notify_params *next;
} notify_params_t;

void initWebConfigNotifyTask();
pthread_t get_global_notify_threadid();
void addWebConfgNotifyMsg(char *docname, uint32_t version, char *status, char *error_details, char *transaction_uuid, uint32_t timeout,char* type, uint16_t error_code, char *root_string, long response_code);
pthread_cond_t *get_global_notify_con(void);
pthread_mutex_t *get_global_notify_mut(void);
uint16_t getStatusErrorCodeAndMessage(WEBCFG_ERROR_CODE status, char** result);
#endif
