/*
 * If not stated otherwise in this file or this component's Licenses.txt file the
 * following copyright and licenses apply:
 *
 * Copyright 2021 RDK Management
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

#ifndef _WEBCFG_RBUS_H_
#define _WEBCFG_RBUS_H_

#include <stdio.h>
#include <stdbool.h>
#include <rbus/rbus.h>
#include <rbus/rbus_object.h>
#include <rbus/rbus_property.h>
#include <rbus/rbus_value.h>

#include <wdmp-c.h>
#include <wrp-c.h>
#include <cJSON.h>
#include "webcfg.h"
#include "webcfg_log.h"
#include "webcfg_generic.h"
#include "webcfg_event.h"

#define buffLen 1024
#define maxParamLen 128

#define NUM_WEBCFG_ELEMENTS1 8

#if !defined (FEATURE_SUPPORT_MQTTCM)
#define NUM_WEBCFG_ELEMENTS2 3
#endif

#define MAX_FORCE_RESET_SET_COUNT 3
#define MAX_FORCE_RESET_TIME_SECS 24*60*60

#define WEBCFG_COMPONENT_NAME "webconfig"
#define MAX_PARAMETERNAME_LEN			4096
#define WEBCFG_EVENT_NAME "webconfigSignal"

// Data elements provided by webconfig
#define WEBCFG_RFC_PARAM "Device.X_RDK_WebConfig.RfcEnable"
#define WEBCFG_URL_PARAM "Device.X_RDK_WebConfig.URL"
#define WEBCFG_FORCESYNC_PARAM "Device.X_RDK_WebConfig.ForceSync"
#define WEBCFG_DATA_PARAM		"Device.X_RDK_WebConfig.Data"
#define WEBCFG_SUPPORTED_DOCS_PARAM	"Device.X_RDK_WebConfig.SupportedDocs"
#define WEBCFG_SUPPORTED_VERSION_PARAM	"Device.X_RDK_WebConfig.SupportedSchemaVersion"
#define WEBCFG_SUPPLEMENTARY_TELEMETRY_PARAM  "Device.X_RDK_WebConfig.SupplementaryServiceUrls.Telemetry"
#define WEBCFG_SUBDOC_FORCERESET_PARAM  "Device.X_RDK_WebConfig.webcfgSubdocForceReset"

#ifdef WAN_FAILOVER_SUPPORTED
#define WEBCFG_INTERFACE_PARAM "Device.X_RDK_WanManager.CurrentActiveInterface"
#endif

#define WEBCFG_UPSTREAM_EVENT  "Webconfig.Upstream"
#define PARAM_RFC_ENABLE "eRT.com.cisco.spvtg.ccsp.webpa.WebConfigRfcEnable"

#define WEBCFG_UTIL_METHOD "Device.X_RDK_WebConfig.FetchCachedBlob"
#define WEBCFG_SCRIPT_EXEC_PARAM "Device.X_RDK_WebConfig.ScriptExec"

#define CCSP_Msg_Bus_OK             100
#define CCSP_Msg_Bus_OOM            101
#define CCSP_Msg_Bus_ERROR          102

#define CCSP_Msg_BUS_CANNOT_CONNECT 190
#define CCSP_Msg_BUS_TIMEOUT        191
#define CCSP_Msg_BUS_NOT_EXIST      192
#define CCSP_Msg_BUS_NOT_SUPPORT    193
#define CCSP_ERR_UNSUPPORTED_PROTOCOL 9013
#define CCSP_ERR_INVALID_PARAMETER_VALUE 9007
#define CCSP_ERR_UNSUPPORTED_NAMESPACE 204

typedef enum _webcfgError
{
	ERROR_SUCCESS = 0,

	ERROR_FAILURE = 1,

	ERROR_INVALID_INPUT,

	ERROR_ELEMENT_DOES_NOT_EXIST

}webcfgError_t;

typedef struct ForceSyncMsg {
	char *ForceSyncVal;
	char *ForceSyncTransID;
    struct ForceSyncMsg* next;
} ForceSyncMsg;

bool isRbusEnabled();

bool isRfcEnabled();

WEBCFG_STATUS webconfigRbusInit(const char *pComponentName);
WEBCFG_STATUS regWebConfigDataModel();

rbusHandle_t get_global_rbus_handle(void);

void blobSet_rbus(char *name, void *bufVal, int len, WDMP_STATUS *retStatus, int *ccspRetStatus);

void getValues_rbus(const char *paramName[], const unsigned int paramCount, int index, money_trace_spans *timeSpan, param_t ***paramArr, int *retValCount, int *retStatus);
void setValues_rbus(const param_t paramVal[], const unsigned int paramCount, const int setType,char *transactionId, money_trace_spans *timeSpan, WDMP_STATUS *retStatus, int *ccspRetStatus);

DATA_TYPE mapRbusToWdmpDataType(rbusValueType_t rbusType);
int mapRbusStatus(int Rbus_error_code);
rbusError_t registerRBUSEventElement();
rbusError_t removeRBUSEventElement();
rbusError_t rbusWebcfgEventHandler(rbusHandle_t handle, rbusProperty_t prop, rbusSetHandlerOptions_t* opts);
int set_rbus_RfcEnable(bool bValue);
int set_rbus_ForceSync(char* pString, int *pStatus);
void set_global_webconfig_url(char *value);
void set_global_supplementary_url(char *value);
int parseForceSyncJson(char *jsonpayload, char **forceSyncVal, char **forceSynctransID);
int get_rbus_ForceSync(char** pString, char **transactionId );
bool get_rbus_RfcEnable();
void sendNotification_rbus(char *payload, char *source, char *destination);
void waitForUpstreamEventSubscribe(int wait_time);
void trigger_webcfg_forcedsync();
void registerRbusLogger();
webcfgError_t fetchMpBlobData(char *docname, void **blobdata, int *len, uint32_t *etag);
bool isRbusInitialized();
void webpaRbus_Uninit();
rbusError_t publishSubdocResetEvent(char *subdocName);
bool get_global_isRbus(void);
char * webcfgError_ToString(webcfgError_t e);
rbusValueType_t mapWdmpToRbusDataType(DATA_TYPE wdmpType);
int mapRbusToCcspStatus(int Rbus_error_code);
rbusError_t eventSubHandler(rbusHandle_t handle, rbusEventSubAction_t action, const char* eventName, rbusFilter_t filter, int32_t interval, bool* autoPublish);
rbusError_t resetEventSubHandler(rbusHandle_t handle, rbusEventSubAction_t action, const char* eventName, rbusFilter_t filter, int32_t interval, bool* autoPublish);
void rbus_log_handler(rbusLogLevel level, const char* file, int line, int threadId, char* message);
webcfgError_t checkSubdocInDb(char *docname);
webcfgError_t resetSubdocVersion(char *docname);
#ifdef WAN_FAILOVER_SUPPORTED
int subscribeTo_CurrentActiveInterface_Event();
#endif
int addForceSyncMsgToQueue(char *ForceSync, char *ForceSyncTransID);
int updateForceSyncMsgQueue(char* trans_id);
void deleteForceSyncMsgQueue();
void DisplayQueue();
ForceSyncMsg* getForceSyncMsgQueue();
#endif
