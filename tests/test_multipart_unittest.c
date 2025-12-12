 /**
  * Copyright 2019 Comcast Cable Communications Management, LLC
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  *
 */


#include <stdio.h>
#include <stdint.h>
#include <errno.h>
#include <string.h>
#include <msgpack.h>
#include <curl/curl.h>
#include <base64.h>
#include <uuid/uuid.h>
#include <pthread.h>
#include <math.h>
#include <unistd.h>
#include <CUnit/Basic.h>
#include <wdmp-c.h>
#include <cmocka.h>

#include "../src/webcfg_log.h"
#include "../src/webcfg_param.h"
#include "../src/webcfg.h"
#include "../src/webcfg_multipart.h"
#include "../src/webcfg_helpers.h"
#include "../src/webcfg_db.h"
#include "../src/webcfg_notify.h"
#include "../src/webcfg_metadata.h"
#include "../src/webcfg_generic.h"
#include "../src/webcfg_event.h"
#include "../src/webcfg_auth.h"
#include "../src/webcfg_blob.h"
#ifdef WEBCONFIG_BIN_SUPPORT	
#include "../src/webcfg_rbus.h"
	
rbusHandle_t handle;
#endif

#ifdef FEATURE_SUPPORT_AKER
#include "webcfg_aker.h"
#endif

#define WEBCFG_BLOB_PARAM "Device.DeviceInfo.Test"

#define NUM_SUBDOCS 512
#define SUBDOC_SIZE 16

int numLoops;

#define MAX_HEADER_LEN	4096
#define UNUSED(x) (void )(x)

struct mock_token_data {
    size_t size;
    char* data;
};

struct mock_token_data mock_data;

char *mock_device_mac = "b42xxxxxxxxx";

char* get_deviceMAC()
{
	return mock_device_mac;
}

char * getRebootReason()
{
	char *reason = strdup("factory-reset");
	return reason;
}

char *get_global_systemReadyTime()
{
	char *sTime = strdup("158000123");
	return sTime;
}
char * getDeviceBootTime()
{
	char *bTime = strdup("152200345");
	return bTime;
}
char * getFirmwareVersion()
{
	char *fName = strdup("Firmware.bin");
	return fName;
}

char * getProductClass()
{
	char *pClass = strdup("Product");
	return pClass;
}

char * getModelName()
{
	char *mName = strdup("Model");
	return mName;
}

#ifdef WEBCONFIG_BIN_SUPPORT	
int getEncodedBlob(char *data, char **encodedData);
static void packJsonString( cJSON *item, msgpack_packer *pk );
static void packJsonNumber( cJSON *item, msgpack_packer *pk );
static void packJsonArray( cJSON *item, msgpack_packer *pk, int isBlob );
static void packJsonObject( cJSON *item, msgpack_packer *pk, int isBlob);
static void __msgpack_pack_string( msgpack_packer *pk, const void *string, size_t n );
static int convertJsonToMsgPack(char *data, char **encodedData, int isBlob);

static void packBlobData(cJSON *item, msgpack_packer *pk )
{
	char *blobData = NULL, *encodedBlob = NULL;
	int len = 0;
	WebcfgDebug("------ %s ------\n",__FUNCTION__);
	blobData = strdup(item->valuestring);
	if(strlen(blobData) > 0)
	{
		WebcfgDebug("%s\n",blobData);
		len = getEncodedBlob(blobData, &encodedBlob);
		WebcfgDebug("%s\n",encodedBlob);
	}
	__msgpack_pack_string(pk, item->string, strlen(item->string));
	__msgpack_pack_string(pk, encodedBlob, len);
	free(encodedBlob);
	free(blobData);
	WebcfgDebug("------ %s ------\n",__FUNCTION__);
}

static int getItemsCount(cJSON *object)
{
	int count = 0;
	while(object != NULL)
	{
		object = object->next;
		count++;
	}
	return count;
}

static void packJsonNumber( cJSON *item, msgpack_packer *pk )
{
	WebcfgDebug("%s:%d\n",__FUNCTION__,item->valueint);
	if(item->string != NULL)
	{
		__msgpack_pack_string(pk, item->string, strlen(item->string));
	}
	msgpack_pack_int(pk, item->valueint);
}
static void packJsonArray(cJSON *item, msgpack_packer *pk, int isBlob)
{
	int arraySize = cJSON_GetArraySize(item);
	WebcfgDebug("%s:%s\n",__FUNCTION__, item->string);
	if(item->string != NULL)
	{
		//printf("packing %s\n",item->string);
		__msgpack_pack_string(pk, item->string, strlen(item->string));
	}
	msgpack_pack_array( pk, arraySize );
	int i=0;
	for(i=0; i<arraySize; i++)
	{
		cJSON *arrItem = cJSON_GetArrayItem(item, i);
		switch((arrItem->type) & 0XFF)
		{
			case cJSON_Object:
				packJsonObject(arrItem, pk, isBlob);
				break;
		}
	}
}

static void packJsonString( cJSON *item, msgpack_packer *pk )
{
	if(item->string != NULL)
	{
		__msgpack_pack_string(pk, item->string, strlen(item->string));
	}
	__msgpack_pack_string(pk, item->valuestring, strlen(item->valuestring));
}

static void __msgpack_pack_string( msgpack_packer *pk, const void *string, size_t n )
{
	WebcfgDebug("%s:%s\n",__FUNCTION__,(char *)string);
    msgpack_pack_str( pk, n );
    msgpack_pack_str_body( pk, string, n );
}

static void packJsonObject( cJSON *item, msgpack_packer *pk, int isBlob )
{
	WebcfgDebug("%s\n",__FUNCTION__);
	cJSON *child = item->child;
	msgpack_pack_map( pk, getItemsCount(child));
	while(child != NULL)
	{
		switch((child->type) & 0XFF)
		{
			case cJSON_String:
				if(child->string != NULL && (strcmp(child->string, "value") == 0) && isBlob == 1)
				{
					packBlobData(child, pk);
				}
				else
				{
					packJsonString(child, pk);
				}
				break;
			case cJSON_Number:
				packJsonNumber(child, pk);
				break;
			case cJSON_Array:
				packJsonArray(child, pk, isBlob);
				break;
		}
		child = child->next;
	}
}

int getEncodedBlob(char *data, char **encodedData)
{
	cJSON *jsonData=NULL;
	int encodedDataLen = 0;
	WebcfgDebug("------- %s -------\n",__FUNCTION__);
	jsonData=cJSON_Parse(data);
	if(jsonData != NULL)
	{
		msgpack_sbuffer sbuf1;
		msgpack_packer pk1;
		msgpack_sbuffer_init( &sbuf1 );
		msgpack_packer_init( &pk1, &sbuf1, msgpack_sbuffer_write );
		packJsonObject(jsonData, &pk1, 1);
		if( sbuf1.data )
		{
		    *encodedData = ( char * ) malloc( sizeof( char ) * sbuf1.size );
		    if( NULL != *encodedData )
			{
		        memcpy( *encodedData, sbuf1.data, sbuf1.size );
			}
			encodedDataLen = sbuf1.size;
		}
		msgpack_sbuffer_destroy(&sbuf1);
		cJSON_Delete(jsonData);
	}
	WebcfgDebug("------- %s -------\n",__FUNCTION__);
	return encodedDataLen;
}

static int convertJsonToMsgPack(char *data, char **encodedData, int isBlob)
{
	cJSON *jsonData=NULL;
	int encodedDataLen = 0;
	jsonData=cJSON_Parse(data);
	if(jsonData != NULL)
	{
		msgpack_sbuffer sbuf;
		msgpack_packer pk;
		msgpack_sbuffer_init( &sbuf );
		msgpack_packer_init( &pk, &sbuf, msgpack_sbuffer_write );
		packJsonObject(jsonData, &pk, isBlob);
		if( sbuf.data )
		{
		    *encodedData = ( char * ) malloc( sizeof( char ) * sbuf.size );
		    if( NULL != *encodedData )
			{
		        memcpy( *encodedData, sbuf.data, sbuf.size );
			}
			encodedDataLen = sbuf.size;
		}
		msgpack_sbuffer_destroy(&sbuf);
		cJSON_Delete(jsonData);
	}
	return encodedDataLen;
}

rbusError_t webcfgBlobElement1SetHandler(rbusHandle_t handle, rbusProperty_t prop, rbusSetHandlerOptions_t* opts) 
{
		(void) handle;
    	(void) opts;
    	char const* paramName = rbusProperty_GetName(prop);
    	WebcfgInfo("Parameter name is %s \n", paramName);
    	rbusValueType_t type_t;
    	rbusValue_t paramValue_t = rbusProperty_GetValue(prop);
    	if(paramValue_t) {
        	type_t = rbusValue_GetType(paramValue_t);
    	} 
		else {
			WebcfgError("Invalid input to set\n");
        	return RBUS_ERROR_INVALID_INPUT;
    	}

    	if(strncmp(paramName, WEBCFG_BLOB_PARAM, maxParamLen) == 0)
		{
			if(type_t == RBUS_STRING)
			{
				WebcfgDebug("The value is string\n");
			}
			else{
				WebcfgDebug("Value not string\n");
			}
			
		}
		else {
		WebcfgError("Unexpected parameter = %s\n", paramName);
		return RBUS_ERROR_ELEMENT_DOES_NOT_EXIST;
	}   
     	return RBUS_ERROR_SUCCESS;
}
#endif

void populate_db_with_subdocs()
{
    webconfig_db_data_t *head = NULL;
    webconfig_db_data_t *current = NULL;

    for (int i = 0; i < NUM_SUBDOCS; i++) {
        webconfig_db_data_t *new_node = (webconfig_db_data_t *)malloc(sizeof(webconfig_db_data_t));
        if (!new_node) {
            perror("malloc failed");
            return;
        }

        memset(new_node, 0, sizeof(webconfig_db_data_t));

        new_node->name = (char *)malloc(SUBDOC_SIZE);
        if (!new_node->name) {
            perror("malloc failed");
            free(new_node);
            return;
        }

        if (i == 0) {
            snprintf(new_node->name, SUBDOC_SIZE - 1, "root");
        } else {
            snprintf(new_node->name, SUBDOC_SIZE - 1, "subdoc_%d", i);
        }
        new_node->version = i;
        new_node->next = NULL;

        if (head == NULL) {
            head = new_node;
            current = head;
        } else {
            current->next = new_node;
            current = current->next;
        }
    }

    set_global_db_node(head);
}

void test_generate_trans_uuid(){

	char *transaction_uuid = NULL;
	transaction_uuid = generate_trans_uuid();
	CU_ASSERT_FATAL( NULL != transaction_uuid);
}


void test_replaceMac(){
	
	char *configURL= "https://config/{sss}/com";
	char c[] = "{sss}";
	char *webConfigURL = replaceMacWord(configURL, c, get_deviceMAC());
	CU_ASSERT_STRING_EQUAL(webConfigURL,"https://config/b42xxxxxxxxx/com");	
}

void test_replaceMac_NullUrl(){
    char *configURL= NULL;
    char c[] = "{sss}";
    char *webConfigURL = replaceMacWord(configURL, c, get_deviceMAC());
    CU_ASSERT_PTR_NULL(webConfigURL);
}

void test_replaceMac_NullMac(){
    char *configURL= "https://config/{sss}/com";
    char c[] = "{sss}";
    mock_device_mac = NULL;
    char *webConfigURL = replaceMacWord(configURL, c, get_deviceMAC());
    CU_ASSERT_STRING_EQUAL(webConfigURL,"https://config/000000000000/com");
    mock_device_mac = "b42xxxxxxxxx"; // Restore for other tests
}

void test_checkValidURL_ValidURL(){

	char *webConfigURL = strdup("https://sample/device/b42xxxxxxxxx/config");
	checkValidURL(&webConfigURL);
	const char *expected_url ="https://sample/device/b42xxxxxxxxx/config";
	CU_ASSERT_STRING_EQUAL(webConfigURL,expected_url);
}

void test_checkValidURL_InvalidURL(){

	char *webConfigURL = strdup("https://sample/device//config");
	checkValidURL(&webConfigURL);
	const char *expected_url ="https://sample/device/b42xxxxxxxxx/config";
	CU_ASSERT_STRING_EQUAL(webConfigURL,expected_url);
}

void test_checkValidURL_NullInput(){
    char *webConfigURL = NULL;
    checkValidURL(&webConfigURL);
    CU_ASSERT_PTR_NULL(webConfigURL);
}

void test_checkValidURL_InvalidMac(){

	char *webConfigURL = strdup("https://sample/device/c44xxxxxxxxx/config");
	checkValidURL(&webConfigURL);
	const char *expected_url ="https://sample/device/b42xxxxxxxxx/config";
	CU_ASSERT_STRING_EQUAL(webConfigURL,expected_url);
}


void test_createHeader(){
	
	CURL *curl;
	struct curl_slist *list = NULL;
	struct curl_slist *headers_list = NULL;
	char *transID = NULL;
	int status=0;
	curl = curl_easy_init();
	CU_ASSERT_PTR_NOT_NULL(curl);
	createCurlHeader(list, &headers_list, status, &transID);
	CU_ASSERT_PTR_NOT_NULL(transID);
	CU_ASSERT_PTR_NOT_NULL(headers_list);

	//success case
	setsupportedDocs("00000001000000000000000000000001");
	setsupportedVersion("1234-v0");	
	set_global_supplementarySync(1);
	setsupplementaryDocs("telemetry");
	createCurlHeader(list, &headers_list, status, &transID);
	CU_ASSERT_PTR_NOT_NULL(transID);
	CU_ASSERT_PTR_NOT_NULL(headers_list);
}

void test_createHeader_doc_header_max_data() {
    CURL *curl;
    struct curl_slist *list = NULL;
    struct curl_slist *headers_list = NULL;
    char *transID = NULL;
    int status = 0;

    curl = curl_easy_init();
    CU_ASSERT_PTR_NOT_NULL(curl);

    set_global_supplementarySync(0);

    populate_db_with_subdocs();

    createCurlHeader(list, &headers_list, status, &transID);
    CU_ASSERT_PTR_NOT_NULL(transID);
    CU_ASSERT_PTR_NOT_NULL(headers_list);

    struct curl_slist *temp = headers_list;
    int subdoc_found = 0;

    while (temp)
	{
		WebcfgInfo("  Checking header: %s\n", temp->data);
        if (strstr(temp->data, "Doc-Name:") == temp->data)
		{
            subdoc_found = 1;
            break;
        }
        temp = temp->next;
    }

	if (!subdoc_found)
		WebcfgInfo("  'Doc-Name' header NOT found!\n");
	else
		WebcfgInfo("  'Doc-Name' header found!\n");
	CU_ASSERT_TRUE(subdoc_found);

    if (transID) {
        free(transID);
        transID = NULL;
    }

    if (headers_list) {
        curl_slist_free_all(headers_list);
        headers_list = NULL;
    }

    if (curl) {
        curl_easy_cleanup(curl);
        curl = NULL;
    }

    webconfig_db_data_t *db_head = get_global_db_node();
    webconfig_db_data_t *next = NULL;

    while (db_head) {
        next = db_head->next;
        webcfgdb_destroy(db_head);
        db_head = next;
    }

    set_global_db_node(NULL);
}

void test_createHeader_primary_sync() {
    CURL *curl;
    struct curl_slist *list = NULL;
    struct curl_slist *headers_list = NULL;
    char *transID = NULL;
    int status = 0;

    curl = curl_easy_init();
    CU_ASSERT_PTR_NOT_NULL(curl);
    set_global_supplementarySync(0);

    createCurlHeader(list, &headers_list, status, &transID);
    CU_ASSERT_PTR_NOT_NULL(transID);
    CU_ASSERT_PTR_NOT_NULL(headers_list);

    const char *expected_headers[] = {
        "IF-NONE-MATCH:", "Doc-Name:", "POST-NONE-RETAIN: true"
    };
    const int num_expected_headers = sizeof(expected_headers) / sizeof(expected_headers[0]);
    int found_headers[num_expected_headers];
    memset(found_headers, 0, sizeof(found_headers));

    struct curl_slist *temp = headers_list;
    WebcfgInfo("Checking headers in headers_list for primary sync:\n");
    while (temp)
	{
        for (int i = 0; i < num_expected_headers; i++)
		{
            if (strncmp(temp->data, expected_headers[i], strlen(expected_headers[i])) == 0)
			{
                WebcfgInfo("  --> Found header: '%s'\n", expected_headers[i]);
                found_headers[i] = 1;
            }
        }
        temp = temp->next;
    }

    for (int i = 0; i < num_expected_headers; i++)
	{
        if (!found_headers[i]) printf("  --> Missing header: %s\n", expected_headers[i]);
        CU_ASSERT_TRUE(found_headers[i]);
    }

    // Cleanup
    if (transID) {
		free(transID);
		transID = NULL;
	}
	if (headers_list) {
		curl_slist_free_all(headers_list);
		headers_list = NULL;
	}
	if (curl) {
		curl_easy_cleanup(curl);
		curl = NULL;
	}
}

void test_createHeader_supplementary_sync() {
    CURL *curl;
    struct curl_slist *list = NULL;
    struct curl_slist *headers_list = NULL;
    char *transID = NULL;
    int status = 0;

    curl = curl_easy_init();
    CU_ASSERT_PTR_NOT_NULL(curl);
    set_global_supplementarySync(1);

    createCurlHeader(list, &headers_list, status, &transID);
    CU_ASSERT_PTR_NOT_NULL(transID);
    CU_ASSERT_PTR_NOT_NULL(headers_list);

    const char *expected_headers[] = {
        "X-System-SupplementaryService-Sync:", "X-System-Telemetry-Profile-Version:",
        "X-System-PartnerID:", "X-System-AccountID:", "X-System-Wan-Mac:"
    };
    int num_expected_headers = sizeof(expected_headers) / sizeof(expected_headers[0]);
    int found_headers[num_expected_headers];
    memset(found_headers, 0, sizeof(found_headers));

    struct curl_slist *temp = headers_list;
    WebcfgInfo("Checking headers in headers_list for supplementary sync:\n");

    while (temp)
	{
        for (int i = 0; i < num_expected_headers; i++)
		{
            if (strncmp(temp->data, expected_headers[i], strlen(expected_headers[i])) == 0)
			{
                WebcfgInfo("  --> Found header: '%s'\n", expected_headers[i]);
                found_headers[i] = 1;
            }
        }
        temp = temp->next;
    }

    for (int i = 0; i < num_expected_headers; i++)
	{
        if (!found_headers[i]) WebcfgInfo("  --> Missing header: %s\n", expected_headers[i]);
        CU_ASSERT_TRUE(found_headers[i]);
    }

    if (transID) {
		free(transID);
		transID = NULL;
	}
	if (headers_list) {
		curl_slist_free_all(headers_list);
		headers_list = NULL;
	}
	if (curl) {
		curl_easy_cleanup(curl);
		curl = NULL;
	}
}

void test_validateParam()
{	param_t *reqParam = NULL;
	int paramCount = 1;
 	reqParam = (param_t *) malloc(sizeof(param_t) * paramCount);
	reqParam[0].name = strdup("Device.X_RDK_WebConfig.RootConfig.Data");
	reqParam[0].value = strdup("true");
	reqParam[0].type = 2;
	int n=validate_request_param(reqParam, paramCount);
	CU_ASSERT_EQUAL(0,n);
	reqParam_destroy(paramCount, reqParam);
	
}

void test_checkRootUpdate(){
	webconfig_tmp_data_t *tmpData = (webconfig_tmp_data_t *)malloc(sizeof(webconfig_tmp_data_t));
	tmpData->name = strdup("wan");
	tmpData->version = 410448631;
	tmpData->status = strdup("success");
	tmpData->trans_id = 14464;
	tmpData->retry_count = 0;
	tmpData->error_code = 0;
	tmpData->error_details = strdup("none");
	tmpData->next = NULL;
	set_global_tmp_node(tmpData);
	int m=checkRootUpdate();
	CU_ASSERT_EQUAL(0,m);
	if(tmpData)
	{
		WEBCFG_FREE(tmpData->name);
		WEBCFG_FREE(tmpData->status);
		WEBCFG_FREE(tmpData->error_details);
		WEBCFG_FREE(tmpData);
	}
}

void test_checkDBList(){
	char *root_str = strdup("factory-reset");
	uint32_t root_version = 1234;
	webconfig_db_data_t * webcfgdb = NULL;
  	webcfgdb = (webconfig_db_data_t *) malloc (sizeof(webconfig_db_data_t));
	checkDBList("root", root_version, root_str);
	memset( webcfgdb, 0, sizeof( webconfig_db_data_t ) );
	CU_ASSERT_PTR_NOT_NULL(webcfgdb );
	WEBCFG_FREE(webcfgdb);
}
	
void test_updateDBlist(){
	char *rootstr = strdup("factory-reset");
	uint32_t version = 1234;
	int sts = updateDBlist("root", version, rootstr);
	CU_ASSERT_EQUAL(2,sts);
}

void test_appendedDoc(){
	size_t appenddocPackSize = -1;
	size_t embeddeddocPackSize = -1;
	void *appenddocdata = NULL;
	void *embeddeddocdata = NULL;
	appenddoc_t *appenddata = NULL;
        char * finaldocdata = NULL;

	//blob data
	webconfig_db_data_t *blob_data;
    	blob_data = (webconfig_db_data_t *) malloc (sizeof(webconfig_db_data_t));
	blob_data->name = strdup("wan");
	blob_data->version = 410448631;
	blob_data->root_string = strdup("portmapping");
	blob_data->next=NULL;
  	size_t blob_size=175;

	//append data
	appenddata = (appenddoc_t *) malloc(sizeof(appenddoc_t ));
	CU_ASSERT_PTR_NOT_NULL(appenddata);
	memset(appenddata, 0, sizeof(appenddoc_t));
        appenddata->subdoc_name = strdup("Device.Data");
        appenddata->version = 410448631;
	appenddata->transaction_id =22334;

	//pack append doc
	appenddocPackSize = webcfg_pack_appenddoc(appenddata, &appenddocdata);
	CU_ASSERT_FATAL( 0 != appenddocPackSize);
	WEBCFG_FREE(appenddata->subdoc_name);
    	WEBCFG_FREE(appenddata);
	
	//encode data
        embeddeddocPackSize = appendWebcfgEncodedData(&embeddeddocdata, (void *)blob_data, blob_size, appenddocdata, appenddocPackSize);
	CU_ASSERT_FATAL( 0 != embeddeddocPackSize);
	WEBCFG_FREE(appenddocdata);

	//base64 encode
        finaldocdata = base64blobencoder((char *)embeddeddocdata, embeddeddocPackSize);
	CU_ASSERT_FATAL( NULL != finaldocdata);
	WEBCFG_FREE(embeddeddocdata);
        
}

#ifdef FEATURE_SUPPORT_AKER

void test_UpdateErrorsendAkerblob(){
	WDMP_STATUS ret = WDMP_FAILURE;
	char *paramName= strdup("Device.DeviceInfo.X_RDKCENTRAL-COM_Aker.Update");
	char *blob= strdup("true");;
	uint32_t blobSize =2;
	uint16_t docTransId=12345; 
	int version=2.0;
	ret = send_aker_blob(paramName,blob,blobSize,docTransId,version);
	CU_ASSERT_EQUAL(1, ret);

}
void test_DeleteErrorsendAkerblob(){
	WDMP_STATUS ret = WDMP_FAILURE;
	char *paramName= strdup("Device.DeviceInfo.X_RDKCENTRAL-COM_Aker.Delete");
	char *blob= strdup("true");;
	uint32_t blobSize =2;
	uint16_t docTransId=12345; 
	int version=2.0;
	ret = send_aker_blob(paramName,blob,blobSize,docTransId,version);
	CU_ASSERT_EQUAL(1, ret);

}
void test_akerWait(){
	
	int backoffRetryTime = 0;
	int c=2;
	backoffRetryTime = (int) pow(2, c) -1;
	int wait=akerwait__ (backoffRetryTime);
	CU_ASSERT_EQUAL(0,wait);
}
#endif

void test_checkRootDelete(){
	webconfig_tmp_data_t *tmpData = (webconfig_tmp_data_t *)malloc(sizeof(webconfig_tmp_data_t));
	tmpData->name = strdup("moca");
	tmpData->version = 1234;
	tmpData->status = strdup("success");
	tmpData->trans_id = 4104;
	tmpData->retry_count = 0;
	tmpData->error_code = 0;
	tmpData->error_details = strdup("none");
	tmpData->next = NULL;
	set_global_tmp_node(tmpData);
	int m=checkRootDelete();
	CU_ASSERT_EQUAL(0,m);
	if(tmpData)
	{
		WEBCFG_FREE(tmpData->name);
		WEBCFG_FREE(tmpData->status);
		WEBCFG_FREE(tmpData->error_details);
		WEBCFG_FREE(tmpData);
	}

}

void test_checkRootDeleteFailure(){
	webconfig_tmp_data_t *tmpData = (webconfig_tmp_data_t *)malloc(sizeof(webconfig_tmp_data_t));
	tmpData->name = strdup("privatessid");
	tmpData->version = 1234;
	tmpData->status = strdup("pending");
	tmpData->trans_id = 4104;
	tmpData->retry_count = 0;
	tmpData->error_code = 0;
	tmpData->error_details = strdup("failed");
	tmpData->next = NULL;
	set_global_tmp_node(tmpData);
	int m=checkRootDelete();
	CU_ASSERT_EQUAL(1,m);
	if(tmpData)
	{
		WEBCFG_FREE(tmpData->name);
		WEBCFG_FREE(tmpData->status);
		WEBCFG_FREE(tmpData->error_details);
		WEBCFG_FREE(tmpData);
	}
}
void test_updateRootVersionToDB(){
	webconfig_tmp_data_t *tmpData = (webconfig_tmp_data_t *)malloc(sizeof(webconfig_tmp_data_t));
	tmpData->name = strdup("root");
	tmpData->version = 232323;
	tmpData->status = strdup("pending");
	tmpData->trans_id = 4104;
	tmpData->retry_count = 0;
	tmpData->error_code = 0;
	tmpData->error_details = strdup("none");
	tmpData->cloud_trans_id = NULL;
	tmpData->next = NULL;
	set_global_tmp_node(tmpData);
	set_global_ETAG("123");
	int m=updateRootVersionToDB();
	CU_ASSERT_EQUAL(0,m);
	if(tmpData)
	{
		WEBCFG_FREE(tmpData->name);
		WEBCFG_FREE(tmpData->status);
		WEBCFG_FREE(tmpData->error_details);
		WEBCFG_FREE(tmpData);
	}

}

void test_updateRootVersionToDBNoroot(){
	webconfig_tmp_data_t *tmpData = (webconfig_tmp_data_t *)malloc(sizeof(webconfig_tmp_data_t));
	tmpData->name = strdup("wan");
	tmpData->version = 232323;
	tmpData->status = strdup("pending");
	tmpData->trans_id = 4104;
	tmpData->retry_count = 0;
	tmpData->error_code = 0;
	tmpData->error_details = strdup("none");
	tmpData->next = NULL;
	set_global_tmp_node(tmpData);
	set_global_ETAG("123");
	int m=updateRootVersionToDB();
	CU_ASSERT_EQUAL(0,m);
	if(tmpData)
	{
		WEBCFG_FREE(tmpData->name);
		WEBCFG_FREE(tmpData->status);
		WEBCFG_FREE(tmpData->error_details);
		WEBCFG_FREE(tmpData);
	}

}
void test_set_global_ETAG(){
	set_global_ETAG("1234");
	CU_ASSERT_STRING_EQUAL(get_global_ETAG(),"1234");
}

void test_get_global_ETAG(){
	WebcfgDebug("etag is %s\n", get_global_ETAG());
	CU_ASSERT_FATAL( NULL != get_global_ETAG() );
}

void test_set_global_transID(){
	set_global_transID("3344411a");
	CU_ASSERT_STRING_EQUAL(get_global_transID(),"3344411a");
}

void test_get_global_transID(){
	WebcfgDebug("transID is %s\n", get_global_transID());
	CU_ASSERT_FATAL( NULL != get_global_transID() );
}
void test_set_global_contentLen(){
	set_global_contentLen("1001");
	CU_ASSERT_STRING_EQUAL(get_global_contentLen(),"1001");
}

void test_get_global_contentLen(){
	WebcfgDebug("contentLen is %s\n", get_global_contentLen());
	CU_ASSERT_FATAL( NULL != get_global_contentLen() );
}
void test_set_global_eventFlag(){
	set_global_eventFlag(1);
	CU_ASSERT_EQUAL(get_global_eventFlag(),1);
}

void test_get_global_eventFlag(){
	WebcfgDebug("eventFlag is %d\n", get_global_eventFlag());
	CU_ASSERT_FATAL( 0 != get_global_eventFlag() );
}

void test_reset_global_eventFlag(){
	reset_global_eventFlag();
	CU_ASSERT_EQUAL(get_global_eventFlag(),0);
}

#ifdef WAN_FAILOVER_SUPPORTED
void test_set_global_interface(){
	set_global_interface("eth0");
	CU_ASSERT_STRING_EQUAL(get_global_interface(),"eth0");
}

void test_get_global_interface(){
	WebcfgDebug("interface is %s\n", get_global_interface());
	CU_ASSERT_FATAL( NULL != get_global_interface() );
}
#endif
void test_print_tmp_doc_list(){
	webconfig_tmp_data_t *tmpData = (webconfig_tmp_data_t *)malloc(sizeof(webconfig_tmp_data_t));
	tmpData->name = strdup("wan");
	tmpData->version = 232323;
	tmpData->status = strdup("pending");
	tmpData->trans_id = 4104;
	tmpData->retry_count = 0;
	tmpData->error_code = 0;
	tmpData->error_details = strdup("none");
	tmpData->next = NULL;
	int m=print_tmp_doc_list(1);
	CU_ASSERT_EQUAL(0,m);
	if(tmpData)
	{
		WEBCFG_FREE(tmpData->name);
		WEBCFG_FREE(tmpData->status);
		WEBCFG_FREE(tmpData->error_details);
		WEBCFG_FREE(tmpData);
	}

}
void test_get_global_mp_null(){
	CU_ASSERT_FATAL( NULL == get_global_mp());
}

void test_get_global_mp(){
	multipartdocs_t *multipartdocs = (multipartdocs_t *)malloc(sizeof(multipartdocs_t));
	multipartdocs->name_space = strdup("portforwarding");
	multipartdocs->data = (char* )malloc(64);
	multipartdocs->isSupplementarySync = 0;
	multipartdocs->next = NULL;
	set_global_mp(multipartdocs);
	CU_ASSERT_FATAL( NULL !=get_global_mp());
	if(multipartdocs)
	{
		WEBCFG_FREE(multipartdocs->name_space);
		WEBCFG_FREE(multipartdocs->data);
		multipartdocs->data_size = 0;
		WEBCFG_FREE(multipartdocs);
		set_global_mp(NULL);
	}
}

void test_deleteRootAndMultipartDocs(){
	multipartdocs_t *multipartdocs = (multipartdocs_t *)malloc(sizeof(multipartdocs_t));
	multipartdocs->name_space = strdup("moca");
	multipartdocs->data = (char* )malloc(64);
	multipartdocs->isSupplementarySync = 0;
	multipartdocs->next = NULL;
	set_global_mp(multipartdocs);
	CU_ASSERT_FATAL( NULL !=get_global_mp());
	webconfig_tmp_data_t *tmpData = (webconfig_tmp_data_t *)malloc(sizeof(webconfig_tmp_data_t));
	tmpData->name = strdup("moca");
	tmpData->version = 1234;
	tmpData->status = strdup("success");
	tmpData->trans_id = 4104;
	tmpData->retry_count = 0;
	tmpData->error_code = 0;
	tmpData->error_details = strdup("none");
	tmpData->cloud_trans_id = NULL;
	tmpData->next = NULL;
	set_global_tmp_node(tmpData);
	int m=deleteRootAndMultipartDocs();
	CU_ASSERT_EQUAL(0,m);
	set_global_tmp_node(NULL);
	set_global_mp(NULL);
}

void test_deleteRootAndMultipartDocs_fail(){
	multipartdocs_t *multipartdocs = (multipartdocs_t *)malloc(sizeof(multipartdocs_t));
	multipartdocs->name_space = strdup("wan");
	multipartdocs->data = (char* )malloc(64);
	multipartdocs->isSupplementarySync = 0;
	multipartdocs->next = NULL;
	set_global_mp(multipartdocs);
	CU_ASSERT_FATAL( NULL !=get_global_mp());
	webconfig_tmp_data_t *tmpData = (webconfig_tmp_data_t *)malloc(sizeof(webconfig_tmp_data_t));
	tmpData->name = strdup("wan");
	tmpData->version = 3456;
	tmpData->status = strdup("pending");
	tmpData->trans_id = 1231;
	tmpData->retry_count = 0;
	tmpData->error_code = 0;
	tmpData->error_details = strdup("failed");
	tmpData->next = NULL;
	set_global_tmp_node(tmpData);
	int m=deleteRootAndMultipartDocs();
	CU_ASSERT_EQUAL(0,m);
	set_global_tmp_node(NULL);
	set_global_mp(NULL);
}

void test_deleteFromMpList(){
	multipartdocs_t *multipartdocs = (multipartdocs_t *)malloc(sizeof(multipartdocs_t));
	multipartdocs->name_space = strdup("wan");
	multipartdocs->data = (char* )malloc(64);
	multipartdocs->isSupplementarySync = 0;
	multipartdocs->next = NULL;
	set_global_mp(multipartdocs);
	int m= deleteFromMpList("wan");
	CU_ASSERT_EQUAL(0,m);
	set_global_mp(NULL);
}

void test_deleteFromMpListFailure(){
	multipartdocs_t *multipartdocs = (multipartdocs_t *)malloc(sizeof(multipartdocs_t));
	multipartdocs->name_space = strdup("wan");
	multipartdocs->data = (char* )malloc(64);
	multipartdocs->isSupplementarySync = 0;
	multipartdocs->next = NULL;
	set_global_mp(multipartdocs);
	int m= deleteFromMpList("moca");
	CU_ASSERT_EQUAL(1,m);
	set_global_mp(NULL);
}

void test_deleteFromMpListInvalidDoc(){
	multipartdocs_t *multipartdocs = (multipartdocs_t *)malloc(sizeof(multipartdocs_t));
	multipartdocs->name_space = strdup("wan");
	multipartdocs->data = (char* )malloc(64);
	multipartdocs->isSupplementarySync = 0;
	multipartdocs->next = NULL;
	set_global_mp(multipartdocs);
	int m= deleteFromMpList(NULL);
	CU_ASSERT_EQUAL(1,m);
	set_global_mp(NULL);
}

void test_deleteFromMpList_2docs(){
	addToMpList(123, "wan", "data1", 10);
	addToMpList(1234, "moca", "data2", 20);
	int m= deleteFromMpList("moca");
	CU_ASSERT_EQUAL(0,m);
	set_global_mp(NULL);
}

void test_addToMpList(){
	addToMpList(123, "wan", "data1", 10);
        CU_ASSERT_STRING_EQUAL(get_global_mp()->name_space, "wan");
	CU_ASSERT_EQUAL(get_global_mp()->etag,123);
	addToMpList(1234, "moca", "data2", 20);
	CU_ASSERT_STRING_EQUAL(get_global_mp()->next->name_space, "moca");
	CU_ASSERT_EQUAL(get_global_mp()->next->etag,1234);
	delete_multipart();
	set_global_mp(NULL);
}

void test_delete_mp_doc(){
	addToMpList(123, "wan", "data1", 10);
	addToMpList(1234, "moca", "data2", 20);
	delete_mp_doc("moca");
	delete_mp_doc("wan");
	CU_ASSERT_FATAL( NULL == get_global_mp() );
}

void test_get_multipartdoc_count(){
	addToMpList(44, "wan", "data1", 10);
	addToMpList(555, "moca", "data2", 20);
	addToMpList(666, "privatessid", "data3", 20);
	addToMpList(7777, "lan", "data4", 30);
	addToMpList(1111, "mesh", "data5", 20);
	CU_ASSERT_EQUAL(5,get_multipartdoc_count());
	delete_multipart();
	CU_ASSERT_FATAL( NULL == get_global_mp() );
}

void test_parseMultipartDocument_ValidBoundary() {
    WEBCFG_STATUS result;
	const char config_data[] = "HTTP 200 OK\r\nContent-Type: multipart/mixed; boundary=+CeB5yCWds7LeVP4oibmKefQ091Vpt2x4g99cJfDCmXpFxt5d\r\nEtag: 345431215\r\n\n--+CeB5yCWds7LeVP4oibmKefQ091Vpt2x4g99cJfDCmXpFxt5d\r\nContent-type: application/msgpack\r\nEtag: 2132354\r\nNamespace: value\r\n\r\nparameter: somedata\r\n--+CeB5yCWds7LeVP4oibmKefQ091Vpt2x4g99cJfDCmXpFxt5d--\r\n";
    size_t data_size = strlen(config_data);
    char ct[] = "Content-Type: multipart/mixed; boundary=+CeB5yCWds7LeVP4oibmKefQ091Vpt2x4g99cJfDCmXpFxt5d";
    char *trans_uuid = strdup("1234");
    char *config_data_copy = (char *)malloc(data_size + 1);

    if (config_data_copy != NULL) {
        strcpy(config_data_copy, config_data);
        result = parseMultipartDocument(config_data_copy, ct, data_size, trans_uuid);
        CU_ASSERT_EQUAL(result, WEBCFG_FAILURE);
    } 
}

void test_parseMultipartDocument_InvalidBoundary() {
    WEBCFG_STATUS result;
    const char config_data[] = "HTTP 200 OK\nContent-Type: multipart/mixed; boundary=\nEtag: 345431215\n\n--\nContent-type: application/msgpack\nEtag: 2132354\nNamespace: value\nparameter: somedata\n--";
    size_t data_size = strlen(config_data);
    char ct[] = "Content-Type: multipart/mixed; boundary=";
    char *trans_uuid = strdup("1234");
    char *config_data_copy = (char *)malloc(data_size + 1);

    if (config_data_copy != NULL) {
        strcpy(config_data_copy, config_data);
        result = parseMultipartDocument(config_data_copy, ct, data_size, trans_uuid);
        CU_ASSERT_EQUAL(result, WEBCFG_FAILURE);
    } else {
        CU_FAIL("Memory allocation for config_data_copy (invalid boundary) failed");
    }

    free(trans_uuid);
}

void test_loadInitURLFromFile()
{
	//URL value NULL
	char *data="";
	char *web_url = NULL;
	writeToDBFile(DEVICE_PROPS_FILE,data,strlen(data));
	loadInitURLFromFile(&web_url);
	CU_ASSERT_PTR_NULL(web_url);

	//URL value not NULL
	data = "WEBCONFIG_INIT_URL=tcp://https://test ";
	writeToDBFile(DEVICE_PROPS_FILE,data,strlen(data));
	loadInitURLFromFile(&web_url);
	CU_ASSERT_STRING_EQUAL("tcp://https://test", web_url);
	if (remove(DEVICE_PROPS_FILE) != 0) {
    		WebcfgError("Failed to remove file");
	}
}

void test_failedDocsRetry()
{
	multipartdocs_t *multipartdocs = (multipartdocs_t *)malloc(sizeof(multipartdocs_t));
	multipartdocs->name_space = strdup("moca");
	multipartdocs->data = (char* )malloc(64);
	multipartdocs->isSupplementarySync = 0;
	multipartdocs->next = NULL;
	set_global_mp(multipartdocs);
	//checkRetryTimer returns true
	webconfig_tmp_data_t *tmpData = (webconfig_tmp_data_t *)malloc(sizeof(webconfig_tmp_data_t));
	tmpData->name = strdup("moca");
	tmpData->version = 1234;
	tmpData->status = strdup("success");
	tmpData->trans_id = 4104;
	tmpData->retry_count = 0;
	tmpData->error_code = 192;
	tmpData->error_details = strdup("none");
	tmpData->retry_timestamp = 1;
	tmpData->cloud_trans_id = strdup("abc123");
	tmpData->next = NULL;
	set_global_tmp_node(tmpData);

	//subdoc set fails
	failedDocsRetry();
	CU_ASSERT_STRING_EQUAL("moca",tmpData->name);

	//retry skip
	failedDocsRetry();
	CU_ASSERT_PTR_NOT_NULL(tmpData->error_details);

}

void test_getRootDocVersionFromDBCache()
{
	uint32_t expected_version;
    	char *expected_string;
     	int expected_subdoclist;

	uint32_t rt_version;
    	char *rt_string = NULL;
     	int subdoclist = 0;
    	char *rootstr = strdup("factory-reset");
	uint32_t version = 1234;
	updateDBlist("root", version, rootstr);
    	getRootDocVersionFromDBCache(&rt_version, &rt_string, &subdoclist);

	expected_version = version;
	expected_string = rootstr;
	expected_subdoclist = 1;

	CU_ASSERT(rt_version == expected_version);
     	CU_ASSERT_STRING_EQUAL(rt_string, expected_string);  
      	CU_ASSERT_EQUAL(subdoclist, expected_subdoclist);
}

void test_lineparser()
{
	char *name_space = NULL;
    	uint32_t etag = 0;
    	char *data = NULL;
    	size_t data_size = 0;
	char input[100] = "Content-type: application/msgpack";

	//content type as msgpack
	line_parser(input, sizeof(input), &name_space, &etag, &data, &data_size);

	//content type not msgpack
	strcpy(input, "Content-type: application/");
	line_parser(input, sizeof(input), &name_space, &etag, &data, &data_size);

	//proper name
	strcpy(input, "Namespace: blob");
	line_parser(input, sizeof(input), &name_space, &etag, &data, &data_size);
	CU_ASSERT_STRING_EQUAL(name_space, "blob");

	//proper etag
	strcpy(input, "Etag: 2132354");
	line_parser(input, sizeof(input), &name_space, &etag, &data, &data_size);
    	CU_ASSERT_EQUAL(etag, 2132354);

	//parameter 
	strcpy(input, "ªparameters: somedata");
	line_parser(input, sizeof(input), &name_space, &etag, &data, &data_size);

    	CU_ASSERT_PTR_NOT_NULL(data);
      	CU_ASSERT_EQUAL(data_size, sizeof(input));
}

void test_subdoc_parser()
{
	set_global_mp(NULL);
	//Proper values
	char *input_data = "Content-type: application/msgpack\nNamespace: blob\nEtag: 12345\nparameters: somedata\nThis is the end of it\n";
	int no_of_bytes = strlen(input_data);
	subdoc_parser(input_data, no_of_bytes);

	CU_ASSERT_PTR_NOT_NULL(get_global_mp());

	//NULL etag and namespace
	input_data = "Content-type: application/msgpack\nNamespace: NULL\nEtag: 0\nparameters: somedata\nThis is the end of it\n";
	no_of_bytes = strlen(input_data);
	subdoc_parser(input_data, no_of_bytes);
	CU_ASSERT_PTR_NOT_NULL(get_global_mp());
}

void test_stripspaces() 
{
	char input1[] = "This is a test"; // No spaces, newlines, or carriage returns
    	char input2[] = "This\nis a\rtest"; // Contains newlines and carriage returns
    	char input3[] = " Remove   spaces "; // Contains extra spaces

	char *result1 = NULL;
    	char *result2 = NULL;
    	char *result3 = NULL;

	// Test with input1
	stripspaces(input1, &result1);
	CU_ASSERT_STRING_EQUAL(result1, "Thisisatest");

    	// Test with input2
	stripspaces(input2, &result2);
    	CU_ASSERT_STRING_EQUAL(result2, "Thisisatest");

	// Test with input3
    	stripspaces(input3, &result3);
    	CU_ASSERT_STRING_EQUAL(result3, "Removespaces");
}

void test_get_webCfg_interface()
{
	char *data = "WEBCONFIG_INTERFACE=eth0 ";
	char *interface = NULL;
	writeToDBFile(DEVICE_PROPS_FILE,data,strlen(data));

	get_webCfg_interface(&interface);
	CU_ASSERT_PTR_NOT_NULL(interface);
	CU_ASSERT_STRING_EQUAL(interface, "eth0");
	if (remove(DEVICE_PROPS_FILE) != 0) {
    		WebcfgError("Failed to remove file");
	}
}

void test_headr_callback()
{
	char buffer[100] = "Etag: 2132354";
	size_t size = strlen(buffer);
	size_t nitems = 15;
	void* data = NULL;
	set_global_contentLen(NULL);

	size_t result = headr_callback(buffer, size, nitems, data);
	CU_ASSERT_EQUAL(result, nitems);
	CU_ASSERT_PTR_NULL(get_global_contentLen());

	char *content = get_global_contentLen();
	WebcfgDebug("The content length is %s\n",content);
	strcpy(buffer,"Content-Length: 1052");
	size = strlen(buffer);
	result = headr_callback(buffer, size, nitems, data);
	CU_ASSERT_EQUAL(result, nitems);
	CU_ASSERT_PTR_NOT_NULL(get_global_contentLen());
	CU_ASSERT_STRING_EQUAL(get_global_contentLen(), "1052");
}

void test_writer_callback_fn() 
{
	mock_data.size = 0;
	mock_data.data = NULL;
	char buffer[] = "Hello, World!";
	size_t size = sizeof(char);
	size_t nmemb = strlen(buffer);

	size_t result = writer_callback_fn(buffer, size, nmemb, &mock_data);
	CU_ASSERT_EQUAL(result, nmemb);
	CU_ASSERT_EQUAL(mock_data.size, nmemb);
}

void test_refreshConfigVersionList() 
{
    	char versionsList[512];
    	char docsList[512];
	int http_status = 200;
	refreshConfigVersionList(versionsList, http_status, docsList);
	CU_ASSERT_PTR_NOT_NULL(get_global_db_node);
}

void test_readFromFile_success()
{
    char *filename = "testfile.txt";
    char *data = NULL;
    int len = 0;

    // Create a test file with some content
    FILE *testFile = fopen(filename, "w");
    fprintf(testFile, "This is a test file.");
    fclose(testFile);

    // Test the function
    int result = readFromFile(filename, &data, &len);

    // Check the result
    CU_ASSERT_EQUAL(result, 1); // Assuming your function returns 1 on success
    CU_ASSERT_PTR_NOT_NULL(data);
    CU_ASSERT_EQUAL(len, 20); // Update this value based on the actual length of the test file content

    // Clean up
    free(data);

    // Remove the test file
    remove(filename);
}

void test_readFromFile_failure(void) {
    char *filename = "nonexistentfile.txt";
    char *data = NULL;
    int len = 0;

    // Test the function with a nonexistent file
    int result = readFromFile(filename, &data, &len);

    // Check the result
    CU_ASSERT_EQUAL(result, 0);
    CU_ASSERT_PTR_NULL(data);
    CU_ASSERT_EQUAL(len, 0);
}

void test_processMsgpackSubdoc_failure()
{
	char *transaction_id = strdup("1234");
	WEBCFG_STATUS result;
	result = processMsgpackSubdoc(transaction_id);
	CU_ASSERT_EQUAL(result, WEBCFG_FAILURE);
}

void test_setForceTransID() {
    // Test with a valid transaction ID
    char testTransID[] = "1234";
    setForceSyncTransID(testTransID);
    CU_ASSERT_STRING_EQUAL(getForceSyncTransID(), "1234");
    // Test with a NULL transaction ID
    setForceSyncTransID(NULL);
    CU_ASSERT_STRING_EQUAL(getForceSyncTransID(), ""); // g_ForceSyncTransID should be cleared
}

#ifdef WEBCONFIG_BIN_SUPPORT
void test_processMsgpackSubdoc_msgpack_failure()
{
	int status = webconfigRbusInit("consumerComponent"); 
	if(status)
	{
		WebcfgInfo("rbus init success\n");
	}
	else{
		WebcfgError("rbus init failed");
	}

	SubDocSupportMap_t *supportdocs = (SubDocSupportMap_t *)malloc(sizeof(SubDocSupportMap_t));
	if(supportdocs)
	{
		strcpy(supportdocs->name,"value");
		strcpy(supportdocs->support,"true");
		strcpy(supportdocs->rbus_listener,"true");
		strcpy(supportdocs->dest,WEBCFG_BLOB_PARAM);
		supportdocs->next=NULL;
	}
	set_global_sdInfoHead(supportdocs);
	set_global_supplementarySync(0);


	char *transaction_id = strdup("1234");
	char *Data = "{\"parameters\": [{\"name\":\"Device.DeviceInfo.Test\",\"value\":\"false\",\"dataType\":3},{\"name\":\"Device.DeviceInfo.Test1\",\"value\":\"true\",\"dataType\":12}]}";

	char *encodedData = NULL;
	int encodedLen = 0;

	encodedLen = convertJsonToMsgPack(Data, &encodedData, 1);
	if(encodedLen)
	{
		multipartdocs_t *node = (multipartdocs_t *)malloc(sizeof(multipartdocs_t));
		if (node != NULL)
    	{
			node->etag = 345431215;
			node->name_space = strdup("value"); // Assuming strdup is available
			node->data = (char *)malloc(encodedLen);
			if (node->data != NULL)
			{
				memcpy(node->data, encodedData, encodedLen);
			}
			node->data_size = encodedLen;
			node->isSupplementarySync = 0;
			node->next = NULL;
			WEBCFG_STATUS result;
			set_global_mp(node);
			result = processMsgpackSubdoc(transaction_id);
			CU_ASSERT_EQUAL(result, WEBCFG_FAILURE);
		}
	}
	webpaRbus_Uninit(); 
}

void test_processMsgpackSubdoc_setValues_rbus()
{
	int res = rbus_open(&handle, "providerComponent");
    	if(res != RBUS_ERROR_SUCCESS)
	{
		CU_FAIL("rbus_open failed for providerComponent");
	}

	rbusDataElement_t webcfgBlobElement[1] = {
		{WEBCFG_BLOB_PARAM, RBUS_ELEMENT_TYPE_PROPERTY, {NULL, webcfgBlobElement1SetHandler, NULL, NULL, NULL, NULL}}
	};
	
	rbusError_t ret = rbus_regDataElements(handle, 1, webcfgBlobElement);
	CU_ASSERT_EQUAL(ret, RBUS_ERROR_SUCCESS);

	webconfigRbusInit("consumerComponent");
	regWebConfigDataModel();	
	SubDocSupportMap_t *supportdocs = (SubDocSupportMap_t *)malloc(sizeof(SubDocSupportMap_t));
	if(supportdocs)
	{
		strcpy(supportdocs->name,"value");
		strcpy(supportdocs->support,"true");
		strcpy(supportdocs->rbus_listener,"false");
		strcpy(supportdocs->dest,WEBCFG_BLOB_PARAM);
		supportdocs->next=NULL;
	}
	set_global_sdInfoHead(supportdocs);
	set_global_supplementarySync(0);

	char *transaction_id = strdup("1234");
	char *Data = "{\"parameters\": [{\"name\":\"Device.DeviceInfo.Test\",\"value\":\"false\",\"dataType\":0}]}";

	char *encodedData = NULL;
	int encodedLen = 0;

	encodedLen = convertJsonToMsgPack(Data, &encodedData, 1);
	if(encodedLen)
	{
		multipartdocs_t *node = (multipartdocs_t *)malloc(sizeof(multipartdocs_t));
		if (node != NULL)
    	{
			node->etag = 345431215;
			node->name_space = strdup("value"); 
			node->data = (char *)malloc(encodedLen);
			if (node->data != NULL)
			{
				memcpy(node->data, encodedData, encodedLen);
			}
			node->data_size = encodedLen;
			node->isSupplementarySync = 0;
			node->next = NULL;
			WEBCFG_STATUS result;
			set_global_mp(node);
			result = processMsgpackSubdoc(transaction_id);
			CU_ASSERT_EQUAL(result, WEBCFG_SUCCESS);
		}
	}
	webpaRbus_Uninit(); 
}
#endif

void add_suites( CU_pSuite *suite )
{
    *suite = CU_add_suite( "tests", NULL, NULL );
      CU_add_test( *suite, "test  generate_trans_uuid", test_generate_trans_uuid);
      CU_add_test( *suite, "test  replaceMacWord", test_replaceMac);  
      CU_add_test( *suite, "test  replaceMacWord_NullMac", test_replaceMac_NullMac);
      CU_add_test( *suite, "test replaceMac_NullUrl", test_replaceMac_NullUrl);
      CU_add_test( *suite, "test  checkValidURL_ValidURL", test_checkValidURL_ValidURL);  
      CU_add_test( *suite, "test  checkValidURL_InvalidURL", test_checkValidURL_InvalidURL);  
      CU_add_test( *suite, "test  checkValidURL_NullInput", test_checkValidURL_NullInput);
      CU_add_test( *suite, "test checkValidURL_InvalidMac", test_checkValidURL_InvalidMac);
      CU_add_test( *suite, "test  createCurlHeader", test_createHeader);
	  CU_add_test( *suite, "test  createHeader_doc_header_max_data", test_createHeader_doc_header_max_data);
	  CU_add_test( *suite, "test  createHeader_primary_sync", test_createHeader_primary_sync);
	  CU_add_test( *suite, "test  createHeader_supplementary_sync", test_createHeader_supplementary_sync);
      CU_add_test( *suite, "test  validateParam", test_validateParam);
      CU_add_test( *suite, "test  checkRootUpdate", test_checkRootUpdate);
      CU_add_test( *suite, "test  checkRootDelete", test_checkRootDelete);
      CU_add_test( *suite, "test  checkRootDeleteFailure", test_checkRootDeleteFailure);
      CU_add_test( *suite, "test  updateRootVersionToDB", test_updateRootVersionToDB);
      CU_add_test( *suite, "test  updateRootVersionToDBNoroot", test_updateRootVersionToDBNoroot);
      CU_add_test( *suite, "test  set_global_ETAG", test_set_global_ETAG);
      CU_add_test( *suite, "test  get_global_ETAG", test_get_global_ETAG);
      CU_add_test( *suite, "test  set_global_transID", test_set_global_transID);
      CU_add_test( *suite, "test  get_global_transID", test_get_global_transID);
      CU_add_test( *suite, "test  set_global_contentLen", test_set_global_contentLen);
      CU_add_test( *suite, "test  get_global_contentLen", test_get_global_contentLen);
      CU_add_test( *suite, "test  set_global_eventFlag", test_set_global_eventFlag);
      CU_add_test( *suite, "test  get_global_eventFlag", test_get_global_eventFlag);
      CU_add_test( *suite, "test  reset_global_eventFlag", test_reset_global_eventFlag);
#ifdef WAN_FAILOVER_SUPPORTED
      CU_add_test( *suite, "test  set_global_interface", test_set_global_interface);
      CU_add_test( *suite, "test  get_global_interface", test_get_global_interface);
#endif
      CU_add_test( *suite, "test  print_tmp_doc_list", test_print_tmp_doc_list);
      CU_add_test( *suite, "test  get_global_mp_null", test_get_global_mp_null);
      CU_add_test( *suite, "test  get_global_mp", test_get_global_mp);
      CU_add_test( *suite, "test  deleteRootAndMultipartDocs", test_deleteRootAndMultipartDocs);
      CU_add_test( *suite, "test  deleteRootAndMultipartDocs_fail", test_deleteRootAndMultipartDocs_fail);
      CU_add_test( *suite, "test  checkDBList", test_checkDBList);
      CU_add_test( *suite, "test  updateDBlist", test_updateDBlist);
      CU_add_test( *suite, "test  pack append doc", test_appendedDoc);
#ifdef FEATURE_SUPPORT_AKER
      CU_add_test( *suite, "test  Aker update blob send", test_UpdateErrorsendAkerblob);
      CU_add_test( *suite, "test  Aker delete blob send", test_DeleteErrorsendAkerblob);
      CU_add_test( *suite, "test  Aker wait", test_akerWait);
#endif
      CU_add_test( *suite, "test  deleteFromMpList", test_deleteFromMpList);
      CU_add_test( *suite, "test  deleteFromMpListFailure", test_deleteFromMpListFailure);
      CU_add_test( *suite, "test  deleteFromMpListInvalidDoc", test_deleteFromMpListInvalidDoc);
      CU_add_test( *suite, "test  deleteFromMpList_2docs", test_deleteFromMpList_2docs);
      CU_add_test( *suite, "test  addToMpList", test_addToMpList);
      CU_add_test( *suite, "test  delete_mp_doc", test_delete_mp_doc);
      CU_add_test( *suite, "test  get_multipartdoc_count", test_get_multipartdoc_count);
      CU_add_test( *suite, "test  parseMultipartDocument_ValidBoundary", test_parseMultipartDocument_ValidBoundary);
      CU_add_test( *suite, "test  parseMultipartDocument_InvalidBoundary", test_parseMultipartDocument_InvalidBoundary);
	  CU_add_test( *suite, "test loadInitURLFromFile", test_loadInitURLFromFile);
      CU_add_test( *suite, "test failedDocsRetry", test_failedDocsRetry);
      CU_add_test( *suite, "test getRootDocVersionFromDBCache", test_getRootDocVersionFromDBCache);
      CU_add_test( *suite, "test lineparser", test_lineparser);
      CU_add_test( *suite, "test subdoc_parser", test_subdoc_parser);
      CU_add_test( *suite, "test stripspaces", test_stripspaces);
      CU_add_test( *suite, "test get_webCfg_interface", test_get_webCfg_interface);
      CU_add_test( *suite, "test headr_callback", test_headr_callback);  
      CU_add_test( *suite, "test writer_callback_fn", test_writer_callback_fn);  
      CU_add_test( *suite, "test refreshConfigVersionList", test_refreshConfigVersionList);
      CU_add_test( *suite, "test readFromFile_success", test_readFromFile_success);
	  CU_add_test( *suite, "test readFromFile_failure", test_readFromFile_failure);  
      CU_add_test( *suite, "test processMsgpackSubdoc_failure", test_processMsgpackSubdoc_failure);
      CU_add_test( *suite, "test  reset_global_eventFlag", test_setForceTransID);
#ifdef WEBCONFIG_BIN_SUPPORT
      CU_add_test( *suite, "test processMsgpackSubdoc_msgpack_failure", test_processMsgpackSubdoc_msgpack_failure);
      CU_add_test( *suite, "test processMsgpackSubdoc_setValues_rbus", test_processMsgpackSubdoc_setValues_rbus);
#endif
}

/*----------------------------------------------------------------------------*/
/*                             External Functions                             */
/*----------------------------------------------------------------------------*/
int main( int argc, char *argv[] )
{
    unsigned rv = 1;
    CU_pSuite suite = NULL;
 
    (void ) argc;
    (void ) argv;
    
    if( CUE_SUCCESS == CU_initialize_registry() ) {
        add_suites( &suite );

        if( NULL != suite ) {
            CU_basic_set_mode( CU_BRM_VERBOSE );
            CU_basic_run_tests();
            WebcfgDebug( "\n" );
            CU_basic_show_failures( CU_get_failure_list() );
            WebcfgDebug( "\n\n" );
            rv = CU_get_number_of_tests_failed();
        }

        CU_cleanup_registry();

    }

    return rv;
}
