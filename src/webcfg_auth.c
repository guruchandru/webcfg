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
#include "webcfg_auth.h"
#include "webcfg_generic.h"
#include "webcfg.h"
/*----------------------------------------------------------------------------*/
/*                                   Macros                                   */
/*----------------------------------------------------------------------------*/
char webpa_auth_token[4096]={'\0'};
char serialNum[64]={'\0'};
/*----------------------------------------------------------------------------*/
/*                               Data Structures                              */
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
/*                            File Scoped Variables                           */
/*----------------------------------------------------------------------------*/
static int flag_unk = 0;
/*----------------------------------------------------------------------------*/
/*                             Function Prototypes                            */
/*----------------------------------------------------------------------------*/
void execute_token_script(char *token, char *name, size_t len, char *mac, char *serNum);
/*----------------------------------------------------------------------------*/
/*                             External Functions                             */
/*----------------------------------------------------------------------------*/

char* get_global_auth_token()
{
    return webpa_auth_token;
}

char* get_global_serialNum()
{
    return serialNum;
}

/*
* Fetches authorization token from the output of read script. If read script returns "ERROR"
* it will call createNewAuthToken to create and read new token
*/

void getAuthToken()
{
	//local var to update webpa_auth_token only in success case
	char output[4069] = {'\0'} ;
	char *serial_number=NULL;
	memset (webpa_auth_token, 0, sizeof(webpa_auth_token));

	if( strlen(WEBPA_READ_HEADER) !=0 && strlen(WEBPA_CREATE_HEADER) !=0)
	{
                get_deviceMAC();
                WebcfgInfo("deviceMAC: %s\n",get_deviceMAC());

		if( get_deviceMAC() != NULL && strlen(get_deviceMAC()) !=0 )
		{
			if((strlen(serialNum) ==0) || flag_unk == 1)
			{
				WebcfgInfo("getSerialNumber\n");
				serial_number = getSerialNumber();
		                if(serial_number != NULL && strlen(serial_number) > 0)
		                {
					strncpy(serialNum ,serial_number, sizeof(serialNum)-1);
					WebcfgInfo("serialNum: %s\n", serialNum);
					WEBCFG_FREE(serial_number);
					flag_unk = 0;
		                }
				else
				{
					WebcfgError("serialNum is NULL, adding default as unknown\n");
					strncpy(serialNum ,"unknown", sizeof(serialNum)-1);
					WebcfgInfo("serialNum: %s\n", serialNum);
					flag_unk = 1;
				}
			}

			if( strlen(serialNum)>0 )
			{
				execute_token_script(output, WEBPA_READ_HEADER, sizeof(output), get_deviceMAC(), serialNum);
				if ((strlen(output) == 0))
				{
					WebcfgError("Unable to get auth token\n");
				}
				else if(strcmp(output,"ERROR")==0)
				{
					WebcfgInfo("Failed to read token from %s. Proceeding to create new token.\n",WEBPA_READ_HEADER);
					//Call create/acquisition script
					createNewAuthToken(webpa_auth_token, sizeof(webpa_auth_token), get_deviceMAC(), serialNum );
				}
				else
				{
					WebcfgInfo("update webpa_auth_token in success case\n");
					webcfgStrncpy(webpa_auth_token, output, sizeof(webpa_auth_token));
				}
			}
			else
			{
				WebcfgError("serialNum is NULL, failed to fetch auth token\n");
			}
		}
		else
		{
			WebcfgError("deviceMAC is NULL, failed to fetch auth token\n");
		}
	}
	else
	{
		WebcfgError("Both read and write file are NULL \n");
	}
}

/*
* call parodus create/acquisition script to create new auth token, if success then calls
* execute_token_script func with args as parodus read script.
*/

void createNewAuthToken(char *newToken, size_t len, char *hw_mac, char* hw_serial_number)
{
	//Call create script
	char output[12] = {'\0'};
	execute_token_script(output,WEBPA_CREATE_HEADER,sizeof(output),hw_mac,hw_serial_number);
	if (strlen(output)>0  && strcmp(output,"SUCCESS")==0)
	{
		//Call read script
		execute_token_script(newToken,WEBPA_READ_HEADER,len,hw_mac,hw_serial_number);
	}
	else
	{
		WebcfgError("Failed to create new token\n");
	}
}

/*----------------------------------------------------------------------------*/
/*                             External Functions                             */
/*----------------------------------------------------------------------------*/

void execute_token_script(char *token, char *name, size_t len, char *mac, char *serNum)
{
    FILE* out = NULL, *file = NULL;
    char command[MAX_BUF_SIZE] = {'\0'};
    if(strlen(name)>0)
    {
        file = fopen(name, "r");
        if(file)
        {
            snprintf(command,sizeof(command),"%s %s %s",name,serNum,mac);
            out = popen(command, "r");
            if(out)
            {
                fgets(token, len, out);
                pclose(out);
            }
            fclose(file);
        }
        else
        {
            WebcfgError ("File %s open error\n", name);
        }
    }
}



