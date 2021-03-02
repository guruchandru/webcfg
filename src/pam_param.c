/*
 * Copyright 2019 Comcast Cable Communications Management, LLC
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
#include <errno.h>
#include <string.h>
#include <msgpack.h>
#include <stdarg.h>
#include "webcfg_log.h"
#include "msgpack_blob_parser.h"
#include "pam_param.h"
/*----------------------------------------------------------------------------*/
/*                                   Macros                                   */
/*----------------------------------------------------------------------------*/
/* none */
/*----------------------------------------------------------------------------*/
/*                               Data Structures                              */
/*----------------------------------------------------------------------------*/
enum {
    OK                       = MSGPACK_OK,
    OUT_OF_MEMORY            = MSGPACK_OUT_OF_MEMORY,
    INVALID_FIRST_ELEMENT    = MSGPACK_INVALID_ELEMENT,
    MISSING_ENTRY         = MSGPACK_MISSING_WRAPPER,
    UNPACK_FAILED         = MSGPACK_UNPACK_FAILED,
    INVALID_DATATYPE,
    MISSING_TARGET_IP,
    MISSING_PORT_RANGE,
    MISSING_PROTOCOL,
    INVALID_OBJECT,
    INVALID_VERSION,
};
/*----------------------------------------------------------------------------*/
/*                            File Scoped Variables                           */
/*----------------------------------------------------------------------------*/
/* none */
/*----------------------------------------------------------------------------*/
/*                             Function Prototypes                            */
/*----------------------------------------------------------------------------*/
int process_pamparams( pparam_t *e, msgpack_object_map *map );
int process_tunnel_tableparams( tunnel_t *e, msgpack_object_map *map );
int process_tunnelparams( tdoc_t *e, msgpack_object_map *map );
int process_pamdoc( pamparam_t *pd, int num, ...); 
int process_tunneldoc( tunneldoc_t *td,int num, ... );
int process_wifi_doc( wifi_doc_t *wd,int num, ... );
/*----------------------------------------------------------------------------*/
/*                             External Functions                             */
/*----------------------------------------------------------------------------*/
/* See xdnsdoc.h for details. */
pamparam_t* pamdoc_convert( const void *buf, size_t len )
{
	return msgpack_process( buf, len, sizeof(pamparam_t), "PublicHotspotData", 
                            MSGPACK_OBJECT_ARRAY, true,
                           (process2_fn_t) process_pamdoc,
                           (destroy2_fn_t) pamdoc_destroy, 2 );
}

tunneldoc_t* tunneldoc_convert(const void *buf, size_t len)
{
	return msgpack_process( buf, len, sizeof(tunneldoc_t), "Tunnels", 
                            MSGPACK_OBJECT_ARRAY, true,
                           (process2_fn_t) process_tunneldoc,
                           (destroy2_fn_t) tunneldoc_destroy, 1 );
}

wifi_doc_t* wifi_doc_convert(const void *buf, size_t len)
{
	return msgpack_process( buf, len, sizeof(tunneldoc_t), "Wifi_SSID_Config", 
                            MSGPACK_OBJECT_ARRAY, true,
                           (process2_fn_t) process_wifi_doc,
                           (destroy2_fn_t) wifi_doc_destroy, 1);
}
/* See xdnsdoc.h for details. */
void tunneldoc_destroy( tunneldoc_t *td )
{
	size_t i, j = 0;

	if( NULL != td )
	{
		if(NULL != td->entries)
		{
			for(i = 0; i< td->entries_count; i++)
			{
				if( NULL != td->entries[i].gre_primary_endpoint )
				{
					free(td->entries[i].gre_primary_endpoint);
				}
				if( NULL != td->entries[i].gre_sec_endpoint)
				{
					free(td->entries[i].gre_sec_endpoint);
				}
				if( NULL != td->entries[i].table_param )
				{
					if( NULL != td->entries[i].table_param->entries )
					{
						for( j = 0; j < td->entries[i].table_param->entries_count; j++ )
						{
							if( NULL != td->entries[i].table_param->entries[j].vap_name )
							{
								free(td->entries[i].table_param->entries[j].vap_name);
							}
						
						}
						free(td->entries[i].table_param->entries);
					}
					free(td->entries[i].table_param);
				}
			}
		free(td->entries);
		}
		free( td );
	}
}

void wifi_doc_destroy( wifi_doc_t *wd )
{
	if(wd != NULL)
	{
		free(wd);
	}
	
}

void pamdoc_destroy( pamparam_t *pd )
{

	if( NULL != pd )
	{
		size_t i;
		for( i = 0; i < pd->entries_count; i++ )
		{
			if( NULL != pd->entries[i].name )
			{
				free( pd->entries[i].name );
			}
			if( NULL != pd->entries[i].value )
			{
				free( pd->entries[i].value );
			}
		}
		if( NULL != pd->entries )
		{
			free( pd->entries );
		}
		if(NULL != pd->subdoc_name)
		{
			free(pd->subdoc_name);
		}
		free( pd );
    	}
	
}

/* See xdnsdoc.h for details. */
const char* pamdoc_strerror( int errnum )
{
    struct error_map {
        int v;
        const char *txt;
    } map[] = {
        { .v = OK,                               .txt = "No errors." },
        { .v = OUT_OF_MEMORY,                    .txt = "Out of memory." },
        { .v = INVALID_FIRST_ELEMENT,            .txt = "Invalid first element." },
        { .v = INVALID_VERSION,                 .txt = "Invalid 'version' value." },
        { .v = INVALID_OBJECT,                .txt = "Invalid 'value' array." },
	{ .v = UNPACK_FAILED,                .txt = "Msgpack Unpack Failed" },
        { .v = 0, .txt = NULL }
    };
    int i = 0;
    while( (map[i].v != errnum) && (NULL != map[i].txt) ) { i++; }
    if( NULL == map[i].txt )
    {
	WebcfgDebug("----xdnsdoc_strerror----\n");
        return "Unknown error.";
    }
    return map[i].txt;
}
/*----------------------------------------------------------------------------*/
/*                             Internal functions                             */
/*----------------------------------------------------------------------------*/
/**
 *  Convert the msgpack map into the dnsMapping_t structure.
 *
 *  @param e    the entry pointer
 *  @param map  the msgpack map pointer
 *
 *  @return 0 on success, error otherwise
 */
int process_tunnel_tableparams( tunnel_t *e, msgpack_object_map *map )
{
    int left = map->size;
    uint8_t objects_left = 0x03;
    msgpack_object_kv *p;
    p = map->ptr;
    WebcfgDebug("Starting of tunnel table params\n");
    WebcfgDebug("The left value is %d\n", left);
    WebcfgDebug("The objects_left value is %d\n", objects_left);
    while( (0 < objects_left) && (0 < left--) )
    {
        if( MSGPACK_OBJECT_STR == p->key.type )
        {
              if(MSGPACK_OBJECT_STR == p->val.type)
              {
                 if( 0 == match1(p, "vap_name") )
                 {
                     e->vap_name = strndup( p->val.via.str.ptr, p->val.via.str.size );
                     objects_left &= ~(1 << 0);
                     WebcfgDebug("Inside vap_name\n");
		     WebcfgDebug("The left value is %d\n", left);
    WebcfgDebug("The objects_left value is %d\n", objects_left);
                 }
              }
              else if( MSGPACK_OBJECT_POSITIVE_INTEGER == p->val.type )
              {
                 if( 0 == match1(p, "wan_vlan") )
                 {
                     if( UINT16_MAX < p->val.via.u64 )
                     {
                         errno = INVALID_DATATYPE;
                         return -1;
                     }
                     else
                     {
                         e->wan_vlan = (unsigned int) p->val.via.u64;
			WebcfgDebug("Inside wan_vlan\n");
		     WebcfgDebug("The left value is %d\n", left);
    WebcfgDebug("The objects_left value is %d\n", objects_left);
                     }
                     objects_left &= ~(1 << 2);
                 }
              }
              else if( MSGPACK_OBJECT_BOOLEAN == p->val.type )
              {
                 if( 0 == match1(p, "enable") )
                 {
                     e->enable = p->val.via.boolean;
                     objects_left &= ~(1 << 1);
		     WebcfgDebug("Inside enable\n");
		     WebcfgDebug("The left value is %d\n", left);
    WebcfgDebug("The objects_left value is %d\n", objects_left);
                 }
              }

        }
           p++;
    }
        
     WebcfgDebug("The Final left value is %d\n", left);
    WebcfgDebug("The Final objects_left value is %d\n", objects_left);
    if( 1 & objects_left ) {
    } else {
        errno = OK;
    }
   
    return (0 == objects_left) ? 0 : -1;
}

/**
 *  Convert the msgpack map into the doc_t structure.
 *
 *  @param e    the entry pointer
 *  @param map  the msgpack map pointer
 *
 *  @return 0 on success, error otherwise
 */
int process_tunnelparams( tdoc_t *e, msgpack_object_map *map )
{
    int left = map->size;
    size_t i =0;
    uint8_t objects_left = 0x08;
    msgpack_object_kv *p;
    p = map->ptr;
    WebcfgDebug("The Initial left value is %d\n", left);
    WebcfgDebug("The Initial objects_left value is %d\n", objects_left/2);
    while( (0 < objects_left) && (0 < left--) )
    {
        if( MSGPACK_OBJECT_STR == p->key.type )
        {
              if( MSGPACK_OBJECT_BOOLEAN == p->val.type )
              {
                 if( 0 == match1(p, "gre_enable") )
                 {
                     e->gre_enable = p->val.via.boolean;
                     objects_left = objects_left >> 1;
                     WebcfgDebug("Inside gre_enable\n");
                     WebcfgDebug("The left value is %d\n", left);
                     WebcfgDebug("The objects_left value is %d\n", objects_left/2);
                 }
              }
              else if(MSGPACK_OBJECT_STR == p->val.type)
              {
                 if( 0 == match1(p, "gre_primary_endpoint") )
                 {
                     e->gre_primary_endpoint = strndup( p->val.via.str.ptr, p->val.via.str.size );
                     objects_left = objects_left >> 1;
                     WebcfgDebug("Inside gre_primary_endpoint\n");
                     WebcfgDebug("The left value is %d\n", left);
                     WebcfgDebug("The objects_left value is %d\n", objects_left/2);
                 }
                 if( 0 == match1(p, "gre_sec_endpoint") )
                 {
                     e->gre_sec_endpoint = strndup( p->val.via.str.ptr, p->val.via.str.size );
                     objects_left = objects_left >> 1;
                     WebcfgDebug("Inside gre_sec_endpoint\n");
                     WebcfgDebug("The left value is %d\n", left);
                     WebcfgDebug("The objects_left value is %d\n", objects_left/2);
                 }
              }
              else if( MSGPACK_OBJECT_POSITIVE_INTEGER == p->val.type )
              {
                 if( 0 == match1(p, "gre_dscp") )
                 {
                     if( UINT16_MAX < p->val.via.u64 )
                     {
                         errno = INVALID_DATATYPE;
                         return -1;
                     }
                     else
                     {
                         e->gre_dscp = (int) p->val.via.u64;
                         WebcfgDebug("Inside gre_dscp\n");
                         WebcfgDebug("The left value is %d\n", left);
                         WebcfgDebug("The objects_left value is %d\n", objects_left/2);
                     }
                 }
              }
              else if( MSGPACK_OBJECT_ARRAY == p->val.type )
              {
                 if( 0 == match1(p, "tunnel_network") )
                 {
                      e->table_param = (tunnelTable_t *) malloc( sizeof(tunnelTable_t) );
                      if( NULL == e->table_param )
                      {
	                  WebcfgDebug("table_param malloc failed\n");
                          return -1;
                      }
                      memset( e->table_param, 0, sizeof(tunnelTable_t));

                      e->table_param->entries_count = p->val.via.array.size;

                      e->table_param->entries = (tunnel_t *) malloc( sizeof(tunnel_t) * e->table_param->entries_count);

                      if( NULL == e->table_param->entries )
                      {
	                  WebcfgDebug("table_param malloc failed\n");
                          e->table_param->entries_count = 0;
                          return -1;
                      }
                      memset( e->table_param->entries, 0, sizeof(tunnel_t) * e->table_param->entries_count);

                      for( i = 0; i < e->table_param->entries_count; i++ )
                      {
                          if( MSGPACK_OBJECT_MAP != p->val.via.array.ptr[i].type )
                          {
                              WebcfgDebug("invalid OBJECT \n");
                              errno = INVALID_OBJECT;
                              return -1;
                          }

                          if( 0 != process_tunnel_tableparams(&e->table_param->entries[i], &p->val.via.array.ptr[i].via.map) )
                          {
		              WebcfgDebug("process_tunnel_tableparams failed\n");
                              return -1;
                          }
           
                      }
		      WebcfgDebug("Inside tunnel table\n");
                      objects_left = objects_left >> 1;
                     WebcfgDebug("Inside array part\n");
                     WebcfgDebug("The left value is %d\n", left);
                     WebcfgDebug("The objects_left value is %d\n", objects_left/2);
                }
             }
        }
           p++;
    }
    WebcfgDebug("The Final left value is %d\n", left);
    WebcfgDebug("The Final objects_left value is %d\n", objects_left/2);  
   if(e->gre_enable == 0)
    {
        objects_left = objects_left >> 1;
        WebcfgDebug("The condition check objects_left value is %d\n", objects_left/2);
    }
    if( 1 & objects_left ) {
    } else {
        errno = OK;
    }
   
    return (0 == objects_left) ? 0 : -1;
}

int process_pamparams( pparam_t *e, msgpack_object_map *map )
{
    int left = map->size;
    uint8_t objects_left = 0x02;
    msgpack_object_kv *p;

    p = map->ptr;
    while( (0 < objects_left) && (0 < left--) )
	{
        if( MSGPACK_OBJECT_STR == p->key.type )
	{
        	if( MSGPACK_OBJECT_STR == p->val.type )
		{
		        if( 0 == match1(p, "name") )
			{
		            e->name = strndup( p->val.via.str.ptr, p->val.via.str.size );
		            objects_left &= ~(1 << 0);
		        }
			if( 0 == match1(p, "value"))
			{
				WebcfgDebug("blob size update\n");
				e->value = malloc(sizeof(char) * p->val.via.str.size+1 );
				memset( e->value, 0, sizeof(char) * p->val.via.str.size+1);
				e->value = memcpy(e->value, p->val.via.str.ptr, p->val.via.str.size+1 );
				e->value[p->val.via.str.size] = '\0';
				e->value_size =(uint32_t) p->val.via.str.size;
				WebcfgDebug("e->value_size is %lu\n", (long)e->value_size);
				objects_left &= ~(1 << 1);
		        }
	
            }
        }
        p++;
    }

    if( 1 & objects_left ) {
    } else {
        errno = OK;
    }

    return (0 == objects_left) ? 0 : -1;
}

int process_pamdoc( pamparam_t *pd,int num, ... )
{
	if(pd != NULL)
	{
//To access the variable arguments use va_list 
	va_list valist;
	va_start(valist, num);//start of variable argument loop
	printf("check\n");
	msgpack_object *obj = va_arg(valist, msgpack_object *);//each usage of va_arg fn argument iterates by one time
	msgpack_object_array *array = &obj->via.array;
	printf("check\n");
	msgpack_object *obj1 = va_arg(valist, msgpack_object *);
	printf("check\n");
	pd->subdoc_name = strndup( obj1->via.str.ptr, obj1->via.str.size );
	printf("check\n");
	msgpack_object *obj2 = va_arg(valist, msgpack_object *);
	pd->version = (uint32_t) obj2->via.u64;
	printf("check\n");
	msgpack_object *obj3 = va_arg(valist, msgpack_object *);
	pd->transaction_id = (uint16_t) obj3->via.u64;

	va_end(valist);//End of variable argument loop
	printf("before array check\n");
	if( 0 < array->size )
	{
		size_t i;

		pd->entries_count = array->size;
		pd->entries = (pparam_t *) malloc( sizeof(pparam_t) * pd->entries_count );
		if( NULL == pd->entries )
		{
		    pd->entries_count = 0;
		    return -1;
		}

		memset( pd->entries, 0, sizeof(pparam_t) * pd->entries_count );
		for( i = 0; i < pd->entries_count; i++ )
		{
			if( MSGPACK_OBJECT_MAP != array->ptr[i].type )
			{
				errno = INVALID_OBJECT;
				WebcfgError("Invalid object\n");
				return -1;
			}
			if( 0 != process_pamparams(&pd->entries[i], &array->ptr[i].via.map))
			{
				WebcfgError("process_pamparams failed\n");
				return -1;
			}
		}
	}
}
	

    return 0;
}

int process_tunneldoc( tunneldoc_t *td,int num, ... )
{
//To access the variable arguments use va_list 
	va_list valist;
	va_start(valist, num);//start of variable argument loop

	msgpack_object *obj = va_arg(valist, msgpack_object *);//each usage of va_arg fn argument iterates by one time
	msgpack_object_array *array = &obj->via.array;

	va_end(valist);//End of variable argument loop

	if( 0 < array->size )
	{
		size_t i;

		td->entries_count = array->size;
		td->entries = (tdoc_t *) malloc( sizeof(tdoc_t) * td->entries_count );
		if( NULL == td->entries )
		{
		    td->entries_count = 0;
		    return -1;
		}

		memset( td->entries, 0, sizeof(tdoc_t) * td->entries_count );
		for( i = 0; i < td->entries_count; i++ )
		{
			if( MSGPACK_OBJECT_MAP != array->ptr[i].type )
			{
				errno = INVALID_OBJECT;
				return -1;
			}
			if( 0 != process_tunnelparams(&td->entries[i], &array->ptr[i].via.map))
			{
				WebcfgError("process_tunnelparams failed\n");
				return -1;
			}
		}
	}
	
    return 0;
}

int process_wifi_doc( wifi_doc_t *wd,int num, ... )
{
//To access the variable arguments use va_list 
	va_list valist;
	va_start(valist, num);//start of variable argument loop

	msgpack_object *obj = va_arg(valist, msgpack_object *);//each usage of va_arg fn argument iterates by one time

	va_end(valist);//End of variable argument loop

	wd->entries_count = msgpack_array_count(obj);
	WebcfgDebug("The wifi doc array size is %d\n", (int)wd->entries_count);
		

    return 0;
}
