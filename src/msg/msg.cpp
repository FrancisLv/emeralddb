/*******************************************************************************
 Copyright (C) 2013 SequoiaDB Software Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License, version 3,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program. If not, see <http://www.gnu.org/license/>.
*******************************************************************************/
#include "msg.hpp"
#include "core.hpp"
#include "pd.hpp"

using namespace bson;

static int msgCheckBuffer(char **ppBuffer, int *pBufferSize, int length)
{
	int rc = EDB_OK;
	if(length < 0)
	{
		PD_LOG(PDERROR, "invalid length: %d", length);
		rc = EDB_INVALIDARG;
		goto error;
	}
	if(length > *pBufferSize)
	{
		char *pOldBuf = *ppBuffer;
		*ppBuffer = (char*)realloc(*ppBuffer, sizeof(char)*length);
		if(!*ppBuffer)
		{
			PD_LOG(PDERROR, "Failed to allocate %d bytes buffer", length);
			rc = EDB_OOM;
			*ppBuffer = pOldBuf;
			goto error;
		}
		*pBufferSize = length;
	}
done:
	return rc;
error:
	goto done;
}

