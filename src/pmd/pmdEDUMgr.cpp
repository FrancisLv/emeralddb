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
/*
 * EDU Status Transition Table
 * C: CREATING
 * R: RUNNING
 * W: WAITING
 * I: IDLE
 * D: DESTROY
 * c: createNewEDU
 * a: activateEDU
 * d: destroyEDU
 * w: waitEDU
 * t: deactivateEDU
 *   C   R   W   I   D  <--- from
 * C c
 * R a   -   a   a   -  <--- Create/Idle/Wait status can move to Running status
 * W -   w   -   -   -  <--- Running status move to Waiting
 * I t   -   t   -   -  <--- Creating/Waiting status move to Idle
 * D d   -   d   d   -  <--- Creating / Waiting / Idle can be destroyed
 * ^ To
 */

#include "pd.hpp"
#include "pmd.hpp"
#include "pmdEDUMgr.hpp"

int pmdEDUMgr::_destroyAll()
{
   _setDestroyed(true);
   setQuiesced(true);

   // stop all user edus
   unsigned int timeCounter = 0;
   unsigned int eduCount = _getEDUCount(EDU_USER);

   while(eduCount != 0)
   {
      if(0 == timeCounter % 50)
      {
         _forceEDUs(EDU_USER);
      }
      ++timeCounter;
      ossSleepmillis(100);
      eduCount = _getEDUCount(EDU_USER);
   }

   // stop all system edus
   timeCounter = 0;
   eduCount = _getEDUCount(EDU_ALL);
   while(eduCount != 0)
   {
      if(0 == timeCounter % 50)
      {
         _forceEDUs(EDU_ALL);
      }
      ++timeCounter;
      ossSleepmillis(100);
      eduCount = _getEDUCount(EDU_ALL);
   }

   return EDB_OK;
}

// force a specific EDU
int pmdEDUMgr::forceUserEDU(EDUID eduID)
{
   int rc = EDB_OK;
   std::map<EDUID, pmdEDUCB*>::iterator it;
   _mutex.get();
   if(isSystemEDU(eduID))
   {
      PD_LOG(PDERROR, "System EDU %d can't be forced", eduID);
      rc = EDB_PMD_FORCE_SYSTEM_EDU;
      goto error;
   }
   {
      for(it = _runQueue.begin(); it != _runQueue.end(); ++it)
      {
         if((*it).second->getID() == eduID)
         {
            (*it).second->force();
            goto done;
         }
      }
      for(it = _idleQueue.begin(); it != _idleQueue.end(); ++it)
      {
         if((*it).second->getID() == eduID)
         {
            (*it).second->force();
            goto done;
         }
      }
   }
done:
   _mutex.release();
   return rc;
error:
   goto done;
}

// block all new request and attempt to terminate existing requests
int pmdEDUMgr::_forceEDUs(int property)
{
   std::map<EDUID, pmdEDUCB*>::iterator it;

   /****************CRITICAL SECTION ****************/
   _mutex.get();
   // send terminate request to everyone
   for(it = _runQueue.begin(); it != _runQueue.end(); ++it)
   {
      if(((EDU_SYSTEM & property) && _isSystemEDU(it->first))
         || ((EDU_USER & property) && !_isSystemEDU(it->first)))
      {
         (*it).second->force();
         PD_LOG(PDDEBUG, "force edu[ID:%lld]", it->first);
      }
   }

   for(it = _idleQueue.begin(); it != _idleQueue.end(); ++it)
   {
      if(EDU_USER & property)
      {
         (*it).second->force();
      }
   }
   _mutex.release();
   /*******************END CRITICAL SECTION***************/
   return EDB_OK;
}

unsigned int pmdEDUMgr::_getEDUCount(int property)
{
   unsigned int eduCount = 0;
   std::map<EDUID, pmdEDUCB*>::iterator it;
   
   /****************CrITICAL SECTION*************/
   _mutex.get_shared();
   for(it = _runQueue.begin(); it != _runQueue.end(); ++it)
   {
      if(((EDU_SYSTEM &property) && _isSystemEDU(it->first)) 
         || ((EDU_USER & property) && !_isSystemEDU(it->first)))
      {
         ++eduCount;
      }
   }

   for(it = _idleQueue.begin(); it != _idleQueue.end(); ++it)
   {
      if (EDU_USER & property)
      {
         ++eduCount;
      }
   }
   _mutex.release_shared();
   /*************END CRITICAL SECTION**************/
   return eduCount;
}

int pmdEDUMgr::postEDUPost(EDUID eduID, pmdEDUEventTypes type,
                           bool release, void *pData)
{
   int rc = EDB_OK;
   pmdEDUCB *eduCB = NULL;
   std::map<EDUID, pmdEDUCB*>::iterator it;
   // shared lock the block, since we don't change anything
   _mutex.get_shared();
   if(_runQueue.end() == (it = _runQueue.find(eduID)))
   {
      // if we cannot find it in runqueue, we search for idle queue
      // not that during the time, we already have EDUMgr locked,
      // so thread cannot change queue from idle to run 
      // thar means we are safe to exame both queues
      if(_idleQueue.end() == (it = _idleQueue.find(eduID)))
      {
         // we can't find edu id anywhere
         rc = EDB_SYS;
         goto error;
      }
   }
   eduCB = (*it).second;
   eduCB->postEvent(pmdEDUEvent(type, release, pData));
done:
   _mutex.release_shared();
   return rc;
error:
   goto done;
}

int pmdEDUMgr::waitEDUPost(EDUID eduID, pmdEDUEvent& event,
                           long ling millsecond = -1)
{
   int rc = EDB_OK;
   pmdEDUCB* eduCB = NULL;
   std::map<EDUID, pmdEDUCB*>::iterator it;
   // shared lock the block, since we don't change anything
   _mutex.get_shared();
   if(_runQueue.end() == (it = _runQueue.find(eduID)))
   {
      // if we cannot find it in runqueue, we search for idle queue
      // note that during the time, we already have EDUMgr locked,
      // so thread cannot change queue from idle to run
      // that means we are safe to exame both queues
      if(_idleQueue.end() == (it = _idleQueue.find(eduID)))
      {
         // we can't find edu id anywhere
         rc = EDB_SYS;
         goto error;
      }
   }
   eduCB = (*it).second;
   // wait for event. when millsecond is 0, it should always return true
   if(!eduCB->waitEvent(event, millsecond))
   {
      rc = EDB_TIMEOUT;
      goto error;
   }
done:
   _mutex.release_shared();
   return rc;
error:
   goto done;
}

// release control from a given EDU
// EDUMgr should decide whether put the EDU to pool or destroy it
// EDU Status must be in waiting or creating
int pmdEDUMgr::returnEDU(EDUID eduID, bool force, bool* destroyed)
{
   int rc = EDB_OK;
   EDU_TYPES type = EDU_TYPE_UNKNOWN;
   pmdEDUCB *educb = NULL;
   std::map<EDUID, pmdEDUCB*>::iterator it;
   // shared critical section
   _mutex.get_shared();
   if(_runQueue.end() == (it = _runQueue.find(eduID)))
   {
      if(_idleQueue.end() == (it = _idleQueue.find(eduID)))
      {
         rc = EDB_SYS;
         *destroyed = false;
         _mutex.release_shared();
         goto error;
      }
   }
   educb = (*it).second;
   // if we are trying to destroy EDU manager, or enforce destroy, or
   // if the total number of threads are more than what we need
   // we need to destroy this EDU
   //
   // Current1 we only able to pool agent and coordagent
   if(educb)
   {
      type = educb->getType();
   }
   _mutex.release_shared();

   // if the EDU type can't be pooled, or if we forced, or if the EDU is
   // destroied, or we exceed max pooled edus, let's destroy it
   if(!isPoolable(type) || force || isDestroyed() ||
      size() > (unsigned int)pmdGetKRCB()->getMaxPool())
   {
      rc = _destroyEDU(eduID);
      if(destroy)
      {
         // we consider the EDU is destroyed when destroyEDU returns
         // OK or EDB_SYS (edu can't be found), so that thread can terminate
         // itself
         if(EDB_OK == rc || EDB_SYS == rc)
         {
            *destroyed = true;
         }
         else
            *destroyed = false;
      }
   }
   else
   {
      // in this case, we don't need to care whether the EDU is agent or not
      // as long as we treat EDB_SYS as "destroyed" signel, we should be 
      // safe here
      rc = _deactivateEDU(eduID);
      if(destroyed)
      {
         // when we try to pool the EDU, destroyed set to true only when 
         // the EDU can't be found in the list
         if(EDB_SYS == rc)
         {
            *destroyed = true;
         }
         else
            *destroyed = false;
      }
   }
done:
   return rc;
error:
   goto done;
}
